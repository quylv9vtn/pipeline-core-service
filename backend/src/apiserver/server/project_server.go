// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	projectIDLabel    = "project-id"
	projectSlugLabel  = "project-slug"
	projectService    = "ai-pipeline"
	projectManagedBy  = "pipeline-core"
	projectRunnerName = "pipeline-runner"
	artifactSecret    = "mlpipeline-minio-artifact"
	maxProjectBodyLen = 1 << 20
)

var errProjectMetadataConflict = errors.New("project namespace metadata conflict")

type ProjectProvisionRequest struct {
	ProjectID string `json:"project_id"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type ProjectProvisionResponse struct {
	ProjectID string            `json:"project_id"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Status    string            `json:"status"`
	Labels    map[string]string `json:"labels"`
}

type ProjectListResponse struct {
	Projects []ProjectProvisionResponse `json:"projects"`
}

type projectErrorResponse struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

type projectAuthorizer func(context.Context, *authorizationv1.ResourceAttributes) error

type ProjectServer struct {
	kubernetes kubernetes.Interface
	authorize  projectAuthorizer
}

func NewProjectServer(resourceManager *resource.ResourceManager) *ProjectServer {
	return newProjectServer(
		resourceManager.KubernetesCoreClient().GetClientSet(),
		resourceManager.IsAuthorized,
	)
}

func newProjectServer(kubernetes kubernetes.Interface, authorize projectAuthorizer) *ProjectServer {
	return &ProjectServer{kubernetes: kubernetes, authorize: authorize}
}

func (s *ProjectServer) ProvisionProject(w http.ResponseWriter, r *http.Request) {
	request, err := decodeProjectProvisionRequest(w, r)
	if err != nil {
		writeProjectError(w, http.StatusBadRequest, codes.InvalidArgument, err.Error())
		return
	}
	if err := validateProjectProvisionRequest(request); err != nil {
		writeProjectError(w, http.StatusBadRequest, codes.InvalidArgument, err.Error())
		return
	}

	if common.IsMultiUserMode() {
		ctx := contextWithHTTPHeaders(r)
		err := s.authorize(ctx, &authorizationv1.ResourceAttributes{
			Verb:     "create",
			Group:    "",
			Version:  "v1",
			Resource: "namespaces",
		})
		if err != nil {
			writeProjectError(w, http.StatusForbidden, codes.PermissionDenied, "Not authorized to provision project namespaces")
			return
		}
	}

	response, created, err := s.provision(r.Context(), request)
	if err != nil {
		if errors.Is(err, errProjectMetadataConflict) {
			writeProjectError(w, http.StatusConflict, codes.AlreadyExists, err.Error())
			return
		}
		writeProjectError(w, http.StatusInternalServerError, codes.Internal, "Failed to provision project namespace")
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if created {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (s *ProjectServer) ListProjects(w http.ResponseWriter, r *http.Request) {
	if err := s.authorizeProjectRead(r, "list"); err != nil {
		writeProjectError(w, http.StatusForbidden, codes.PermissionDenied, "Not authorized to list projects")
		return
	}
	namespaces, err := s.kubernetes.CoreV1().Namespaces().List(r.Context(), metav1.ListOptions{LabelSelector: projectManagedBySelector()})
	if err != nil {
		writeProjectError(w, http.StatusInternalServerError, codes.Internal, "Failed to list projects")
		return
	}
	projects := make([]ProjectProvisionResponse, 0, len(namespaces.Items))
	for _, namespace := range namespaces.Items {
		if project := projectResponseFromNamespace(&namespace); project != nil {
			projects = append(projects, *project)
		}
	}
	writeProjectJSON(w, http.StatusOK, ProjectListResponse{Projects: projects})
}

func (s *ProjectServer) GetProject(w http.ResponseWriter, r *http.Request) {
	if err := s.authorizeProjectRead(r, "get"); err != nil {
		writeProjectError(w, http.StatusForbidden, codes.PermissionDenied, "Not authorized to get projects")
		return
	}
	projectID := strings.TrimSpace(mux.Vars(r)["project_id"])
	if _, err := uuid.Parse(projectID); err != nil {
		writeProjectError(w, http.StatusBadRequest, codes.InvalidArgument, "project_id must be a valid UUID")
		return
	}
	namespaces, err := s.kubernetes.CoreV1().Namespaces().List(r.Context(), metav1.ListOptions{
		LabelSelector: projectIDLabel + "=" + projectID,
	})
	if err != nil {
		writeProjectError(w, http.StatusInternalServerError, codes.Internal, "Failed to get project")
		return
	}
	for _, namespace := range namespaces.Items {
		if namespace.Labels[projectManagedByLabel()] == projectManagedBy {
			writeProjectJSON(w, http.StatusOK, projectResponseFromNamespace(&namespace))
			return
		}
	}
	writeProjectError(w, http.StatusNotFound, codes.NotFound, "Project not found")
}

func (s *ProjectServer) authorizeProjectRead(r *http.Request, verb string) error {
	if !common.IsMultiUserMode() || s.authorize == nil {
		return nil
	}
	return s.authorize(contextWithHTTPHeaders(r), &authorizationv1.ResourceAttributes{
		Verb: verb, Group: "", Version: "v1", Resource: "namespaces",
	})
}

func projectManagedByLabel() string { return "app.kubernetes.io/managed-by" }
func projectManagedBySelector() string {
	return projectManagedByLabel() + "=" + projectManagedBy + ",service=" + projectService
}

func projectResponseFromNamespace(namespace *corev1.Namespace) *ProjectProvisionResponse {
	projectID := namespace.Labels[projectIDLabel]
	name := namespace.Labels[projectSlugLabel]
	if projectID == "" || name == "" {
		return nil
	}
	return &ProjectProvisionResponse{
		ProjectID: projectID,
		Name: name,
		Namespace: namespace.Name,
		Status: "AVAILABLE",
		Labels: namespace.Labels,
	}
}

func decodeProjectProvisionRequest(w http.ResponseWriter, r *http.Request) (*ProjectProvisionRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxProjectBodyLen)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	request := &ProjectProvisionRequest{}
	if err := decoder.Decode(request); err != nil {
		return nil, fmt.Errorf("invalid JSON request body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("request body must contain exactly one JSON object")
	}
	return request, nil
}

func validateProjectProvisionRequest(request *ProjectProvisionRequest) error {
	request.ProjectID = strings.TrimSpace(request.ProjectID)
	request.Name = strings.TrimSpace(request.Name)
	request.Namespace = strings.TrimSpace(request.Namespace)

	if request.ProjectID == "" || request.Name == "" || request.Namespace == "" {
		return errors.New("project_id, name, and namespace are required")
	}
	if _, err := uuid.Parse(request.ProjectID); err != nil {
		return errors.New("project_id must be a valid UUID")
	}
	if validationErrors := k8svalidation.IsDNS1123Label(request.Name); len(validationErrors) > 0 {
		return fmt.Errorf("name must be a valid Kubernetes label: %s", strings.Join(validationErrors, "; "))
	}
	if validationErrors := k8svalidation.IsDNS1123Label(request.Namespace); len(validationErrors) > 0 {
		return fmt.Errorf("namespace must be a valid Kubernetes namespace name: %s", strings.Join(validationErrors, "; "))
	}
	return nil
}

func (s *ProjectServer) provision(ctx context.Context, request *ProjectProvisionRequest) (*ProjectProvisionResponse, bool, error) {
	desiredLabels := map[string]string{
		projectIDLabel:                 request.ProjectID,
		projectSlugLabel:               request.Name,
		"service":                      projectService,
		"app.kubernetes.io/managed-by": projectManagedBy,
	}

	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: request.Namespace, Labels: desiredLabels}}
	createdNamespace, err := s.kubernetes.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
	if err == nil {
		if err := s.provisionProjectRuntime(ctx, request, desiredLabels); err != nil {
			return nil, false, err
		}
		return projectProvisionResponse(request, "CREATED", createdNamespace.Labels), true, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return nil, false, err
	}

	status := "UNCHANGED"
	var reconciled *corev1.Namespace
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, getErr := s.kubernetes.CoreV1().Namespaces().Get(ctx, request.Namespace, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if existing.DeletionTimestamp != nil {
			return fmt.Errorf("%w: namespace %q is terminating", errProjectMetadataConflict, request.Namespace)
		}

		updated := existing.DeepCopy()
		if updated.Labels == nil {
			updated.Labels = map[string]string{}
		}
		changed := false
		for key, desiredValue := range desiredLabels {
			if existingValue, found := updated.Labels[key]; found && existingValue != desiredValue {
				return fmt.Errorf("%w: namespace %q has label %q=%q, requested %q", errProjectMetadataConflict, request.Namespace, key, existingValue, desiredValue)
			}
			if _, found := updated.Labels[key]; !found {
				updated.Labels[key] = desiredValue
				changed = true
			}
		}
		if !changed {
			reconciled = existing
			return nil
		}

		status = "UPDATED"
		reconciled, getErr = s.kubernetes.CoreV1().Namespaces().Update(ctx, updated, metav1.UpdateOptions{})
		return getErr
	})
	if err != nil {
		return nil, false, err
	}
	if err := s.provisionProjectRuntime(ctx, request, desiredLabels); err != nil {
		return nil, false, err
	}
	return projectProvisionResponse(request, status, reconciled.Labels), false, nil
}

func (s *ProjectServer) provisionProjectRuntime(ctx context.Context, request *ProjectProvisionRequest, labels map[string]string) error {
	serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name:      projectRunnerName,
		Namespace: request.Namespace,
		Labels:    labels,
	}}
	if _, err := s.kubernetes.CoreV1().ServiceAccounts(request.Namespace).Create(ctx, serviceAccount, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to provision project pipeline runner service account: %w", err)
	}

	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: projectRunnerName + "-binding", Namespace: request.Namespace, Labels: labels},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     projectRunnerName,
		},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: projectRunnerName, Namespace: request.Namespace}},
	}
	if _, err := s.kubernetes.RbacV1().RoleBindings(request.Namespace).Create(ctx, roleBinding, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to provision project pipeline runner role binding: %w", err)
		}
		existing, getErr := s.kubernetes.RbacV1().RoleBindings(request.Namespace).Get(ctx, roleBinding.Name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("failed to verify project pipeline runner role binding: %w", getErr)
		}
		if existing.RoleRef != roleBinding.RoleRef || !reflect.DeepEqual(existing.Subjects, roleBinding.Subjects) {
			return fmt.Errorf("%w: role binding %q in namespace %q does not bind ServiceAccount/%s to ClusterRole/%s",
				errProjectMetadataConflict, roleBinding.Name, request.Namespace, projectRunnerName, projectRunnerName)
		}
	}

	if request.Namespace == common.GetPodNamespace() {
		return nil
	}
	source, err := s.kubernetes.CoreV1().Secrets(common.GetPodNamespace()).Get(ctx, artifactSecret, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to read source artifact secret: %w", err)
	}
	target := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: artifactSecret, Namespace: request.Namespace, Labels: labels},
		Type:       source.Type,
		Data:       source.Data,
	}
	if _, err := s.kubernetes.CoreV1().Secrets(request.Namespace).Create(ctx, target, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to provision project artifact secret: %w", err)
		}
		existing, getErr := s.kubernetes.CoreV1().Secrets(request.Namespace).Get(ctx, artifactSecret, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("failed to verify project artifact secret: %w", getErr)
		}
		if existing.Type != target.Type || !reflect.DeepEqual(existing.Data, target.Data) {
			return fmt.Errorf("%w: secret %q in namespace %q differs from the pipeline artifact secret",
				errProjectMetadataConflict, artifactSecret, request.Namespace)
		}
	}
	return nil
}

func projectProvisionResponse(request *ProjectProvisionRequest, status string, labels map[string]string) *ProjectProvisionResponse {
	return &ProjectProvisionResponse{
		ProjectID: request.ProjectID,
		Name:      request.Name,
		Namespace: request.Namespace,
		Status:    status,
		Labels:    labels,
	}
}

func contextWithHTTPHeaders(r *http.Request) context.Context {
	md := metadata.MD{}
	for key, values := range r.Header {
		md.Set(key, values...)
	}
	return metadata.NewIncomingContext(r.Context(), md)
}

func writeProjectJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeProjectError(w http.ResponseWriter, httpStatus int, code codes.Code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(&projectErrorResponse{Code: int32(code), Message: message})
}
