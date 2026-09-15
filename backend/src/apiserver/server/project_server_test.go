// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

const validProjectRequest = `{
  "project_id": "a57cf979-7230-463f-8664-d8209a7b05ac",
  "name": "fraud-detection",
  "namespace": "project-fraud-detection"
}`

func TestProvisionProjectCreatesNamespaceAndIsIdempotent(t *testing.T) {
	server, client := newTestProjectServer()

	first := invokeProvisionProject(t, server, validProjectRequest)
	require.Equal(t, http.StatusCreated, first.Code)
	var firstResponse ProjectProvisionResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &firstResponse))
	require.Equal(t, "CREATED", firstResponse.Status)

	namespace, err := client.CoreV1().Namespaces().Get(context.Background(), "project-fraud-detection", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		projectIDLabel:                 "a57cf979-7230-463f-8664-d8209a7b05ac",
		projectSlugLabel:               "fraud-detection",
		"service":                      projectService,
		"app.kubernetes.io/managed-by": projectManagedBy,
	}, namespace.Labels)

	serviceAccount, err := client.CoreV1().ServiceAccounts(namespace.Name).Get(context.Background(), projectRunnerName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, projectManagedBy, serviceAccount.Labels["app.kubernetes.io/managed-by"])

	roleBinding, err := client.RbacV1().RoleBindings(namespace.Name).Get(context.Background(), projectRunnerName+"-binding", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: projectRunnerName}, roleBinding.RoleRef)
	require.Equal(t, []rbacv1.Subject{{Kind: "ServiceAccount", Name: projectRunnerName, Namespace: namespace.Name}}, roleBinding.Subjects)

	secret, err := client.CoreV1().Secrets(namespace.Name).Get(context.Background(), artifactSecret, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"accesskey": []byte("minio"), "secretkey": []byte("minio123")}, secret.Data)

	second := invokeProvisionProject(t, server, validProjectRequest)
	require.Equal(t, http.StatusOK, second.Code)
	var secondResponse ProjectProvisionResponse
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &secondResponse))
	require.Equal(t, "UNCHANGED", secondResponse.Status)
}

func TestProvisionProjectAddsMissingManagedLabels(t *testing.T) {
	existing := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   "project-fraud-detection",
		Labels: map[string]string{"owner": "platform-team"},
	}}
	server, client := newTestProjectServer(existing)

	response := invokeProvisionProject(t, server, validProjectRequest)
	require.Equal(t, http.StatusOK, response.Code)
	var body ProjectProvisionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "UPDATED", body.Status)

	namespace, err := client.CoreV1().Namespaces().Get(context.Background(), existing.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "platform-team", namespace.Labels["owner"])
	require.Equal(t, "a57cf979-7230-463f-8664-d8209a7b05ac", namespace.Labels[projectIDLabel])
	require.Equal(t, projectManagedBy, namespace.Labels["app.kubernetes.io/managed-by"])
}

func TestProvisionProjectRejectsConflictingManagedLabels(t *testing.T) {
	existing := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: "project-fraud-detection",
		Labels: map[string]string{
			projectIDLabel: "f8d3ffbd-7ded-4370-834d-465d281e4e5f",
		},
	}}
	server, client := newTestProjectServer(existing)

	response := invokeProvisionProject(t, server, validProjectRequest)
	require.Equal(t, http.StatusConflict, response.Code)

	namespace, err := client.CoreV1().Namespaces().Get(context.Background(), existing.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "f8d3ffbd-7ded-4370-834d-465d281e4e5f", namespace.Labels[projectIDLabel])
	require.NotContains(t, namespace.Labels, projectSlugLabel)
}

func TestProvisionProjectValidatesRequest(t *testing.T) {
	testCases := map[string]string{
		"missing field":     `{"project_id":"a57cf979-7230-463f-8664-d8209a7b05ac","name":"fraud-detection"}`,
		"invalid UUID":      `{"project_id":"not-a-uuid","name":"fraud-detection","namespace":"project-fraud-detection"}`,
		"invalid slug":      `{"project_id":"a57cf979-7230-463f-8664-d8209a7b05ac","name":"Fraud Detection","namespace":"project-fraud-detection"}`,
		"invalid namespace": `{"project_id":"a57cf979-7230-463f-8664-d8209a7b05ac","name":"fraud-detection","namespace":"Project_Fraud"}`,
		"unknown field":     `{"project_id":"a57cf979-7230-463f-8664-d8209a7b05ac","name":"fraud-detection","namespace":"project-fraud-detection","owner":"alice"}`,
	}

	for name, requestBody := range testCases {
		t.Run(name, func(t *testing.T) {
			server, _ := newTestProjectServer()
			response := invokeProvisionProject(t, server, requestBody)
			require.Equal(t, http.StatusBadRequest, response.Code)
		})
	}
}

func TestListProjectsReturnsManagedProjects(t *testing.T) {
	server, _ := newTestProjectServer(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "project-a", Labels: map[string]string{
			projectIDLabel: "a57cf979-7230-463f-8664-d8209a7b05ac", projectSlugLabel: "project-a",
			projectManagedByLabel(): projectManagedBy, "service": projectService,
		}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged", Labels: map[string]string{projectIDLabel: "f8d3ffbd-7ded-4370-834d-465d281e4e5f"}}},
	)
	request := httptest.NewRequest(http.MethodGet, "/apis/v2beta1/projects", nil)
	response := httptest.NewRecorder()
	server.ListProjects(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	var body ProjectListResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Len(t, body.Projects, 1)
	require.Equal(t, "project-a", body.Projects[0].Namespace)
}

func TestGetProjectReturnsManagedProject(t *testing.T) {
	server, _ := newTestProjectServer(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "project-a", Labels: map[string]string{
		projectIDLabel: "a57cf979-7230-463f-8664-d8209a7b05ac", projectSlugLabel: "project-a",
		projectManagedByLabel(): projectManagedBy,
	}}})
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/apis/v2beta1/projects/a57cf979-7230-463f-8664-d8209a7b05ac", nil), map[string]string{"project_id": "a57cf979-7230-463f-8664-d8209a7b05ac"})
	response := httptest.NewRecorder()
	server.GetProject(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	var body ProjectProvisionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "a57cf979-7230-463f-8664-d8209a7b05ac", body.ProjectID)
}

func TestGetProjectReturnsNotFound(t *testing.T) {
	server, _ := newTestProjectServer()
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/apis/v2beta1/projects/a57cf979-7230-463f-8664-d8209a7b05ac", nil), map[string]string{"project_id": "a57cf979-7230-463f-8664-d8209a7b05ac"})
	response := httptest.NewRecorder()
	server.GetProject(response, request)

	require.Equal(t, http.StatusNotFound, response.Code)
}

func newTestProjectServer(objects ...runtime.Object) (*ProjectServer, *k8sfake.Clientset) {
	objects = append([]runtime.Object{&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: artifactSecret, Namespace: common.GetPodNamespace()},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"accesskey": []byte("minio"), "secretkey": []byte("minio123")},
	}}, objects...)
	client := k8sfake.NewSimpleClientset(objects...)
	return newProjectServer(client, nil), client
}

func invokeProvisionProject(t *testing.T, server *ProjectServer, requestBody string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/apis/v2beta1/projects", bytes.NewBufferString(requestBody))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ProvisionProject(response, request)
	return response
}
