// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strings"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/resource"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"google.golang.org/grpc/codes"
	authorizationv1 "k8s.io/api/authorization/v1"
)

const (
	pipelineIDPathKey        = "pipeline_id"
	pipelineVersionIDPathKey = "pipeline_version_id"
)

type pipelineDownloadErrorResponse struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// PipelineDownloadServer serves the stored, executable definition of a pipeline version.
// It is an HTTP-only endpoint because the response is a downloadable file rather than a
// protobuf resource.
type PipelineDownloadServer struct {
	*BasePipelineServer
}

func NewPipelineDownloadServer(resourceManager *resource.ResourceManager) *PipelineDownloadServer {
	return &PipelineDownloadServer{
		BasePipelineServer: &BasePipelineServer{
			resourceManager: resourceManager,
			httpClient:      http.DefaultClient,
			options:         &PipelineServerOptions{CollectMetrics: false},
		},
	}
}

// DownloadPipelineVersion returns the original pipeline definition stored for a PipelineVersion.
func (s *PipelineDownloadServer) DownloadPipelineVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pipelineID := strings.TrimSpace(vars[pipelineIDPathKey])
	pipelineVersionID := strings.TrimSpace(vars[pipelineVersionIDPathKey])
	if pipelineID == "" || pipelineVersionID == "" {
		writePipelineDownloadError(w, http.StatusBadRequest, codes.InvalidArgument, "pipeline_id and pipeline_version_id are required")
		return
	}

	pipelineVersion, err := s.resourceManager.GetPipelineVersion(pipelineVersionID)
	if err != nil {
		if util.IsUserErrorCodeMatch(err, codes.NotFound) {
			writePipelineDownloadError(w, http.StatusNotFound, codes.NotFound, "Pipeline version not found")
			return
		}
		glog.Errorf("Failed to get pipeline version %q for download: %+v", pipelineVersionID, err)
		writePipelineDownloadError(w, http.StatusInternalServerError, codes.Internal, "Failed to get pipeline version")
		return
	}
	if pipelineVersion.PipelineId != pipelineID {
		writePipelineDownloadError(w, http.StatusNotFound, codes.NotFound, "Pipeline version does not belong to the specified pipeline")
		return
	}

	err = s.canAccessPipelineVersion(contextWithHTTPHeaders(r), pipelineVersionID, &authorizationv1.ResourceAttributes{
		Verb: common.RbacResourceVerbGet,
	})
	if err != nil {
		glog.Errorf("Failed to authorize pipeline version %q download: %+v", pipelineVersionID, err)
		writePipelineDownloadError(w, http.StatusForbidden, codes.PermissionDenied, "Not authorized to download this pipeline version")
		return
	}

	template, err := s.resourceManager.GetPipelineVersionTemplate(pipelineVersionID)
	if err != nil {
		glog.Errorf("Failed to load pipeline version %q template for download: %+v", pipelineVersionID, err)
		writePipelineDownloadError(w, http.StatusInternalServerError, codes.Internal, "Failed to load pipeline version definition")
		return
	}

	fileName := fmt.Sprintf("pipeline-version-%s.yaml", sanitizeDownloadFileName(pipelineVersionID))
	contentDisposition := mime.FormatMediaType("attachment", map[string]string{"filename": fileName})
	if contentDisposition == "" {
		contentDisposition = "attachment"
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Content-Disposition", contentDisposition)
	w.Header().Set("Cache-Control", "no-cache, private")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(template); err != nil {
		glog.Errorf("Failed to write pipeline version %q definition: %+v", pipelineVersionID, err)
	}
}

func sanitizeDownloadFileName(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
}

func writePipelineDownloadError(w http.ResponseWriter, httpStatus int, code codes.Code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(&pipelineDownloadErrorResponse{Code: int32(code), Message: message})
}
