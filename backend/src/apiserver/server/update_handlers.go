// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

// Update HTTP handlers for Pipeline, PipelineVersion, and RecurringRun.
//
// These handlers expose PATCH endpoints that allow updating display_name and
// description on existing resources. They are registered on topMux before the
// grpc-gateway PathPrefix so they take precedence.
//
//	PATCH /apis/v2beta1/pipelines/{pipeline_id}
//	PATCH /apis/v2beta1/pipelines/{pipeline_id}/versions/{pipeline_version_id}
//	PATCH /apis/v2beta1/recurringruns/{recurring_run_id}

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// updateFieldsRequest is the JSON body accepted by all three PATCH handlers.
type updateFieldsRequest struct {
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
}

// PatchPipelineHttpHandler serves PATCH /apis/v2beta1/pipelines/{pipeline_id}.
// It updates display_name and description and returns the full updated pipeline
// resource encoded with the proto-JSON marshaler.
func (s *PipelineServer) PatchPipelineHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := mux.Vars(r)["pipeline_id"]
		var body updateFieldsRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r,
				status.Errorf(codes.InvalidArgument, "invalid JSON body: %v", err))
			return
		}

		// Verify existence before update so we can return a proper 404.
		pipeline, err := s.resourceManager.GetPipeline(pipelineID)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		if err := s.resourceManager.UpdatePipelineFields(pipelineID, body.DisplayName, body.Description); err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		// Reflect the update in the response object without a second DB round-trip.
		pipeline.DisplayName = body.DisplayName
		pipeline.Description = model.LargeText(body.Description)

		marshaler := common.CustomMarshaler()
		w.Header().Set("Content-Type", marshaler.ContentType(nil))
		payload, err := marshaler.Marshal(toApiPipeline(pipeline))
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	}
}

// PatchPipelineVersionHttpHandler serves
// PATCH /apis/v2beta1/pipelines/{pipeline_id}/versions/{pipeline_version_id}.
func (s *PipelineServer) PatchPipelineVersionHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		versionID := mux.Vars(r)["pipeline_version_id"]
		var body updateFieldsRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r,
				status.Errorf(codes.InvalidArgument, "invalid JSON body: %v", err))
			return
		}

		// Verify existence before update.
		pv, err := s.resourceManager.GetPipelineVersion(versionID)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		if err := s.resourceManager.UpdatePipelineVersionFields(versionID, body.DisplayName, body.Description); err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		pv.DisplayName = body.DisplayName
		pv.Description = model.LargeText(body.Description)

		marshaler := common.CustomMarshaler()
		w.Header().Set("Content-Type", marshaler.ContentType(nil))
		payload, err := marshaler.Marshal(toApiPipelineVersion(pv))
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	}
}

// PatchRecurringRunHttpHandler serves
// PATCH /apis/v2beta1/recurringruns/{recurring_run_id}.
func (s *JobServer) PatchRecurringRunHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobID := mux.Vars(r)["recurring_run_id"]
		var body updateFieldsRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r,
				status.Errorf(codes.InvalidArgument, "invalid JSON body: %v", err))
			return
		}

		// Verify existence before update.
		job, err := s.resourceManager.GetJob(jobID)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		if err := s.resourceManager.UpdateJobFields(jobID, body.DisplayName, body.Description); err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		job.DisplayName = body.DisplayName
		job.Description = body.Description

		marshaler := common.CustomMarshaler()
		w.Header().Set("Content-Type", marshaler.ContentType(nil))
		payload, err := marshaler.Marshal(toApiRecurringRun(job))
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	}
}
