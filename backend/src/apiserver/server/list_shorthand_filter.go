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

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Shorthand query parameters let clients filter List requests with plain
// parameters instead of URL-encoded Filter JSON:
//
//	GET /apis/v2beta1/runs?state=RUNNING,FAILED
//	GET /apis/v2beta1/runs?display_name=my-run
//	GET /apis/v2beta1/runs?name=my-run                 (v1beta1 alias)
//	GET /apis/v2beta1/recurringruns?mode=DISABLED
//	GET /apis/v2beta1/recurringruns?status=ENABLED
//	GET /apis/v2beta1/experiments?display_name=demo
//	GET /apis/v2beta1/pipelines?display_name=etl
//	GET /apis/v2beta1/runs/{run_id}/tasks?state=SUCCEEDED
//	GET /apis/v2beta1/runs/{run_id}/executions?state=COMPLETE,FAILED
//
// Each parameter holds a comma-separated list that is translated into one IN
// predicate on the same filter key and combined with an optional `filter`
// parameter as extra AND-ed predicates. Values of state-like keys
// (state, mode, status) are upper-cased before matching; name values are
// kept verbatim (names are case-sensitive).

// upperCasedKeys lists the shorthand keys whose values are stored upper-case
// and are therefore matched case-insensitively. Every other shorthand key
// (display_name, name, ...) is matched verbatim.
var upperCasedKeys = map[string]bool{
	"state":  true,
	"mode":   true,
	"status": true,
}

// rewriteShorthandFilters moves the given shorthand query parameters into the
// `filter` query parameter as Filter JSON. The grpc-gateway then binds the
// result to the List*.filter proto fields, and the run metadata handlers apply
// the same JSON in memory for run tasks and executions. Requests without any
// shorthand parameter are returned unchanged.
func rewriteShorthandFilters(r *http.Request, shorthandKeys ...string) (*http.Request, error) {
	query := r.URL.Query()
	predicates := make([]*apiv2beta1.Predicate, 0, len(shorthandKeys))
	rewritten := false
	for _, key := range shorthandKeys {
		raw := query.Get(key)
		if raw == "" {
			continue
		}
		query.Del(key)
		rewritten = true
		values := make([]string, 0)
		for _, value := range strings.Split(raw, ",") {
			if value = strings.TrimSpace(value); value != "" {
				if upperCasedKeys[key] {
					value = strings.ToUpper(value)
				}
				values = append(values, value)
			}
		}
		if len(values) == 0 {
			return nil, status.Errorf(codes.InvalidArgument,
				"Query parameter %q must contain at least one value", key)
		}
		predicates = append(predicates, &apiv2beta1.Predicate{
			Key:       key,
			Operation: apiv2beta1.Predicate_IN,
			Value: &apiv2beta1.Predicate_StringValues_{StringValues: &apiv2beta1.Predicate_StringValues{
				Values: values,
			}},
		})
	}
	if !rewritten {
		return r, nil
	}

	filterProto := &apiv2beta1.Filter{}
	if encoded := query.Get("filter"); encoded != "" {
		parsed, err := parseAPIFilter(encoded, "v2beta1")
		if err != nil {
			return nil, err
		}
		if f, ok := parsed.(*apiv2beta1.Filter); ok && f != nil {
			filterProto = f
		}
	}
	filterProto.Predicates = append(filterProto.GetPredicates(), predicates...)
	marshaled, err := common.CustomMarshaler().Marshal(filterProto)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Failed to build a filter from query parameters")
	}
	query.Set("filter", string(marshaled))
	r.URL.RawQuery = query.Encode()
	return r, nil
}

// ListExperimentsHttpHandler serves GET /apis/v2beta1/experiments with
// optional shorthand filtering, e.g. ?display_name=demo. Requests without
// shorthand parameters are forwarded to the grpc-gateway unchanged.
func (s *ExperimentServer) ListExperimentsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "display_name", "name")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		gatewayMux.ServeHTTP(w, rewritten)
	}
}

// pipelineMarshaler uses proto field names and emits unpopulated fields so that
// description and other optional fields are always present in pipeline responses.
var pipelineMarshaler = &runtime.JSONPb{
	MarshalOptions: protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	},
	UnmarshalOptions: protojson.UnmarshalOptions{
		DiscardUnknown: false,
	},
}

// injectVersionNum marshals a proto pipeline to a JSON object and adds the
// version_num field from the provided counts map.
func injectVersionNum(pipeline *apiv2beta1.Pipeline, counts map[string]int) (json.RawMessage, error) {
	raw, err := pipelineMarshaler.Marshal(pipeline)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	m["version_num"] = counts[pipeline.GetPipelineId()]
	return json.Marshal(m)
}

// pipelineVersionSummary is the slim version shape embedded in GET /pipelines/{id}.
type pipelineVersionSummary struct {
	PipelineVersionID string `json:"pipeline_version_id"`
	DisplayName       string `json:"display_name"`
	Description       string `json:"description"`
	CreatedAt         string `json:"created_at"`
}

// GetPipelineHttpHandler serves GET /apis/v2beta1/pipelines/{pipeline_id}.
// Adds version_num and pipeline_versions (slim list) to the response.
func (s *PipelineServer) GetPipelineHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := mux.Vars(r)["pipeline_id"]

		pipeline, err := s.resourceManager.GetPipeline(pipelineID)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		// Fetch all ready versions sorted newest-first.
		versionOpts, err := validatedListOptions(&model.PipelineVersion{}, "", 1000, "created_at desc", "", "v2beta1")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		versions, totalSize, _, err := s.resourceManager.ListPipelineVersions(pipelineID, versionOpts)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		summaries := make([]pipelineVersionSummary, len(versions))
		for i, v := range versions {
			createdAt := ""
			if v.CreatedAtInSec != 0 {
				createdAt = time.Unix(v.CreatedAtInSec, 0).UTC().Format(time.RFC3339)
			}
			summaries[i] = pipelineVersionSummary{
				PipelineVersionID: v.UUID,
				DisplayName:       v.DisplayName,
				Description:       string(v.Description),
				CreatedAt:         createdAt,
			}
		}

		// Build base pipeline JSON then inject extra fields.
		raw, err := pipelineMarshaler.Marshal(toApiPipeline(pipeline))
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		var m map[string]interface{}
		if err := json.Unmarshal(raw, &m); err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		m["version_num"] = totalSize
		m["pipeline_versions"] = summaries

		payload, err := json.Marshal(m)
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}
}

// ListPipelinesHttpHandler serves GET /apis/v2beta1/pipelines with optional
// shorthand filtering (e.g. ?display_name=etl) and injects version_num into
// each pipeline in the response.
func (s *PipelineServer) ListPipelinesHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "display_name", "name")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		q := rewritten.URL.Query()
		pageToken := q.Get("page_token")
		pageSize := int32(0)
		if ps := q.Get("page_size"); ps != "" {
			if n, parseErr := parseInt32(ps); parseErr == nil {
				pageSize = n
			}
		}
		sortBy := q.Get("sort_by")
		filter := q.Get("filter")

		opts, err := validatedListOptions(&model.Pipeline{}, pageToken, int(pageSize), sortBy, filter, "v2beta1")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		// Namespace scoping: PipelineStore.ListPipelines dereferences
		// filterContext.ReferenceKey, so a nil filterContext panics (and Go's
		// http server then drops the connection). Honor ?namespace= the same
		// way the gRPC ListPipelines handler does.
		filterContext := &model.FilterContext{}
		if ns := q.Get("namespace"); ns != "" {
			filterContext.ReferenceKey = &model.ReferenceKey{
				Type: model.NamespaceResourceType,
				ID:   ns,
			}
		}
		pipelines, totalSize, nextPageToken, err := s.resourceManager.ListPipelines(filterContext, opts)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		// Collect pipeline IDs for a single version-count query.
		ids := make([]string, len(pipelines))
		for i, p := range pipelines {
			ids[i] = p.UUID
		}
		counts, err := s.resourceManager.CountVersionsForPipelines(ids)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		pipelineItems := make([]json.RawMessage, len(pipelines))
		for i, p := range pipelines {
			item, marshalErr := injectVersionNum(toApiPipeline(p), counts)
			if marshalErr != nil {
				http.Error(w, `{"code":13,"message":"failed to marshal pipeline"}`, http.StatusInternalServerError)
				return
			}
			pipelineItems[i] = item
		}

		type listResp struct {
			Pipelines     []json.RawMessage `json:"pipelines"`
			TotalSize     int               `json:"total_size"`
			NextPageToken string            `json:"next_page_token,omitempty"`
		}
		payload, err := json.Marshal(listResp{
			Pipelines:     pipelineItems,
			TotalSize:     totalSize,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}
}

// pipelineVersionMarshaler emits description even when it is an empty string
// so that the field is always visible in ListPipelineVersions responses.
var pipelineVersionMarshaler = &runtime.JSONPb{
	MarshalOptions: protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	},
	UnmarshalOptions: protojson.UnmarshalOptions{
		DiscardUnknown: false,
	},
}

// ListPipelineVersionsHttpHandler serves
// GET /apis/v2beta1/pipelines/{pipeline_id}/versions.
//
// It replaces the grpc-gateway route so that the description field is always
// present in the JSON response (the default marshaler omits empty strings).
func (s *PipelineServer) ListPipelineVersionsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := mux.Vars(r)["pipeline_id"]
		q := r.URL.Query()
		pageToken := q.Get("page_token")
		pageSize := int32(0)
		if ps := q.Get("page_size"); ps != "" {
			if n, err := parseInt32(ps); err == nil {
				pageSize = n
			}
		}
		sortBy := q.Get("sort_by")
		filter := q.Get("filter")

		opts, err := validatedListOptions(&model.PipelineVersion{}, pageToken, int(pageSize), sortBy, filter, "v2beta1")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		versions, totalSize, nextPageToken, err := s.resourceManager.ListPipelineVersions(pipelineID, opts)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		resp := &apiv2beta1.ListPipelineVersionsResponse{
			PipelineVersions: toApiPipelineVersions(versions),
			NextPageToken:    nextPageToken,
			TotalSize:        int32(totalSize),
		}

		w.Header().Set("Content-Type", "application/json")
		payload, err := pipelineVersionMarshaler.Marshal(resp)
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	}
}

// GetPipelineVersionHttpHandler serves
// GET /apis/v2beta1/pipelines/{pipeline_id}/versions/{pipeline_version_id}.
//
// Replaces the grpc-gateway route so that description is always present in the
// JSON response.
func (s *PipelineServer) GetPipelineVersionHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		versionID := mux.Vars(r)["pipeline_version_id"]

		pv, err := s.resourceManager.GetPipelineVersion(versionID)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		payload, err := pipelineVersionMarshaler.Marshal(toApiPipelineVersion(pv))
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(payload)
	}
}

// parseInt32 converts a decimal string to int32.
func parseInt32(s string) (int32, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return int32(n), err
}
