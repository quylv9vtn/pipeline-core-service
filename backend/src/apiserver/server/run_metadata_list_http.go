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
	"context"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ListRunTasksRequest and ListRunExecutionsRequest declare only run_id, so the
// grpc-gateway cannot bind an optional `filter` query parameter for these two
// endpoints. These handlers fill the gap: requests without a `filter` (after
// shorthand parameters such as ?state=SUCCEEDED are rewritten into `filter`)
// are forwarded to the grpc-gateway unchanged; with a `filter`, the full list
// is fetched through the gRPC handler and narrowed in memory.
//
// Only filtering by "state" is supported. Predicates accept EQUALS and
// NOT_EQUALS with a string value and IN with repeated string values, and
// multiple predicates are combined with AND. State names are compared
// case-insensitively:
//   - tasks: RUNTIME_STATE_UNSPECIFIED, PENDING, RUNNING, SUCCEEDED, SKIPPED,
//     FAILED, CANCELING, CANCELED, PAUSED
//   - executions: MLMD execution states, e.g. NEW, RUNNING, COMPLETE, FAILED,
//     CANCELED

// ListRunTasksHttpHandler serves GET /apis/v2beta1/runs/{run_id}/tasks.
func (s *RunServer) ListRunTasksHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "state")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		if rewritten.URL.Query().Get("filter") == "" {
			gatewayMux.ServeHTTP(w, rewritten)
			return
		}
		r = rewritten
		ctx := r.Context()
		response, err := s.ListRunTasks(ctx, &apiv2beta1.ListRunTasksRequest{RunId: mux.Vars(r)["run_id"]})
		if err != nil {
			writeRunMetadataHttpError(ctx, gatewayMux, w, r, err)
			return
		}
		filterProto, err := parseListHttpStateFilter(r)
		if err != nil {
			writeRunMetadataHttpError(ctx, gatewayMux, w, r, err)
			return
		}
		tasks := response.GetTasks()
		stateOf := func(i int) string { return tasks[i].GetState().String() }
		kept, err := filterIndicesByState(len(tasks), stateOf, filterProto)
		if err != nil {
			writeRunMetadataHttpError(ctx, gatewayMux, w, r, err)
			return
		}
		filtered := make([]*apiv2beta1.PipelineTaskDetail, 0, len(kept))
		for _, i := range kept {
			filtered = append(filtered, tasks[i])
		}
		response.Tasks = filtered
		response.TotalSize = int32(len(filtered))
		writeRunMetadataHttpResponse(w, response)
	}
}

// ListRunExecutionsHttpHandler serves GET /apis/v2beta1/runs/{run_id}/executions.
func (s *RunServer) ListRunExecutionsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "state")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		if rewritten.URL.Query().Get("filter") == "" {
			gatewayMux.ServeHTTP(w, rewritten)
			return
		}
		r = rewritten
		ctx := r.Context()
		response, err := s.ListRunExecutions(ctx, &apiv2beta1.ListRunExecutionsRequest{RunId: mux.Vars(r)["run_id"]})
		if err != nil {
			writeRunMetadataHttpError(ctx, gatewayMux, w, r, err)
			return
		}
		executions := response.GetExecutions()
		stateOf := func(i int) string { return executions[i].GetState() }
		filterProto, err := parseListHttpStateFilter(r)
		if err != nil {
			writeRunMetadataHttpError(ctx, gatewayMux, w, r, err)
			return
		}
		kept, err := filterIndicesByState(len(executions), stateOf, filterProto)
		if err != nil {
			writeRunMetadataHttpError(ctx, gatewayMux, w, r, err)
			return
		}
		filtered := make([]*apiv2beta1.RunExecution, 0, len(kept))
		for _, i := range kept {
			filtered = append(filtered, executions[i])
		}
		response.Executions = filtered
		response.TotalSize = int32(len(filtered))
		writeRunMetadataHttpResponse(w, response)
	}
}

func parseListHttpStateFilter(r *http.Request) (*apiv2beta1.Filter, error) {
	parsed, err := parseAPIFilter(r.URL.Query().Get("filter"), "v2beta1")
	if err != nil {
		return nil, err
	}
	filterProto, ok := parsed.(*apiv2beta1.Filter)
	if !ok || filterProto == nil {
		return nil, status.Error(codes.InvalidArgument, "Failed to parse a valid filter")
	}
	return filterProto, nil
}

// filterIndicesByState evaluates a state-only filter over count items and
// returns the indices of the items that match every predicate. See the handler
// comments above for the supported keys, operations and value types.
func filterIndicesByState(count int, stateOf func(int) string, filterProto *apiv2beta1.Filter) ([]int, error) {
	kept := make([]int, 0, count)
	for i := 0; i < count; i++ {
		matches := true
		for _, predicate := range filterProto.GetPredicates() {
			if predicate.GetKey() != "state" {
				return nil, status.Error(codes.InvalidArgument,
					`Only the "state" key is supported when filtering run tasks and executions`)
			}
			itemState := strings.ToUpper(stateOf(i))
			switch predicate.GetOperation() {
			case apiv2beta1.Predicate_EQUALS:
				value, err := predicateSingleStringValue(predicate)
				if err != nil {
					return nil, err
				}
				matches = matches && itemState == strings.ToUpper(value)
			case apiv2beta1.Predicate_NOT_EQUALS:
				value, err := predicateSingleStringValue(predicate)
				if err != nil {
					return nil, err
				}
				matches = matches && itemState != strings.ToUpper(value)
			case apiv2beta1.Predicate_IN:
				values, err := predicateMultiStringValues(predicate)
				if err != nil {
					return nil, err
				}
				matches = matches && containsStateFold(values, itemState)
			default:
				return nil, status.Error(codes.InvalidArgument,
					"Only EQUALS, NOT_EQUALS and IN operations are supported when filtering run tasks and executions by state")
			}
			if !matches {
				break
			}
		}
		if matches {
			kept = append(kept, i)
		}
	}
	return kept, nil
}

func predicateSingleStringValue(predicate *apiv2beta1.Predicate) (string, error) {
	if value, ok := predicate.GetValue().(*apiv2beta1.Predicate_StringValue); ok {
		return value.StringValue, nil
	}
	return "", status.Error(codes.InvalidArgument,
		"Filtering run tasks and executions by state requires a string_value predicate")
}

func predicateMultiStringValues(predicate *apiv2beta1.Predicate) ([]string, error) {
	if value, ok := predicate.GetValue().(*apiv2beta1.Predicate_StringValues_); ok {
		return value.StringValues.GetValues(), nil
	}
	return nil, status.Error(codes.InvalidArgument,
		"Filtering run tasks and executions by state with IN requires string_values")
}

func containsStateFold(values []string, state string) bool {
	for _, value := range values {
		if strings.ToUpper(value) == state {
			return true
		}
	}
	return false
}

func writeRunMetadataHttpError(ctx context.Context, gatewayMux *runtime.ServeMux, w http.ResponseWriter, r *http.Request, err error) {
	runtime.DefaultHTTPErrorHandler(ctx, gatewayMux, common.CustomMarshaler(), w, r, err)
}

func writeRunMetadataHttpResponse(w http.ResponseWriter, message proto.Message) {
	marshaler := common.CustomMarshaler()
	w.Header().Set("Content-Type", marshaler.ContentType(message))
	payload, err := marshaler.Marshal(message)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code": 13, "message": "failed to marshal response"}`))
		return
	}
	_, _ = w.Write(payload)
}
