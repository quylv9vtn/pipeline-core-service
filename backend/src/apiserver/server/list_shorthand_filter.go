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
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Shorthand query parameters let clients filter List requests with plain
// parameters instead of URL-encoded Filter JSON:
//
//	GET /apis/v2beta1/runs?state=RUNNING,FAILED
//	GET /apis/v2beta1/recurringruns?mode=DISABLED
//	GET /apis/v2beta1/recurringruns?status=ENABLED
//	GET /apis/v2beta1/runs/{run_id}/tasks?state=SUCCEEDED
//	GET /apis/v2beta1/runs/{run_id}/executions?state=COMPLETE,FAILED
//
// Each parameter holds a comma-separated list that is translated into one IN
// predicate on the same filter key. Values are upper-cased (states, mode and
// status are stored upper-case) and combined with an optional `filter`
// parameter as extra AND-ed predicates.

// rewriteShorthandFilters moves the given shorthand query parameters into the
// `filter` query parameter as Filter JSON. The grpc-gateway then binds the
// result to ListRunsRequest.filter / ListRecurringRunsRequest.filter, and the
// run metadata handlers apply the same JSON in memory for run tasks and
// executions. Requests without any shorthand parameter are returned unchanged.
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
				values = append(values, strings.ToUpper(value))
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

// ListRunsHttpHandler serves GET /apis/v2beta1/runs with optional shorthand
// state filtering, e.g. ?state=RUNNING,FAILED. Requests without shorthand
// parameters are forwarded to the grpc-gateway unchanged.
func (s *RunServer) ListRunsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "state")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		gatewayMux.ServeHTTP(w, rewritten)
	}
}

// ListRecurringRunsHttpHandler serves GET /apis/v2beta1/recurringruns with
// optional shorthand mode/status filtering, e.g. ?mode=DISABLED or
// ?status=ENABLED. Requests without shorthand parameters are forwarded to the
// grpc-gateway unchanged.
func (s *JobServer) ListRecurringRunsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "mode", "status")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		gatewayMux.ServeHTTP(w, rewritten)
	}
}
