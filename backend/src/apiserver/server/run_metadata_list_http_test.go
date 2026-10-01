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
	"net/http/httptest"
	"net/url"
	"testing"

	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFilterIndicesByState(t *testing.T) {
	states := []string{"RUNNING", "SUCCEEDED", "FAILED", "SUCCEEDED"}
	stateOf := func(i int) string { return states[i] }

	t.Run("equals is case-insensitive", func(t *testing.T) {
		kept, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "state",
				Operation: apiv2beta1.Predicate_EQUALS,
				Value:     &apiv2beta1.Predicate_StringValue{StringValue: "succeeded"},
			}},
		})
		assert.Nil(t, err)
		assert.Equal(t, []int{1, 3}, kept)
	})

	t.Run("not equals", func(t *testing.T) {
		kept, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "state",
				Operation: apiv2beta1.Predicate_NOT_EQUALS,
				Value:     &apiv2beta1.Predicate_StringValue{StringValue: "SUCCEEDED"},
			}},
		})
		assert.Nil(t, err)
		assert.Equal(t, []int{0, 2}, kept)
	})

	t.Run("in", func(t *testing.T) {
		kept, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "state",
				Operation: apiv2beta1.Predicate_IN,
				Value: &apiv2beta1.Predicate_StringValues_{StringValues: &apiv2beta1.Predicate_StringValues{
					Values: []string{"running", "FAILED"},
				}},
			}},
		})
		assert.Nil(t, err)
		assert.Equal(t, []int{0, 2}, kept)
	})

	t.Run("multiple predicates are AND-ed", func(t *testing.T) {
		kept, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{
				{
					Key:       "state",
					Operation: apiv2beta1.Predicate_NOT_EQUALS,
					Value:     &apiv2beta1.Predicate_StringValue{StringValue: "RUNNING"},
				},
				{
					Key:       "state",
					Operation: apiv2beta1.Predicate_IN,
					Value: &apiv2beta1.Predicate_StringValues_{StringValues: &apiv2beta1.Predicate_StringValues{
						Values: []string{"SUCCEEDED", "FAILED"},
					}},
				},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, []int{1, 2, 3}, kept)
	})

	t.Run("unsupported key is rejected", func(t *testing.T) {
		_, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "display_name",
				Operation: apiv2beta1.Predicate_EQUALS,
				Value:     &apiv2beta1.Predicate_StringValue{StringValue: "train"},
			}},
		})
		assert.NotNil(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Convert(err).Code())
	})

	t.Run("unsupported operation is rejected", func(t *testing.T) {
		_, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "state",
				Operation: apiv2beta1.Predicate_GREATER_THAN,
				Value:     &apiv2beta1.Predicate_StringValue{StringValue: "RUNNING"},
			}},
		})
		assert.NotNil(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Convert(err).Code())
	})

	t.Run("non-string value is rejected", func(t *testing.T) {
		_, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "state",
				Operation: apiv2beta1.Predicate_EQUALS,
				Value:     &apiv2beta1.Predicate_IntValue{IntValue: 2},
			}},
		})
		assert.NotNil(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Convert(err).Code())
	})

	t.Run("IN without string_values is rejected", func(t *testing.T) {
		_, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "state",
				Operation: apiv2beta1.Predicate_IN,
				Value:     &apiv2beta1.Predicate_LongValues_{},
			}},
		})
		assert.NotNil(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Convert(err).Code())
	})

	t.Run("empty filter keeps everything", func(t *testing.T) {
		kept, err := filterIndicesByState(len(states), stateOf, &apiv2beta1.Filter{})
		assert.Nil(t, err)
		assert.Equal(t, []int{0, 1, 2, 3}, kept)
	})
}

func TestParseListHttpStateFilter(t *testing.T) {
	t.Run("parses encoded filter JSON", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet,
			"/apis/v2beta1/runs/run-1/tasks?filter="+url.QueryEscape(
				`{"predicates":[{"operation":"EQUALS","key":"state","string_value":"SUCCEEDED"}]}`), nil)
		filterProto, err := parseListHttpStateFilter(request)
		assert.Nil(t, err)
		assert.Len(t, filterProto.GetPredicates(), 1)
		assert.Equal(t, "state", filterProto.GetPredicates()[0].GetKey())
		assert.Equal(t, "SUCCEEDED", filterProto.GetPredicates()[0].GetStringValue())
	})

	t.Run("rejects malformed filter JSON", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet,
			"/apis/v2beta1/runs/run-1/tasks?filter="+url.QueryEscape("not-json"), nil)
		_, err := parseListHttpStateFilter(request)
		assert.NotNil(t, err)
	})
}

func TestWriteRunMetadataHttpResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeRunMetadataHttpResponse(recorder, &apiv2beta1.ListRunTasksResponse{
		RunId:     "run-1",
		Tasks:     []*apiv2beta1.PipelineTaskDetail{{TaskId: "task-1"}},
		TotalSize: 1,
	})
	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Contains(t, recorder.Header().Get("Content-Type"), "application/json")
	// The gateway marshaler uses proto field names (snake_case).
	assert.JSONEq(t,
		`{"run_id": "run-1", "tasks": [{"task_id": "task-1"}], "total_size": 1}`,
		recorder.Body.String())
}
