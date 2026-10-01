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
	"testing"

	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func parsedQueryFilter(t *testing.T, r *http.Request) *apiv2beta1.Filter {
	t.Helper()
	parsed, err := parseAPIFilter(r.URL.Query().Get("filter"), "v2beta1")
	if err != nil {
		t.Fatalf("failed to parse rewritten filter: %+v", err)
	}
	filterProto, ok := parsed.(*apiv2beta1.Filter)
	if !ok || filterProto == nil {
		t.Fatalf("rewritten filter is not a v2beta1 filter: %#v", parsed)
	}
	return filterProto
}

func TestRewriteShorthandFilters(t *testing.T) {
	t.Run("comma-separated values become one IN predicate", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet,
			"/apis/v2beta1/runs?state=running,FAILED&page_size=10", nil)
		rewritten, err := rewriteShorthandFilters(request, "state")
		assert.Nil(t, err)

		query := rewritten.URL.Query()
		assert.Empty(t, query.Get("state"), "shorthand parameter must be removed")
		assert.Equal(t, "10", query.Get("page_size"), "other parameters must be preserved")

		filterProto := parsedQueryFilter(t, rewritten)
		assert.Len(t, filterProto.GetPredicates(), 1)
		predicate := filterProto.GetPredicates()[0]
		assert.Equal(t, "state", predicate.GetKey())
		assert.Equal(t, apiv2beta1.Predicate_IN, predicate.GetOperation())
		assert.Equal(t, []string{"RUNNING", "FAILED"}, predicate.GetStringValues().GetValues())
	})

	t.Run("values are upper-cased and trimmed", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet,
			"/apis/v2beta1/runs?state=succeeded%20,%20pending", nil)
		rewritten, err := rewriteShorthandFilters(request, "state")
		assert.Nil(t, err)
		filterProto := parsedQueryFilter(t, rewritten)
		assert.Equal(t, []string{"SUCCEEDED", "PENDING"},
			filterProto.GetPredicates()[0].GetStringValues().GetValues())
	})

	t.Run("merges with an existing filter parameter", func(t *testing.T) {
		existingFilter := `{"predicates":[{"operation":"EQUALS","key":"display_name","string_value":"train"}]}`
		request := httptest.NewRequest(http.MethodGet,
			"/apis/v2beta1/runs?state=RUNNING&filter="+existingFilter, nil)
		rewritten, err := rewriteShorthandFilters(request, "state")
		assert.Nil(t, err)
		filterProto := parsedQueryFilter(t, rewritten)
		assert.Len(t, filterProto.GetPredicates(), 2)
		assert.Equal(t, "display_name", filterProto.GetPredicates()[0].GetKey())
		assert.Equal(t, apiv2beta1.Predicate_EQUALS, filterProto.GetPredicates()[0].GetOperation())
		assert.Equal(t, "state", filterProto.GetPredicates()[1].GetKey())
	})

	t.Run("no shorthand parameters leaves the request unchanged", func(t *testing.T) {
		rawQuery := "page_size=10&sort_by=created_at%20desc"
		request := httptest.NewRequest(http.MethodGet, "/apis/v2beta1/runs?"+rawQuery, nil)
		rewritten, err := rewriteShorthandFilters(request, "state")
		assert.Nil(t, err)
		assert.Equal(t, rawQuery, rewritten.URL.RawQuery)
	})

	t.Run("empty shorthand value is treated as absent", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/apis/v2beta1/runs?state=", nil)
		rewritten, err := rewriteShorthandFilters(request, "state")
		assert.Nil(t, err)
		assert.Equal(t, "state=", rewritten.URL.RawQuery)
	})

	t.Run("shorthand with no values is rejected", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/apis/v2beta1/runs?state=,,", nil)
		_, err := rewriteShorthandFilters(request, "state")
		assert.NotNil(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Convert(err).Code())
	})

	t.Run("supports mode and status keys for recurring runs", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet,
			"/apis/v2beta1/recurringruns?mode=disabled&status=ENABLED", nil)
		rewritten, err := rewriteShorthandFilters(request, "mode", "status")
		assert.Nil(t, err)
		assert.Empty(t, rewritten.URL.Query().Get("mode"))
		assert.Empty(t, rewritten.URL.Query().Get("status"))
		filterProto := parsedQueryFilter(t, rewritten)
		assert.Len(t, filterProto.GetPredicates(), 2)
		assert.Equal(t, "mode", filterProto.GetPredicates()[0].GetKey())
		assert.Equal(t, []string{"DISABLED"}, filterProto.GetPredicates()[0].GetStringValues().GetValues())
		assert.Equal(t, "status", filterProto.GetPredicates()[1].GetKey())
		assert.Equal(t, []string{"ENABLED"}, filterProto.GetPredicates()[1].GetStringValues().GetValues())
	})
}
