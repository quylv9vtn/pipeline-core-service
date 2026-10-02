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

// BatchDeleteRunsHttpHandler serves
//
//	POST /apis/v2beta1/runs:batchDelete
//
// Request body:
//
//	{"run_ids": ["id1", "id2", ...]}
//
// Response body:
//
//	{"deleted_run_ids": ["id1", "id2"], "errors": [{"run_id": "id3", "error": "not found"}]}
//
// Runs are deleted concurrently. Successfully deleted IDs are listed in
// deleted_run_ids. Any failures are listed in errors; the HTTP status is still
// 200 so the caller can inspect partial results.

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/kubeflow/pipelines/backend/src/common/util"
)

type batchDeleteRunsRequest struct {
	RunIDs []string `json:"run_ids"`
}

type batchDeleteRunError struct {
	RunID string `json:"run_id"`
	Error string `json:"error"`
}

type batchDeleteRunsResponse struct {
	DeletedRunIDs []string              `json:"deleted_run_ids"`
	Errors        []batchDeleteRunError `json:"errors,omitempty"`
}

// BatchDeleteRunsHttpHandler serves POST /apis/v2beta1/runs:batchDelete.
func (s *RunServer) BatchDeleteRunsHttpHandler(w http.ResponseWriter, r *http.Request) {
	var req batchDeleteRunsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if len(req.RunIDs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "run_ids must contain at least one ID"})
		return
	}

	type result struct {
		id  string
		err error
	}
	resultCh := make(chan result, len(req.RunIDs))

	var wg sync.WaitGroup
	for _, id := range req.RunIDs {
		wg.Add(1)
		go func(runID string) {
			defer wg.Done()
			err := s.deleteRun(r.Context(), runID)
			resultCh <- result{id: runID, err: err}
		}(id)
	}
	wg.Wait()
	close(resultCh)

	resp := batchDeleteRunsResponse{
		DeletedRunIDs: []string{},
	}
	for res := range resultCh {
		if res.err != nil {
			resp.Errors = append(resp.Errors, batchDeleteRunError{
				RunID: res.id,
				Error: res.err.Error(),
			})
		} else {
			resp.DeletedRunIDs = append(resp.DeletedRunIDs, res.id)
			if s.options.CollectMetrics {
				deleteRunRequests.Inc()
				if util.GetMetricValue(runCount) > 0 {
					runCount.Dec()
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
