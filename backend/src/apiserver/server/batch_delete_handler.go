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

// BatchDeleteRunsHttpHandler serves POST /apis/v2beta1/runs/delete.
// BatchDeletePipelineVersionsHttpHandler serves POST /apis/v2beta1/pipelines/versions:batchDelete.
//
// Both handlers process items concurrently and return partial results — HTTP 200
// always; failed items are listed in the errors array.
//
// Pipeline version batch delete supports ?cascade=true: for each version, all
// associated runs are deleted before the version itself is removed. Without
// cascade, a version that still has runs is reported as an error.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

// BatchDeleteRunsHttpHandler serves POST /apis/v2beta1/runs/delete.
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

// --- Batch delete pipeline versions ---

type batchDeletePipelineVersionsRequest struct {
	PipelineVersionIDs []string `json:"pipeline_version_ids"`
}

type batchDeletePipelineVersionError struct {
	PipelineVersionID string `json:"pipeline_version_id"`
	Error             string `json:"error"`
}

type batchDeletePipelineVersionsResponse struct {
	DeletedPipelineVersionIDs []string                          `json:"deleted_pipeline_version_ids"`
	Errors                    []batchDeletePipelineVersionError `json:"errors,omitempty"`
}

// BatchDeletePipelineVersionsHttpHandler serves
// POST /apis/v2beta1/pipelines/versions/delete.
//
// With ?cascade=true each version's runs are deleted before the version itself.
// Without cascade, a version that still has runs is reported as an error and
// left untouched. Successfully deleted IDs are in deleted_pipeline_version_ids.
func (s *PipelineServer) BatchDeletePipelineVersionsHttpHandler(w http.ResponseWriter, r *http.Request) {
	cascade := strings.EqualFold(r.URL.Query().Get("cascade"), "true")

	var req batchDeletePipelineVersionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if len(req.PipelineVersionIDs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "pipeline_version_ids must contain at least one ID"})
		return
	}

	type result struct {
		id  string
		err error
	}
	resultCh := make(chan result, len(req.PipelineVersionIDs))

	var wg sync.WaitGroup
	for _, id := range req.PipelineVersionIDs {
		wg.Add(1)
		go func(versionID string) {
			defer wg.Done()

			// Check / cascade-delete associated runs.
			runIDs, err := s.resourceManager.ListRunIDsByPipelineVersion(versionID)
			if err != nil {
				resultCh <- result{id: versionID, err: fmt.Errorf("failed to list runs: %w", err)}
				return
			}
			if len(runIDs) > 0 {
				if !cascade {
					resultCh <- result{id: versionID, err: fmt.Errorf(
						"pipeline version has %d associated run(s); retry with ?cascade=true to delete them", len(runIDs))}
					return
				}
				if runErrs := deleteRunsConcurrently(r, s.resourceManager, runIDs); len(runErrs) > 0 {
					resultCh <- result{id: versionID, err: fmt.Errorf("failed to delete runs: %v", runErrs)}
					return
				}
			}

			// Delete the version itself.
			if err := s.resourceManager.DeletePipelineVersion(versionID); err != nil {
				resultCh <- result{id: versionID, err: err}
				return
			}
			resultCh <- result{id: versionID}
		}(id)
	}
	wg.Wait()
	close(resultCh)

	resp := batchDeletePipelineVersionsResponse{
		DeletedPipelineVersionIDs: []string{},
	}
	for res := range resultCh {
		if res.err != nil {
			resp.Errors = append(resp.Errors, batchDeletePipelineVersionError{
				PipelineVersionID: res.id,
				Error:             res.err.Error(),
			})
		} else {
			resp.DeletedPipelineVersionIDs = append(resp.DeletedPipelineVersionIDs, res.id)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
