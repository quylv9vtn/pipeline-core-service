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
// BatchDeletePipelineVersionsHttpHandler serves POST /apis/v2beta1/pipelines/versions/delete.
// BatchDeletePipelinesHttpHandler serves POST /apis/v2beta1/pipelines/delete.
//
// All handlers process items concurrently and return partial results — HTTP 200
// always; failed items are listed in the errors array.
//
// Pipeline batch delete supports ?cascade=true: for each pipeline, every version
// is deleted (along with all its runs) before the pipeline record itself is
// removed. Without cascade, a pipeline that still has versions is reported as
// an error and left untouched.
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

// --- Batch delete pipelines ---

type batchDeletePipelinesRequest struct {
	PipelineIDs []string `json:"pipeline_ids"`
}

type batchDeletePipelineError struct {
	PipelineID string `json:"pipeline_id"`
	Error      string `json:"error"`
}

type batchDeletePipelinesResponse struct {
	DeletedPipelineIDs []string                   `json:"deleted_pipeline_ids"`
	Errors             []batchDeletePipelineError `json:"errors,omitempty"`
}

// BatchDeletePipelinesHttpHandler serves POST /apis/v2beta1/pipelines/delete.
//
// With ?cascade=true every version (and its runs) is deleted before the
// pipeline itself is removed. Without cascade, a pipeline that still has
// versions is reported as an error and left untouched.
// Successfully deleted IDs are in deleted_pipeline_ids.
func (s *PipelineServer) BatchDeletePipelinesHttpHandler(w http.ResponseWriter, r *http.Request) {
	cascade := strings.EqualFold(r.URL.Query().Get("cascade"), "true")

	var req batchDeletePipelinesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body: " + err.Error()})
		return
	}
	if len(req.PipelineIDs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "pipeline_ids must contain at least one ID"})
		return
	}

	type result struct {
		id  string
		err error
	}
	resultCh := make(chan result, len(req.PipelineIDs))

	var wg sync.WaitGroup
	for _, id := range req.PipelineIDs {
		wg.Add(1)
		go func(pipelineID string) {
			defer wg.Done()

			// List all version IDs for this pipeline.
			versionIDs, err := s.resourceManager.ListAllPipelineVersionIDsByPipeline(pipelineID)
			if err != nil {
				resultCh <- result{id: pipelineID, err: fmt.Errorf("failed to list versions: %w", err)}
				return
			}

			if len(versionIDs) > 0 {
				if !cascade {
					resultCh <- result{id: pipelineID, err: fmt.Errorf(
						"pipeline has %d version(s); retry with ?cascade=true to delete them along with all associated runs",
						len(versionIDs))}
					return
				}

				// Cascade: delete runs then version for each version concurrently.
				type versionErr struct {
					vid string
					err error
				}
				vErrCh := make(chan versionErr, len(versionIDs))
				var vWg sync.WaitGroup
				for _, vid := range versionIDs {
					vWg.Add(1)
					go func(versionID string) {
						defer vWg.Done()
						runIDs, listErr := s.resourceManager.ListRunIDsByPipelineVersion(versionID)
						if listErr != nil {
							vErrCh <- versionErr{versionID, fmt.Errorf("list runs: %w", listErr)}
							return
						}
						if len(runIDs) > 0 {
							if runErrs := deleteRunsConcurrently(r, s.resourceManager, runIDs); len(runErrs) > 0 {
								vErrCh <- versionErr{versionID, fmt.Errorf("delete runs: %v", runErrs)}
								return
							}
						}
						if delErr := s.resourceManager.DeletePipelineVersion(versionID); delErr != nil {
							vErrCh <- versionErr{versionID, fmt.Errorf("delete version: %w", delErr)}
						}
					}(vid)
				}
				vWg.Wait()
				close(vErrCh)

				var vErrs []string
				for ve := range vErrCh {
					vErrs = append(vErrs, fmt.Sprintf("version %s: %v", ve.vid, ve.err))
				}
				if len(vErrs) > 0 {
					resultCh <- result{id: pipelineID, err: fmt.Errorf("cascade delete failed: %s", strings.Join(vErrs, "; "))}
					return
				}
			}

			// Delete the pipeline itself (versions already gone, cascade=false is safe).
			if err := s.resourceManager.DeletePipeline(pipelineID, false); err != nil {
				resultCh <- result{id: pipelineID, err: err}
				return
			}
			resultCh <- result{id: pipelineID}
		}(id)
	}
	wg.Wait()
	close(resultCh)

	resp := batchDeletePipelinesResponse{
		DeletedPipelineIDs: []string{},
	}
	for res := range resultCh {
		if res.err != nil {
			resp.Errors = append(resp.Errors, batchDeletePipelineError{
				PipelineID: res.id,
				Error:      res.err.Error(),
			})
		} else {
			resp.DeletedPipelineIDs = append(resp.DeletedPipelineIDs, res.id)
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
