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

// Cascade-delete HTTP handlers for Pipeline, PipelineVersion, RecurringRun, and Experiment.
//
// These handlers intercept DELETE routes before grpc-gateway to enforce
// referential-integrity rules before deletion.
//
// Pipeline / PipelineVersion / RecurringRun  support ?cascade=true:
//
//	Without ?cascade=true  — fails with 409 Conflict if associated entities exist.
//	With    ?cascade=true  — associated entities are deleted first, then the
//	                         request is forwarded to grpc-gateway.
//
// Experiment does NOT support cascade delete:
//
//	DELETE /apis/v2beta1/experiments/{experiment_id}
//	Fails with 409 Conflict if any job or run still references the experiment.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/kubeflow/pipelines/backend/src/apiserver/resource"
	"google.golang.org/grpc/codes"
)

// DeletePipelineHttpHandler serves DELETE /apis/v2beta1/pipelines/{pipeline_id}.
//
// With ?cascade=true it deletes every version of the pipeline and all runs that
// reference those versions before deleting the pipeline itself. Each version is
// processed concurrently: its runs are deleted first, then the version record.
func (s *PipelineServer) DeletePipelineHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineID := mux.Vars(r)["pipeline_id"]
		cascade := strings.EqualFold(r.URL.Query().Get("cascade"), "true")

		versionIDs, err := s.resourceManager.ListAllPipelineVersionIDsByPipeline(pipelineID)
		if err != nil {
			writeCascadeError(w, http.StatusInternalServerError,
				fmt.Sprintf("failed to list versions for pipeline %s: %v", pipelineID, err))
			return
		}

		if len(versionIDs) > 0 && !cascade {
			writeCascadeConflict(w,
				fmt.Sprintf("pipeline %s has %d version(s); retry with ?cascade=true to delete them along with the pipeline",
					pipelineID, len(versionIDs)))
			return
		}

		if len(versionIDs) > 0 {
			type versionErr struct {
				versionID string
				err       error
			}
			errCh := make(chan versionErr, len(versionIDs))
			var wg sync.WaitGroup
			for _, vid := range versionIDs {
				wg.Add(1)
				go func(versionID string) {
					defer wg.Done()
					// Delete all runs for this version first.
					runIDs, listErr := s.resourceManager.ListRunIDsByPipelineVersion(versionID)
					if listErr != nil {
						errCh <- versionErr{versionID, fmt.Errorf("list runs: %w", listErr)}
						return
					}
					if len(runIDs) > 0 {
						if runErrs := deleteRunsConcurrently(r, s.resourceManager, runIDs); len(runErrs) > 0 {
							errCh <- versionErr{versionID, fmt.Errorf("delete runs: %v", runErrs)}
							return
						}
					}
					// Delete the version itself.
					if delErr := s.resourceManager.DeletePipelineVersion(versionID); delErr != nil {
						errCh <- versionErr{versionID, fmt.Errorf("delete version: %w", delErr)}
					}
				}(vid)
			}
			wg.Wait()
			close(errCh)

			var errs []error
			for ve := range errCh {
				errs = append(errs, fmt.Errorf("version %s: %w", ve.versionID, ve.err))
			}
			if len(errs) > 0 {
				writeCascadeError(w, http.StatusInternalServerError,
					fmt.Sprintf("failed to delete versions/runs before removing pipeline %s: %v", pipelineID, errs))
				return
			}
		}

		// Forward to grpc-gateway for the actual pipeline deletion.
		gatewayMux.ServeHTTP(w, r)
	}
}

// DeletePipelineVersionHttpHandler serves
// DELETE /apis/v2beta1/pipelines/{pipeline_id}/versions/{pipeline_version_id}.
func (s *PipelineServer) DeletePipelineVersionHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelineVersionID := mux.Vars(r)["pipeline_version_id"]
		cascade := strings.EqualFold(r.URL.Query().Get("cascade"), "true")

		runIDs, err := s.resourceManager.ListRunIDsByPipelineVersion(pipelineVersionID)
		if err != nil {
			writeCascadeError(w, http.StatusInternalServerError,
				fmt.Sprintf("failed to list runs for pipeline version %s: %v", pipelineVersionID, err))
			return
		}

		if len(runIDs) > 0 && !cascade {
			writeCascadeConflict(w,
				fmt.Sprintf("pipeline version %s has %d associated run(s); retry with ?cascade=true to delete them along with the version",
					pipelineVersionID, len(runIDs)))
			return
		}

		if len(runIDs) > 0 {
			if errs := deleteRunsConcurrently(r, s.resourceManager, runIDs); len(errs) > 0 {
				writeCascadeError(w, http.StatusInternalServerError,
					fmt.Sprintf("failed to delete %d run(s) before removing pipeline version: %v", len(errs), errs))
				return
			}
		}

		// Forward to grpc-gateway for the actual pipeline version deletion.
		gatewayMux.ServeHTTP(w, r)
	}
}

// DeleteRecurringRunHttpHandler serves
// DELETE /apis/v2beta1/recurringruns/{recurring_run_id}.
func (s *JobServer) DeleteRecurringRunHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		recurringRunID := mux.Vars(r)["recurring_run_id"]
		cascade := strings.EqualFold(r.URL.Query().Get("cascade"), "true")

		runIDs, err := s.resourceManager.ListRunIDsByJob(recurringRunID)
		if err != nil {
			writeCascadeError(w, http.StatusInternalServerError,
				fmt.Sprintf("failed to list runs for recurring run %s: %v", recurringRunID, err))
			return
		}

		if len(runIDs) > 0 && !cascade {
			writeCascadeConflict(w,
				fmt.Sprintf("recurring run %s has %d associated run(s); retry with ?cascade=true to delete them along with the recurring run",
					recurringRunID, len(runIDs)))
			return
		}

		if len(runIDs) > 0 {
			if errs := deleteRunsConcurrently(r, s.resourceManager, runIDs); len(errs) > 0 {
				writeCascadeError(w, http.StatusInternalServerError,
					fmt.Sprintf("failed to delete %d run(s) before removing recurring run: %v", len(errs), errs))
				return
			}
		}

		// Forward to grpc-gateway for the actual job deletion.
		gatewayMux.ServeHTTP(w, r)
	}
}

// deleteRunsConcurrently deletes the given run IDs concurrently via resourceManager.
// Returns a slice of errors (empty if all succeeded).
func deleteRunsConcurrently(r *http.Request, rm *resource.ResourceManager, runIDs []string) []error {
	type result struct {
		id  string
		err error
	}
	ch := make(chan result, len(runIDs))
	var wg sync.WaitGroup
	for _, id := range runIDs {
		wg.Add(1)
		go func(runID string) {
			defer wg.Done()
			ch <- result{id: runID, err: rm.DeleteRun(r.Context(), runID)}
		}(id)
	}
	wg.Wait()
	close(ch)

	var errs []error
	for res := range ch {
		if res.err != nil {
			errs = append(errs, fmt.Errorf("run %s: %w", res.id, res.err))
		}
	}
	return errs
}

// DeleteExperimentHttpHandler serves DELETE /apis/v2beta1/experiments/{experiment_id}.
//
// Cascade delete is NOT supported. The request is rejected with 409 Conflict if
// any recurring run (job) or run still references this experiment.
func (s *ExperimentServer) DeleteExperimentHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		experimentID := mux.Vars(r)["experiment_id"]

		jobCount, err := s.resourceManager.CountJobsByExperiment(experimentID)
		if err != nil {
			writeCascadeError(w, http.StatusInternalServerError,
				fmt.Sprintf("failed to check recurring runs for experiment %s: %v", experimentID, err))
			return
		}
		if jobCount > 0 {
			writeCascadeConflict(w,
				fmt.Sprintf("experiment %s has %d recurring run(s); delete or reassign them before deleting the experiment",
					experimentID, jobCount))
			return
		}

		runCount, err := s.resourceManager.CountRunsByExperiment(experimentID)
		if err != nil {
			writeCascadeError(w, http.StatusInternalServerError,
				fmt.Sprintf("failed to check runs for experiment %s: %v", experimentID, err))
			return
		}
		if runCount > 0 {
			writeCascadeConflict(w,
				fmt.Sprintf("experiment %s has %d run(s); delete them before deleting the experiment",
					experimentID, runCount))
			return
		}

		// No references — forward to grpc-gateway for the actual deletion.
		gatewayMux.ServeHTTP(w, r)
	}
}

func writeCascadeConflict(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    int(codes.FailedPrecondition),
		"message": message,
	})
}

func writeCascadeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"code":    int(codes.Internal),
		"message": message,
	})
}
