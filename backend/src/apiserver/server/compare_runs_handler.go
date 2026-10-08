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

// CompareRunsHttpHandler serves GET /apis/v2beta1/runs/compare.
//
// Query parameters:
//
//	?run_ids=<id1>,<id2>,...   comma-separated list of run IDs to compare (required, 2-10)
//	                           Alternatively, repeat ?run_id=<id> for each run.
//
// Constraints:
//
//	- At least 2, at most 10 run IDs.
//	- All runs must belong to the same pipeline_version_id.
//	- All runs must be in a finished state (SUCCEEDED, FAILED, or CANCELED).
//
// Response:
//
//	{
//	  "pipeline_version_id": "...",
//	  "runs": [
//	    {
//	      "run_id":        "...",
//	      "display_name":  "my-run",
//	      "create_time":   1700000000,
//	      "start_time":    1700000001,
//	      "end_time":      1700000120,
//	      "duration":      119,        ← seconds; 0 if start/end not recorded
//	      "pipeline_root": "gs://bucket/root",
//	      "steps": [
//	        {
//	          "task_id":          "...",
//	          "display_name":     "train-step",
//	          "state":            "COMPLETE",
//	          "input_artifacts":  [{"artifact_id":1,"name":"dataset","type_name":"system.Dataset","uri":"gs://...","state":"LIVE"}],
//	          "output_artifacts": [...],
//	          "properties":       {...},
//	          "custom_properties": {...}
//	        }
//	      ]
//	    }
//	  ]
//	}

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	compareRunsMaxRuns     = 10
	compareRunsMlmdTimeout = 30 * time.Second
)

// finishedRunStates lists the terminal states a run must be in to be comparable.
var finishedRunStates = map[model.RuntimeState]bool{
	model.RuntimeStateSucceeded: true,
	model.RuntimeStateFailed:    true,
	model.RuntimeStateCanceled:  true,
}

// compareArtifact is the slim artifact shape used in the compare response.
type compareArtifact struct {
	ArtifactID int64  `json:"artifact_id"`
	Name       string `json:"name,omitempty"`
	TypeName   string `json:"type_name,omitempty"`
	URI        string `json:"uri,omitempty"`
	State      string `json:"state,omitempty"`
}

// compareRunStep is the per-step element inside a compare run entry.
type compareRunStep struct {
	TaskID           string           `json:"task_id"`
	DisplayName      string           `json:"display_name,omitempty"`
	State            string           `json:"state,omitempty"`
	InputArtifacts   []compareArtifact `json:"input_artifacts,omitempty"`
	OutputArtifacts  []compareArtifact `json:"output_artifacts,omitempty"`
	Properties       json.RawMessage  `json:"properties,omitempty"`
	CustomProperties json.RawMessage  `json:"custom_properties,omitempty"`
}

// compareRunEntry is the per-run element of the compare response.
type compareRunEntry struct {
	RunID        string           `json:"run_id"`
	DisplayName  string           `json:"display_name"`
	CreateTime   int64            `json:"create_time"`
	StartTime    int64            `json:"start_time,omitempty"`
	EndTime      int64            `json:"end_time,omitempty"`
	Duration     int64            `json:"duration"`
	PipelineRoot string           `json:"pipeline_root,omitempty"`
	Steps        []compareRunStep `json:"steps,omitempty"`
}

// compareRunsResponse is the top-level response returned by CompareRunsHttpHandler.
type compareRunsResponse struct {
	PipelineVersionID string            `json:"pipeline_version_id"`
	Runs              []compareRunEntry `json:"runs"`
}

// CompareRunsHttpHandler serves GET /apis/v2beta1/runs/compare.
func (s *RunServer) CompareRunsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := r.URL.Query()

		// Collect run IDs from ?run_ids=a,b,c  and/or repeated ?run_id=a&run_id=b.
		var runIDs []string
		if raw := strings.TrimSpace(q.Get("run_ids")); raw != "" {
			for _, id := range strings.Split(raw, ",") {
				if id = strings.TrimSpace(id); id != "" {
					runIDs = append(runIDs, id)
				}
			}
		}
		for _, id := range q["run_id"] {
			if id = strings.TrimSpace(id); id != "" {
				runIDs = append(runIDs, id)
			}
		}

		// Deduplicate while preserving order.
		seen := make(map[string]bool, len(runIDs))
		deduped := runIDs[:0]
		for _, id := range runIDs {
			if !seen[id] {
				seen[id] = true
				deduped = append(deduped, id)
			}
		}
		runIDs = deduped

		if len(runIDs) < 2 {
			writeCompareRunsError(w, gatewayMux, r,
				status.Errorf(codes.InvalidArgument, "at least 2 run IDs are required (got %d)", len(runIDs)))
			return
		}
		if len(runIDs) > compareRunsMaxRuns {
			writeCompareRunsError(w, gatewayMux, r,
				status.Errorf(codes.InvalidArgument, "at most %d runs can be compared at once (got %d)", compareRunsMaxRuns, len(runIDs)))
			return
		}

		// Fetch all runs and validate constraints.
		runs := make([]*model.Run, 0, len(runIDs))
		var pipelineVersionID string
		for _, runID := range runIDs {
			run, err := s.getRun(ctx, runID)
			if err != nil {
				writeCompareRunsError(w, gatewayMux, r, err)
				return
			}

			// Validate: run must be in a terminal (finished) state.
			stateV2 := model.RuntimeState(run.RunDetails.State).ToV2()
			if !finishedRunStates[stateV2] {
				writeCompareRunsError(w, gatewayMux, r,
					status.Errorf(codes.FailedPrecondition,
						"run %q is not finished (current state: %s); only SUCCEEDED, FAILED, or CANCELED runs can be compared",
						runID, run.RunDetails.State))
				return
			}

			// Validate: all runs must share the same pipeline version.
			pvID := run.PipelineSpec.PipelineVersionId
			if pipelineVersionID == "" {
				pipelineVersionID = pvID
			} else if pvID != pipelineVersionID {
				writeCompareRunsError(w, gatewayMux, r,
					status.Errorf(codes.InvalidArgument,
						"all runs must belong to the same pipeline version; run %q has version %q but expected %q",
						runID, pvID, pipelineVersionID))
				return
			}

			runs = append(runs, run)
		}

		// Build per-run comparison data with a shared MLMD timeout.
		mlmdCtx, cancel := context.WithTimeout(ctx, compareRunsMlmdTimeout)
		defer cancel()

		entries := make([]compareRunEntry, 0, len(runs))
		for _, run := range runs {
			entry := compareRunEntry{
				RunID:        run.UUID,
				DisplayName:  run.DisplayName,
				CreateTime:   run.RunDetails.CreatedAtInSec,
				StartTime:    run.RunDetails.ScheduledAtInSec,
				EndTime:      run.RunDetails.FinishedAtInSec,
				PipelineRoot: string(run.RuntimeConfig.PipelineRoot),
			}
			if entry.EndTime > 0 && entry.StartTime > 0 {
				entry.Duration = entry.EndTime - entry.StartTime
			}

			// Load step executions from MLMD. Non-fatal on failure.
			executions, execErr := s.loadRunExecutions(mlmdCtx, run.UUID, run.RunDetails.TaskDetails)
			if execErr != nil {
				glog.Warningf("CompareRunsHttpHandler: failed to load MLMD executions for run %s: %v", run.UUID, execErr)
			} else {
				// Build task UUID → display_name lookup from DB task records.
				taskDisplayName := make(map[string]string, len(run.RunDetails.TaskDetails))
				for _, t := range run.RunDetails.TaskDetails {
					taskDisplayName[t.UUID] = t.Name
				}

				steps := make([]compareRunStep, 0, len(executions))
				for _, ex := range executions {
					step := compareRunStep{
						TaskID:          ex.GetTaskId(),
						DisplayName:     taskDisplayName[ex.GetTaskId()],
						State:           ex.GetState(),
						InputArtifacts:  toCompareArtifacts(ex.GetInputArtifacts()),
						OutputArtifacts: toCompareArtifacts(ex.GetOutputArtifacts()),
					}
					if props := ex.GetProperties(); props != nil {
						if b, marshalErr := protojson.Marshal(props); marshalErr == nil {
							step.Properties = b
						}
					}
					if custom := ex.GetCustomProperties(); custom != nil {
						if b, marshalErr := protojson.Marshal(custom); marshalErr == nil {
							step.CustomProperties = b
						}
					}
					steps = append(steps, step)
				}
				entry.Steps = steps
			}

			entries = append(entries, entry)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(compareRunsResponse{
			PipelineVersionID: pipelineVersionID,
			Runs:              entries,
		})
	}
}

// toCompareArtifacts converts RunArtifact protos to the slim compareArtifact shape.
func toCompareArtifacts(artifacts []*apiv2beta1.RunArtifact) []compareArtifact {
	if len(artifacts) == 0 {
		return nil
	}
	result := make([]compareArtifact, 0, len(artifacts))
	for _, a := range artifacts {
		result = append(result, compareArtifact{
			ArtifactID: a.GetArtifactId(),
			Name:       a.GetName(),
			TypeName:   a.GetTypeName(),
			URI:        a.GetUri(),
			State:      a.GetState(),
		})
	}
	return result
}

func writeCompareRunsError(w http.ResponseWriter, gatewayMux *runtime.ServeMux, r *http.Request, err error) {
	writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
}
