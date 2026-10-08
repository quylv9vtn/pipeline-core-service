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

// Enriched HTTP handlers for Run and RecurringRun (Job).
//
// These handlers augment the standard proto-JSON response with extra fields
// that are not part of the KFP proto schema:
//
//   pipeline_display_name        — DisplayName of the pipeline
//   pipeline_version_display_name — DisplayName of the pipeline version
//   experiment_display_name      — Name of the experiment
//   duration                     — seconds between created_at and finished_at
//                                  (null if either timestamp is missing; Run only)
//
// All name lookups are batched into a single DB round-trip per entity type.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
)

// ---- shared enrichment helpers ----

type enrichmentMaps struct {
	pipelineNames        map[string]string
	pipelineVersionNames map[string]string
	experimentNames      map[string]string
}

// buildEnrichmentMaps fetches display names for all unique IDs in a single
// round-trip per table. Any ID not found resolves to an empty string.
func buildEnrichmentMaps(rm enrichableRM, pipelineIDs, versionIDs, experimentIDs []string) (enrichmentMaps, error) {
	pNames, err := rm.GetPipelineDisplayNames(unique(pipelineIDs))
	if err != nil {
		return enrichmentMaps{}, err
	}
	vNames, err := rm.GetPipelineVersionDisplayNames(unique(versionIDs))
	if err != nil {
		return enrichmentMaps{}, err
	}
	eNames, err := rm.GetExperimentDisplayNames(unique(experimentIDs))
	if err != nil {
		return enrichmentMaps{}, err
	}
	return enrichmentMaps{
		pipelineNames:        pNames,
		pipelineVersionNames: vNames,
		experimentNames:      eNames,
	}, nil
}

// enrichableRM is a subset of *resource.ResourceManager used by the enrichment helpers.
type enrichableRM interface {
	GetPipelineDisplayNames(ids []string) (map[string]string, error)
	GetPipelineVersionDisplayNames(ids []string) (map[string]string, error)
	GetExperimentDisplayNames(ids []string) (map[string]string, error)
}

// unique returns a deduplicated, non-empty slice of strings.
func unique(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// enrichedRunMarshaler emits all proto fields (including empty strings) so
// that callers always receive a predictable shape.
var enrichedRunMarshaler = common.CustomMarshaler()

// injectRunExtras marshals a model.Run to JSON and injects the extra fields.
func injectRunExtras(run *model.Run, em enrichmentMaps) (json.RawMessage, error) {
	apiRun := toApiRun(run)
	raw, err := enrichedRunMarshaler.Marshal(apiRun)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}

	m["pipeline_display_name"] = em.pipelineNames[run.PipelineSpec.PipelineId]
	m["pipeline_version_display_name"] = em.pipelineVersionNames[run.PipelineSpec.PipelineVersionId]
	m["experiment_display_name"] = em.experimentNames[run.ExperimentId]

	// duration in seconds — null when either timestamp is zero
	if run.CreatedAtInSec != 0 && run.FinishedAtInSec != 0 {
		m["duration"] = run.FinishedAtInSec - run.CreatedAtInSec
	} else {
		m["duration"] = nil
	}

	return json.Marshal(m)
}

// injectJobExtras marshals a model.Job to JSON and injects the extra fields.
func injectJobExtras(job *model.Job, em enrichmentMaps) (json.RawMessage, error) {
	apiJob := toApiRecurringRun(job)
	raw, err := enrichedRunMarshaler.Marshal(apiJob)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}

	m["pipeline_display_name"] = em.pipelineNames[job.PipelineSpec.PipelineId]
	m["pipeline_version_display_name"] = em.pipelineVersionNames[job.PipelineSpec.PipelineVersionId]
	m["experiment_display_name"] = em.experimentNames[job.ExperimentId]

	return json.Marshal(m)
}

// writeEnrichedJSON writes the JSON payload with Content-Type set.
func writeEnrichedJSON(w http.ResponseWriter, payload []byte) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(payload)
}

// ---- Run handlers ----

// GetRunHttpHandler serves GET /apis/v2beta1/runs/{run_id}.
// Adds pipeline/version/experiment display names and duration to the response.
func (s *RunServer) GetRunHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		run, err := s.getRun(r.Context(), mux.Vars(r)["run_id"])
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		em, err := buildEnrichmentMaps(s.resourceManager,
			[]string{run.PipelineSpec.PipelineId},
			[]string{run.PipelineSpec.PipelineVersionId},
			[]string{run.ExperimentId},
		)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		payload, err := injectRunExtras(run, em)
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		writeEnrichedJSON(w, payload)
	}
}

// ListRunsHttpHandler serves GET /apis/v2beta1/runs with optional shorthand
// filtering. Adds pipeline/version/experiment display names and duration to
// each run in the response.
func (s *RunServer) ListRunsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "state", "display_name", "name")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		r = rewritten
		q := r.URL.Query()

		opts, err := validatedListOptions(&model.Run{}, q.Get("page_token"), pageSize(q.Get("page_size")), q.Get("sort_by"), q.Get("filter"), "v2beta1")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		runs, totalSize, nextPageToken, err := s.listRuns(r.Context(), q.Get("page_token"), pageSize(q.Get("page_size")), q.Get("sort_by"), opts, q.Get("namespace"), q.Get("experiment_id"))
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		pIDs, vIDs, eIDs := collectRunIDs(runs)
		em, err := buildEnrichmentMaps(s.resourceManager, pIDs, vIDs, eIDs)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		items := make([]json.RawMessage, len(runs))
		for i, run := range runs {
			items[i], err = injectRunExtras(run, em)
			if err != nil {
				http.Error(w, `{"code":13,"message":"failed to marshal run"}`, http.StatusInternalServerError)
				return
			}
		}

		writeListResponse(w, "runs", items, totalSize, nextPageToken)
	}
}

// ---- Job (RecurringRun) handlers ----

// GetRecurringRunHttpHandler serves GET /apis/v2beta1/recurringruns/{recurring_run_id}.
// Adds pipeline/version/experiment display names to the response.
func (s *JobServer) GetRecurringRunHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job, err := s.getJob(r.Context(), mux.Vars(r)["recurring_run_id"])
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		em, err := buildEnrichmentMaps(s.resourceManager,
			[]string{job.PipelineSpec.PipelineId},
			[]string{job.PipelineSpec.PipelineVersionId},
			[]string{job.ExperimentId},
		)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		payload, err := injectJobExtras(job, em)
		if err != nil {
			http.Error(w, `{"code":13,"message":"failed to marshal response"}`, http.StatusInternalServerError)
			return
		}
		writeEnrichedJSON(w, payload)
	}
}

// ListRecurringRunsHttpHandler serves GET /apis/v2beta1/recurringruns with
// optional shorthand filtering. Adds pipeline/version/experiment display names
// to each recurring run in the response.
func (s *JobServer) ListRecurringRunsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rewritten, err := rewriteShorthandFilters(r, "mode", "status", "display_name", "name")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}
		r = rewritten
		q := r.URL.Query()

		opts, err := validatedListOptions(&model.Job{}, q.Get("page_token"), pageSize(q.Get("page_size")), q.Get("sort_by"), q.Get("filter"), "v2beta1")
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		jobs, totalSize, nextPageToken, err := s.listJobs(r.Context(), q.Get("page_token"), pageSize(q.Get("page_size")), q.Get("sort_by"), opts, q.Get("namespace"), q.Get("experiment_id"))
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		pIDs, vIDs, eIDs := collectJobIDs(jobs)
		em, err := buildEnrichmentMaps(s.resourceManager, pIDs, vIDs, eIDs)
		if err != nil {
			writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
			return
		}

		items := make([]json.RawMessage, len(jobs))
		for i, job := range jobs {
			items[i], err = injectJobExtras(job, em)
			if err != nil {
				http.Error(w, `{"code":13,"message":"failed to marshal job"}`, http.StatusInternalServerError)
				return
			}
		}

		writeListResponse(w, "recurring_runs", items, totalSize, nextPageToken)
	}
}

// ---- private helpers ----

func collectRunIDs(runs []*model.Run) (pipelineIDs, versionIDs, experimentIDs []string) {
	for _, r := range runs {
		pipelineIDs = append(pipelineIDs, r.PipelineSpec.PipelineId)
		versionIDs = append(versionIDs, r.PipelineSpec.PipelineVersionId)
		experimentIDs = append(experimentIDs, r.ExperimentId)
	}
	return
}

func collectJobIDs(jobs []*model.Job) (pipelineIDs, versionIDs, experimentIDs []string) {
	for _, j := range jobs {
		pipelineIDs = append(pipelineIDs, j.PipelineSpec.PipelineId)
		versionIDs = append(versionIDs, j.PipelineSpec.PipelineVersionId)
		experimentIDs = append(experimentIDs, j.ExperimentId)
	}
	return
}

// pageSize converts a page_size query string to an int. Returns 0 (server
// default) if the string is empty or unparsable.
func pageSize(s string) int {
	if s == "" {
		return 0
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0
	}
	return n
}

// writeListResponse writes a standard list JSON envelope.
func writeListResponse(w http.ResponseWriter, itemKey string, items []json.RawMessage, totalSize int, nextPageToken string) {
	m := map[string]interface{}{
		itemKey:    items,
		"total_size": totalSize,
	}
	if nextPageToken != "" {
		m["next_page_token"] = nextPageToken
	}
	payload, err := json.Marshal(m)
	if err != nil {
		http.Error(w, `{"code":13,"message":"failed to marshal list response"}`, http.StatusInternalServerError)
		return
	}
	writeEnrichedJSON(w, payload)
}

// Ensure time import is used (for potential future use).
var _ = time.Second
