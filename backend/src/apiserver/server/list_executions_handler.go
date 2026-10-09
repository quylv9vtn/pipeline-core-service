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

// ListExecutionsHttpHandler serves GET /apis/v2beta1/executions — an
// aggregate view of MLMD executions across all runs.
//
// Query parameters:
//
//	?namespace=<ns>         filter by Kubernetes namespace (optional)
//	?run_id=<id>            filter by run (optional)
//	?state=<S1,S2,...>      filter by MLMD state (optional, comma-separated,
//	                        case-insensitive — e.g. COMPLETE, FAILED, NEW, RUNNING)
//	?page_size=<N>          max tasks to query (default 20, max 1000)
//	?page_token=<tok>       continuation token from a previous response
//
// Response:
//
//	{
//	  "executions": [{"run_id":"...","execution_id":1,"state":"COMPLETE",...}],
//	  "total_size": 42,
//	  "next_page_token": "..."
//	}
//
// The handler queries the tasks table (for namespace/run scoping and
// pagination) and then fetches the referenced MLMD executions in bulk.

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/filter"
	"github.com/kubeflow/pipelines/backend/src/apiserver/list"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	listExecutionsDefaultPageSize = 20
	listExecutionsMaxPageSize     = 1000
	listExecutionsMlmdTimeout     = 15 * time.Second
)

// executionArtifact represents an artifact linked to an execution via an MLMD event.
type executionArtifact struct {
	ArtifactID       int64           `json:"artifact_id"`
	TypeID           int64           `json:"type_id,omitempty"`
	TypeName         string          `json:"type_name,omitempty"`
	Name             string          `json:"name,omitempty"`
	URI              string          `json:"uri,omitempty"`
	ExternalID       string          `json:"external_id,omitempty"`
	State            string          `json:"state,omitempty"`
	Key              string          `json:"key,omitempty"`
	CreatedAt        *int64          `json:"created_at_ms,omitempty"`
	LastUpdatedAt    *int64          `json:"last_updated_at_ms,omitempty"`
	Properties       json.RawMessage `json:"properties,omitempty"`
	CustomProperties json.RawMessage `json:"custom_properties,omitempty"`
}

// executionItem is the per-execution element of the list response. It embeds
// the proto-style fields directly (as plain JSON types) so we can add run_id
// without modifying the RunExecution proto.
type executionItem struct {
	RunID            string              `json:"run_id,omitempty"`
	ExecutionID      int64               `json:"execution_id,omitempty"`
	TypeID           int64               `json:"type_id,omitempty"`
	TypeName         string              `json:"type_name,omitempty"`
	Name             string              `json:"name,omitempty"`
	ExternalID       string              `json:"external_id,omitempty"`
	State            string              `json:"state,omitempty"`
	TaskID           string              `json:"task_id,omitempty"`
	PodName          string              `json:"pod_name,omitempty"`
	CreatedAt        *int64              `json:"created_at_ms,omitempty"`
	LastUpdatedAt    *int64              `json:"last_updated_at_ms,omitempty"`
	Properties       json.RawMessage     `json:"properties,omitempty"`
	CustomProperties json.RawMessage     `json:"custom_properties,omitempty"`
	InputArtifacts   []executionArtifact `json:"input_artifacts,omitempty"`
	OutputArtifacts  []executionArtifact `json:"output_artifacts,omitempty"`
}

type listExecutionsResponse struct {
	Executions    []executionItem `json:"executions"`
	TotalSize     int             `json:"total_size"`
	NextPageToken string          `json:"next_page_token,omitempty"`
}

// ListExecutionsHttpHandler serves GET /apis/v2beta1/executions.
func (s *RunServer) ListExecutionsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := r.URL.Query()

		namespace := strings.TrimSpace(q.Get("namespace"))
		runID := strings.TrimSpace(q.Get("run_id"))
		stateFilter := strings.TrimSpace(q.Get("state"))
		pageToken := strings.TrimSpace(q.Get("page_token"))

		pageSize := listExecutionsDefaultPageSize
		if ps := strings.TrimSpace(q.Get("page_size")); ps != "" {
			n, err := strconv.Atoi(ps)
			if err != nil || n <= 0 {
				writeListExecutionsError(w, gatewayMux, r,
					status.Errorf(codes.InvalidArgument, "invalid page_size %q", ps))
				return
			}
			if n > listExecutionsMaxPageSize {
				n = listExecutionsMaxPageSize
			}
			pageSize = n
		}

		// Build list options for the task table.
		filterCtx := &model.FilterContext{}
		if namespace != "" {
			filterCtx.ReferenceKey = &model.ReferenceKey{Type: model.NamespaceResourceType, ID: namespace}
		}
		if runID != "" {
			filterCtx.ReferenceKey = &model.ReferenceKey{Type: model.RunResourceType, ID: runID}
		}

		// Base filter: only tasks that have a non-empty, non-zero MLMDExecutionID.
		// This prevents DAG-level tasks (which have no container execution) from
		// consuming the page budget and causing empty pages for small page_size values.
		baseFilter, filterErr := filter.New(&apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{
				{
					Key:       "execution_id",
					Operation: apiv2beta1.Predicate_NOT_EQUALS,
					Value:     &apiv2beta1.Predicate_StringValue{StringValue: ""},
				},
				{
					Key:       "execution_id",
					Operation: apiv2beta1.Predicate_NOT_EQUALS,
					Value:     &apiv2beta1.Predicate_StringValue{StringValue: "0"},
				},
			},
		})
		if filterErr != nil {
			writeListExecutionsError(w, gatewayMux, r,
				status.Errorf(codes.Internal, "failed to build execution filter: %v", filterErr))
			return
		}

		var opts *list.Options
		var err error
		if pageToken != "" {
			opts, err = list.NewOptionsFromToken(pageToken, pageSize)
			if err == nil {
				opts.Filter = baseFilter
			}
		} else {
			opts, err = list.NewOptions(&model.Task{}, pageSize, "create_time", baseFilter)
		}
		if err != nil {
			writeListExecutionsError(w, gatewayMux, r,
				status.Errorf(codes.InvalidArgument, "invalid list options: %v", err))
			return
		}

		// Build the state filter set (upper-cased for case-insensitive matching).
		stateAllowSet := make(map[string]bool)
		if stateFilter != "" {
			for _, sv := range strings.Split(stateFilter, ",") {
				if v := strings.TrimSpace(sv); v != "" {
					stateAllowSet[strings.ToUpper(v)] = true
				}
			}
		}

		// Fetch MLMD reader once — reused across loop iterations.
		reader, readerErr := s.metadataReader()
		if readerErr != nil {
			writeListExecutionsError(w, gatewayMux, r, readerErr)
			return
		}

		mlmdCtx, cancel := context.WithTimeout(ctx, listExecutionsMlmdTimeout) //nolint:govet
		defer cancel()

		// Loop: keep fetching task batches until we have pageSize matching executions
		// or there are no more pages. This ensures state filtering does not cause
		// under-filled pages regardless of how sparse matching executions are.
		const maxFetchIterations = 10
		var items []executionItem
		var totalSize int
		var nextPageTokenForResp string
		currentOpts := opts

		for iteration := 0; len(items) < pageSize && iteration < maxFetchIterations; iteration++ {
			batchTasks, batchTotal, batchNextToken, listErr := s.resourceManager.ListTasks(filterCtx, currentOpts)
			if listErr != nil {
				writeListExecutionsError(w, gatewayMux, r, listErr)
				return
			}
			if iteration == 0 {
				totalSize = batchTotal
			}
			nextPageTokenForResp = batchNextToken

			// Collect unique positive MLMDExecutionIDs from this batch.
			executionIDSet := make(map[int64]bool, len(batchTasks))
			taskByExecutionID := make(map[int64]*model.Task, len(batchTasks))
			for _, t := range batchTasks {
				id, parseErr := strconv.ParseInt(t.MLMDExecutionID, 10, 64)
				if parseErr != nil || id <= 0 {
					continue
				}
				if !executionIDSet[id] {
					executionIDSet[id] = true
					taskByExecutionID[id] = t
				}
			}

			if len(executionIDSet) > 0 {
				ids := make([]int64, 0, len(executionIDSet))
				for id := range executionIDSet {
					ids = append(ids, id)
				}
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

				mlmdExecutions, mlmdErr := reader.GetExecutions(mlmdCtx, ids)
				if mlmdErr != nil {
					writeListExecutionsError(w, gatewayMux, r, mlmdErr)
					return
				}

				// Resolve type names.
				typeIDSet := make(map[int64]bool)
				for _, ex := range mlmdExecutions {
					if ex.GetTypeId() > 0 && ex.GetType() == "" {
						typeIDSet[ex.GetTypeId()] = true
					}
				}
				typeNames := make(map[int64]string)
				if len(typeIDSet) > 0 {
					if executionTypes, typeErr := reader.GetExecutionTypes(mlmdCtx, sortedInt64Keys(typeIDSet)); typeErr == nil {
						for _, et := range executionTypes {
							typeNames[et.GetId()] = et.GetName()
						}
					}
				}

				for _, ex := range mlmdExecutions {
					if len(items) >= pageSize {
						break
					}
					if ex == nil || ex.GetId() <= 0 {
						continue
					}
					state := ex.GetLastKnownState().String()
					if len(stateAllowSet) > 0 && !stateAllowSet[strings.ToUpper(state)] {
						continue
					}
					task := taskByExecutionID[ex.GetId()]
					runIDForItem, taskID := "", ""
					podName := executionPodName(ex)
					if task != nil {
						runIDForItem = task.RunID
						taskID = task.UUID
						if task.PodName != "" {
							podName = task.PodName
						}
					}
					typeName := ex.GetType()
					if typeName == "" {
						typeName = typeNames[ex.GetTypeId()]
					}
					item := executionItem{
						RunID:       runIDForItem,
						ExecutionID: ex.GetId(),
						TypeID:      ex.GetTypeId(),
						TypeName:    typeName,
						Name:        ex.GetName(),
						ExternalID:  ex.GetExternalId(),
						State:       state,
						TaskID:      taskID,
						PodName:     podName,
					}
					if ts := ex.GetCreateTimeSinceEpoch(); ts > 0 {
						item.CreatedAt = &ts
					}
					if ts := ex.GetLastUpdateTimeSinceEpoch(); ts > 0 {
						item.LastUpdatedAt = &ts
					}
					if props := ex.GetProperties(); len(props) > 0 {
						if st := mlmdValuesToStruct(props); st != nil {
							if b, marshalErr := protojson.Marshal(st); marshalErr == nil {
								item.Properties = b
							}
						}
					}
					if custom := ex.GetCustomProperties(); len(custom) > 0 {
						if st := mlmdValuesToStruct(custom); st != nil {
							if b, marshalErr := protojson.Marshal(st); marshalErr == nil {
								item.CustomProperties = b
							}
						}
					}
					items = append(items, item)
				}
			}

			if batchNextToken == "" {
				break // no more pages in the task table
			}

			// Prepare options for the next iteration.
			nextOpts, nextErr := list.NewOptionsFromToken(batchNextToken, pageSize)
			if nextErr != nil {
				break
			}
			nextOpts.Filter = baseFilter
			currentOpts = nextOpts
		}

		if items == nil {
			items = []executionItem{}
		}
		resp := listExecutionsResponse{
			Executions:    items,
			TotalSize:     totalSize,
			NextPageToken: nextPageTokenForResp,
		}
		writeListExecutionsJSON(w, resp)
	}
}

// GetExecutionHttpHandler serves GET /apis/v2beta1/executions/{execution_id}.
func (s *RunServer) GetExecutionHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		idStr := mux.Vars(r)["execution_id"]
		executionID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || executionID <= 0 {
			writeListExecutionsError(w, gatewayMux, r,
				status.Errorf(codes.InvalidArgument, "invalid execution_id %q", idStr))
			return
		}

		reader, err := s.metadataReader()
		if err != nil {
			writeListExecutionsError(w, gatewayMux, r, err)
			return
		}

		mlmdCtx, cancel := context.WithTimeout(ctx, listExecutionsMlmdTimeout)
		defer cancel()

		mlmdExecutions, err := reader.GetExecutions(mlmdCtx, []int64{executionID})
		if err != nil {
			writeListExecutionsError(w, gatewayMux, r, err)
			return
		}
		if len(mlmdExecutions) == 0 || mlmdExecutions[0] == nil {
			writeListExecutionsError(w, gatewayMux, r,
				status.Errorf(codes.NotFound, "execution %d not found", executionID))
			return
		}
		ex := mlmdExecutions[0]

		// Resolve type name.
		typeName := ex.GetType()
		if typeName == "" && ex.GetTypeId() > 0 {
			if executionTypes, typeErr := reader.GetExecutionTypes(mlmdCtx, []int64{ex.GetTypeId()}); typeErr == nil && len(executionTypes) > 0 {
				typeName = executionTypes[0].GetName()
			}
		}

		// Look up the task row for run_id / task_id enrichment.
		runIDForItem, taskID, podName := "", "", executionPodName(ex)
		taskFilter, filterErr := filter.New(&apiv2beta1.Filter{
			Predicates: []*apiv2beta1.Predicate{{
				Key:       "execution_id",
				Operation: apiv2beta1.Predicate_EQUALS,
				Value:     &apiv2beta1.Predicate_StringValue{StringValue: idStr},
			}},
		})
		if filterErr == nil {
			if taskOpts, optErr := list.NewOptions(&model.Task{}, 1, "create_time", taskFilter); optErr == nil {
				if tasks, _, _, listErr := s.resourceManager.ListTasks(nil, taskOpts); listErr == nil && len(tasks) > 0 {
					t := tasks[0]
					runIDForItem = t.RunID
					taskID = t.UUID
					if t.PodName != "" {
						podName = t.PodName
					}
				}
			}
		}

		item := executionItem{
			RunID:       runIDForItem,
			ExecutionID: ex.GetId(),
			TypeID:      ex.GetTypeId(),
			TypeName:    typeName,
			Name:        ex.GetName(),
			ExternalID:  ex.GetExternalId(),
			State:       ex.GetLastKnownState().String(),
			TaskID:      taskID,
			PodName:     podName,
		}
		if ts := ex.GetCreateTimeSinceEpoch(); ts > 0 {
			item.CreatedAt = &ts
		}
		if ts := ex.GetLastUpdateTimeSinceEpoch(); ts > 0 {
			item.LastUpdatedAt = &ts
		}
		if props := ex.GetProperties(); len(props) > 0 {
			if st := mlmdValuesToStruct(props); st != nil {
				if b, marshalErr := protojson.Marshal(st); marshalErr == nil {
					item.Properties = b
				}
			}
		}
		if custom := ex.GetCustomProperties(); len(custom) > 0 {
			if st := mlmdValuesToStruct(custom); st != nil {
				if b, marshalErr := protojson.Marshal(st); marshalErr == nil {
					item.CustomProperties = b
				}
			}
		}

		// Enrich with input/output artifacts from MLMD events (non-fatal).
		item.InputArtifacts, item.OutputArtifacts = fetchExecutionArtifacts(mlmdCtx, reader, executionID)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(item)
	}
}

func writeListExecutionsJSON(w http.ResponseWriter, resp listExecutionsResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func writeListExecutionsError(w http.ResponseWriter, gatewayMux *runtime.ServeMux, r *http.Request, err error) {
	writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
}

// fetchExecutionArtifacts returns the input and output artifacts linked to an
// MLMD execution via events. Failures are non-fatal — both slices are nil.
func fetchExecutionArtifacts(ctx context.Context, reader RunMetadataReader, executionID int64) (inputs, outputs []executionArtifact) {
	events, err := reader.GetEventsByExecutionIDs(ctx, []int64{executionID})
	if err != nil || len(events) == 0 {
		return nil, nil
	}

	// Collect artifact IDs along with the first key seen and the direction(s).
	type artifactInfo struct {
		key      string
		isInput  bool
		isOutput bool
	}
	infoByID := make(map[int64]*artifactInfo)
	for _, event := range events {
		id := event.GetArtifactId()
		if id <= 0 {
			continue
		}
		info := infoByID[id]
		if info == nil {
			info = &artifactInfo{key: mlmdEventPath(event.GetPath())}
			infoByID[id] = info
		}
		switch mlmdEventDirection(event.GetType()) {
		case "INPUT":
			info.isInput = true
		case "OUTPUT":
			info.isOutput = true
		}
	}
	if len(infoByID) == 0 {
		return nil, nil
	}

	artifactIDs := sortedInt64Keys(func() map[int64]bool {
		s := make(map[int64]bool, len(infoByID))
		for id := range infoByID {
			s[id] = true
		}
		return s
	}())

	artifacts, err := reader.GetArtifacts(ctx, artifactIDs)
	if err != nil || len(artifacts) == 0 {
		return nil, nil
	}

	// Resolve type names.
	typeIDSet := make(map[int64]bool)
	for _, a := range artifacts {
		if a.GetTypeId() > 0 && a.GetType() == "" {
			typeIDSet[a.GetTypeId()] = true
		}
	}
	typeNames := make(map[int64]string)
	if len(typeIDSet) > 0 {
		if artifactTypes, typeErr := reader.GetArtifactTypes(ctx, sortedInt64Keys(typeIDSet)); typeErr == nil {
			for _, at := range artifactTypes {
				typeNames[at.GetId()] = at.GetName()
			}
		}
	}

	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].GetId() < artifacts[j].GetId() })
	for _, a := range artifacts {
		if a == nil || a.GetId() <= 0 {
			continue
		}
		info := infoByID[a.GetId()]
		if info == nil {
			continue
		}
		typeName := a.GetType()
		if typeName == "" {
			typeName = typeNames[a.GetTypeId()]
		}
		item := executionArtifact{
			ArtifactID: a.GetId(),
			TypeID:     a.GetTypeId(),
			TypeName:   typeName,
			Name:       a.GetName(),
			URI:        a.GetUri(),
			ExternalID: a.GetExternalId(),
			State:      a.GetState().String(),
			Key:        info.key,
		}
		if ts := a.GetCreateTimeSinceEpoch(); ts > 0 {
			item.CreatedAt = &ts
		}
		if ts := a.GetLastUpdateTimeSinceEpoch(); ts > 0 {
			item.LastUpdatedAt = &ts
		}
		if props := a.GetProperties(); len(props) > 0 {
			if s := mlmdValuesToStruct(props); s != nil {
				if b, marshalErr := protojson.Marshal(s); marshalErr == nil {
					item.Properties = b
				}
			}
		}
		if custom := a.GetCustomProperties(); len(custom) > 0 {
			if s := mlmdValuesToStruct(custom); s != nil {
				if b, marshalErr := protojson.Marshal(s); marshalErr == nil {
					item.CustomProperties = b
				}
			}
		}
		if info.isInput {
			inputs = append(inputs, item)
		}
		if info.isOutput {
			outputs = append(outputs, item)
		}
	}
	return inputs, outputs
}

