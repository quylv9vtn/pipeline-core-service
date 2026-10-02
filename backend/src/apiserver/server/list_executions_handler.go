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

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
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

// executionItem is the per-execution element of the list response. It embeds
// the proto-style fields directly (as plain JSON types) so we can add run_id
// without modifying the RunExecution proto.
type executionItem struct {
	RunID            string          `json:"run_id,omitempty"`
	ExecutionID      int64           `json:"execution_id,omitempty"`
	TypeID           int64           `json:"type_id,omitempty"`
	TypeName         string          `json:"type_name,omitempty"`
	Name             string          `json:"name,omitempty"`
	ExternalID       string          `json:"external_id,omitempty"`
	State            string          `json:"state,omitempty"`
	TaskID           string          `json:"task_id,omitempty"`
	PodName          string          `json:"pod_name,omitempty"`
	CreatedAt        *int64          `json:"created_at_ms,omitempty"`
	LastUpdatedAt    *int64          `json:"last_updated_at_ms,omitempty"`
	Properties       json.RawMessage `json:"properties,omitempty"`
	CustomProperties json.RawMessage `json:"custom_properties,omitempty"`
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

		var opts *list.Options
		var err error
		if pageToken != "" {
			opts, err = list.NewOptionsFromToken(pageToken, pageSize)
		} else {
			opts, err = list.NewOptions(&model.Task{}, pageSize, "CreatedTimestamp", nil)
		}
		if err != nil {
			writeListExecutionsError(w, gatewayMux, r,
				status.Errorf(codes.InvalidArgument, "invalid list options: %v", err))
			return
		}

		tasks, totalSize, nextPageToken, err := s.resourceManager.ListTasks(filterCtx, opts)
		if err != nil {
			writeListExecutionsError(w, gatewayMux, r, err)
			return
		}

		// Collect unique positive MLMDExecutionIDs from the returned tasks.
		executionIDSet := make(map[int64]bool, len(tasks))
		taskByExecutionID := make(map[int64]*model.Task, len(tasks))
		for _, t := range tasks {
			id, parseErr := strconv.ParseInt(t.MLMDExecutionID, 10, 64)
			if parseErr != nil || id <= 0 {
				continue
			}
			if !executionIDSet[id] {
				executionIDSet[id] = true
				taskByExecutionID[id] = t
			}
		}

		// Build the state filter set (upper-cased for case-insensitive matching).
		stateAllowSet := make(map[string]bool)
		if stateFilter != "" {
			for _, s := range strings.Split(stateFilter, ",") {
				if v := strings.TrimSpace(s); v != "" {
					stateAllowSet[strings.ToUpper(v)] = true
				}
			}
		}

		// Short-circuit if there are no execution IDs to fetch.
		if len(executionIDSet) == 0 {
			resp := listExecutionsResponse{Executions: []executionItem{}, TotalSize: totalSize, NextPageToken: nextPageToken}
			writeListExecutionsJSON(w, resp)
			return
		}

		// Fetch MLMD executions.
		reader, readerErr := s.metadataReader()
		if readerErr != nil {
			writeListExecutionsError(w, gatewayMux, r, readerErr)
			return
		}

		ids := make([]int64, 0, len(executionIDSet))
		for id := range executionIDSet {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		mlmdCtx, cancel := context.WithTimeout(ctx, listExecutionsMlmdTimeout) //nolint:govet
		defer cancel()

		mlmdExecutions, mlmdErr := reader.GetExecutions(mlmdCtx, ids)
		if mlmdErr != nil {
			writeListExecutionsError(w, gatewayMux, r, mlmdErr)
			return
		}

		// Resolve type names for executions that only carry a type ID.
		typeIDSet := make(map[int64]bool)
		for _, ex := range mlmdExecutions {
			if ex.GetTypeId() > 0 && ex.GetType() == "" {
				typeIDSet[ex.GetTypeId()] = true
			}
		}
		typeNames := make(map[int64]string)
		if len(typeIDSet) > 0 {
			typeIDs := sortedInt64Keys(typeIDSet)
			executionTypes, typeErr := reader.GetExecutionTypes(mlmdCtx, typeIDs)
			if typeErr == nil {
				for _, et := range executionTypes {
					typeNames[et.GetId()] = et.GetName()
				}
			}
		}

		// Build response items.
		items := make([]executionItem, 0, len(mlmdExecutions))
		for _, ex := range mlmdExecutions {
			if ex == nil || ex.GetId() <= 0 {
				continue
			}
			state := ex.GetLastKnownState().String()
			if len(stateAllowSet) > 0 && !stateAllowSet[strings.ToUpper(state)] {
				continue
			}
			task := taskByExecutionID[ex.GetId()]
			runIDForItem := ""
			taskID := ""
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
				if s := mlmdValuesToStruct(props); s != nil {
					if b, marshalErr := protojson.Marshal(s); marshalErr == nil {
						item.Properties = b
					}
				}
			}
			if custom := ex.GetCustomProperties(); len(custom) > 0 {
				if s := mlmdValuesToStruct(custom); s != nil {
					if b, marshalErr := protojson.Marshal(s); marshalErr == nil {
						item.CustomProperties = b
					}
				}
			}
			items = append(items, item)
		}

		resp := listExecutionsResponse{
			Executions:    items,
			TotalSize:     totalSize,
			NextPageToken: nextPageToken,
		}
		writeListExecutionsJSON(w, resp)
	}
}

func writeListExecutionsJSON(w http.ResponseWriter, resp listExecutionsResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func writeListExecutionsError(w http.ResponseWriter, gatewayMux *runtime.ServeMux, r *http.Request, err error) {
	writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
}
