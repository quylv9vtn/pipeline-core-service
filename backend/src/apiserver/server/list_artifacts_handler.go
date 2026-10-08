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

// ListArtifactsHttpHandler serves GET /apis/v2beta1/artifacts.
//
// Query parameters:
//
//	?run_id=<id>        filter to a specific run (optional)
//	                    When set, usages (step that produced/consumed each
//	                    artifact) are populated in the response.
//	?page_size=<N>      max artifacts to return (default 20, max 100)
//	?page_token=<tok>   MLMD continuation token from a previous response
//	                    (only meaningful when run_id is not set)
//
// Response:
//
//	{
//	  "artifacts": [
//	    {
//	      "artifact_id": 42,
//	      "name": "model",
//	      "type_name": "system.Model",
//	      "uri": "gs://bucket/path",
//	      "state": "LIVE",
//	      "usages": [                      ← only present when run_id is given
//	        {
//	          "task_id":    "train-step",
//	          "key":        "model",
//	          "event_type": "OUTPUT"
//	        }
//	      ]
//	    }
//	  ],
//	  "next_page_token": "..."             ← only present when run_id is not set
//	}

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	listArtifactsDefaultPageSize = 20
	listArtifactsMaxPageSize     = 100
	listArtifactsMlmdTimeout     = 15 * time.Second
)

// listArtifactsResponse is the JSON envelope returned by GET /apis/v2beta1/artifacts.
type listArtifactsResponse struct {
	Artifacts     []artifactItem `json:"artifacts"`
	NextPageToken string         `json:"next_page_token,omitempty"`
}

// artifactItem is the per-artifact element of the list response.
type artifactItem struct {
	ArtifactID       int64                   `json:"artifact_id"`
	Name             string                  `json:"name,omitempty"`
	TypeName         string                  `json:"type_name,omitempty"`
	URI              string                  `json:"uri,omitempty"`
	State            string                  `json:"state,omitempty"`
	CreatedAtMs      *int64                  `json:"created_at_ms,omitempty"`
	LastUpdatedAtMs  *int64                  `json:"last_updated_at_ms,omitempty"`
	Properties       json.RawMessage         `json:"properties,omitempty"`
	CustomProperties json.RawMessage         `json:"custom_properties,omitempty"`
	// Usages is populated only when the request includes a run_id parameter.
	Usages []artifactUsageResponse `json:"usages,omitempty"`
}

// ListArtifactsHttpHandler serves GET /apis/v2beta1/artifacts.
func (s *RunServer) ListArtifactsHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := r.URL.Query()

		runID := strings.TrimSpace(q.Get("run_id"))
		pageToken := strings.TrimSpace(q.Get("page_token"))

		pageSize := int32(listArtifactsDefaultPageSize)
		if ps := strings.TrimSpace(q.Get("page_size")); ps != "" {
			n, err := strconv.Atoi(ps)
			if err != nil || n <= 0 {
				writeListArtifactsError(w, gatewayMux, r,
					status.Errorf(codes.InvalidArgument, "invalid page_size %q", ps))
				return
			}
			if n > listArtifactsMaxPageSize {
				n = listArtifactsMaxPageSize
			}
			pageSize = int32(n)
		}

		if runID != "" {
			// Run-scoped listing: fetch via MLMD context so we also get usages.
			s.listArtifactsForRun(ctx, w, gatewayMux, r, runID)
			return
		}

		// Global listing via MLMD native pagination.
		s.listArtifactsGlobal(ctx, w, gatewayMux, r, pageSize, pageToken)
	}
}

// listArtifactsForRun handles the ?run_id= case. It reuses loadRunMetadata
// which fetches artifacts and their usages from MLMD for a single run.
func (s *RunServer) listArtifactsForRun(ctx context.Context, w http.ResponseWriter, gatewayMux *runtime.ServeMux, r *http.Request, runID string) {
	run, err := s.getRun(ctx, runID)
	if err != nil {
		writeListArtifactsError(w, gatewayMux, r, err)
		return
	}

	mlmdCtx, cancel := context.WithTimeout(ctx, listArtifactsMlmdTimeout)
	defer cancel()

	snapshot, err := s.loadRunMetadata(mlmdCtx, run.UUID, run.RunDetails.TaskDetails)
	if err != nil {
		writeListArtifactsError(w, gatewayMux, r, err)
		return
	}

	sorted := sortedRunArtifacts(snapshot.artifacts)
	items := make([]artifactItem, 0, len(sorted))
	for _, a := range sorted {
		item := artifactItem{
			ArtifactID: a.GetArtifactId(),
			Name:       a.GetName(),
			TypeName:   a.GetTypeName(),
			URI:        a.GetUri(),
			State:      a.GetState(),
		}
		for _, u := range a.GetUsages() {
			item.Usages = append(item.Usages, artifactUsageResponse{
				TaskID:    u.GetTaskId(),
				Key:       u.GetKey(),
				EventType: u.GetEventType(),
			})
		}
		items = append(items, item)
	}

	writeListArtifactsJSON(w, listArtifactsResponse{Artifacts: items})
}

// listArtifactsGlobal handles the no-run_id case using MLMD's native pagination.
func (s *RunServer) listArtifactsGlobal(ctx context.Context, w http.ResponseWriter, gatewayMux *runtime.ServeMux, r *http.Request, pageSize int32, pageToken string) {
	reader, err := s.metadataReader()
	if err != nil {
		writeListArtifactsError(w, gatewayMux, r, err)
		return
	}

	mlmdCtx, cancel := context.WithTimeout(ctx, listArtifactsMlmdTimeout)
	defer cancel()

	artifacts, nextPageToken, err := reader.ListAllArtifacts(mlmdCtx, pageSize, pageToken)
	if err != nil {
		writeListArtifactsError(w, gatewayMux, r, err)
		return
	}

	// Resolve type names for artifacts that only carry type_id.
	typeIDSet := make(map[int64]bool)
	for _, a := range artifacts {
		if a.GetTypeId() > 0 && a.GetType() == "" {
			typeIDSet[a.GetTypeId()] = true
		}
	}
	typeNames := make(map[int64]string)
	if len(typeIDSet) > 0 {
		typeIDs := sortedInt64Keys(typeIDSet)
		artifactTypes, typeErr := reader.GetArtifactTypes(mlmdCtx, typeIDs)
		if typeErr != nil {
			glog.Warningf("ListArtifactsHttpHandler: failed to fetch artifact types: %v", typeErr)
		} else {
			for _, at := range artifactTypes {
				typeNames[at.GetId()] = at.GetName()
			}
		}
	}

	items := make([]artifactItem, 0, len(artifacts))
	for _, a := range artifacts {
		if a == nil || a.GetId() <= 0 {
			continue
		}
		typeName := a.GetType()
		if typeName == "" {
			typeName = typeNames[a.GetTypeId()]
		}
		item := artifactItem{
			ArtifactID: a.GetId(),
			Name:       a.GetName(),
			TypeName:   typeName,
			URI:        a.GetUri(),
			State:      a.GetState().String(),
		}
		if ts := a.GetCreateTimeSinceEpoch(); ts > 0 {
			item.CreatedAtMs = &ts
		}
		if ts := a.GetLastUpdateTimeSinceEpoch(); ts > 0 {
			item.LastUpdatedAtMs = &ts
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
		items = append(items, item)
	}

	writeListArtifactsJSON(w, listArtifactsResponse{
		Artifacts:     items,
		NextPageToken: nextPageToken,
	})
}

// GetArtifactHttpHandler serves GET /apis/v2beta1/artifacts/{artifact_id}.
func (s *RunServer) GetArtifactHttpHandler(gatewayMux *runtime.ServeMux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		idStr := mux.Vars(r)["artifact_id"]
		artifactID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil || artifactID <= 0 {
			writeListArtifactsError(w, gatewayMux, r,
				status.Errorf(codes.InvalidArgument, "invalid artifact_id %q", idStr))
			return
		}

		reader, err := s.metadataReader()
		if err != nil {
			writeListArtifactsError(w, gatewayMux, r, err)
			return
		}

		mlmdCtx, cancel := context.WithTimeout(ctx, listArtifactsMlmdTimeout)
		defer cancel()

		artifacts, err := reader.GetArtifacts(mlmdCtx, []int64{artifactID})
		if err != nil {
			writeListArtifactsError(w, gatewayMux, r, err)
			return
		}
		if len(artifacts) == 0 || artifacts[0] == nil {
			writeListArtifactsError(w, gatewayMux, r,
				status.Errorf(codes.NotFound, "artifact %d not found", artifactID))
			return
		}
		a := artifacts[0]

		// Resolve type name if only type_id is present.
		typeName := a.GetType()
		if typeName == "" && a.GetTypeId() > 0 {
			if artifactTypes, typeErr := reader.GetArtifactTypes(mlmdCtx, []int64{a.GetTypeId()}); typeErr == nil && len(artifactTypes) > 0 {
				typeName = artifactTypes[0].GetName()
			} else if typeErr != nil {
				glog.Warningf("GetArtifactHttpHandler: failed to fetch type for artifact %d: %v", artifactID, typeErr)
			}
		}

		item := artifactItem{
			ArtifactID: a.GetId(),
			Name:       a.GetName(),
			TypeName:   typeName,
			URI:        a.GetUri(),
			State:      a.GetState().String(),
		}
		if ts := a.GetCreateTimeSinceEpoch(); ts > 0 {
			item.CreatedAtMs = &ts
		}
		if ts := a.GetLastUpdateTimeSinceEpoch(); ts > 0 {
			item.LastUpdatedAtMs = &ts
		}
		if props := a.GetProperties(); len(props) > 0 {
			if st := mlmdValuesToStruct(props); st != nil {
				if b, marshalErr := protojson.Marshal(st); marshalErr == nil {
					item.Properties = b
				}
			}
		}
		if custom := a.GetCustomProperties(); len(custom) > 0 {
			if st := mlmdValuesToStruct(custom); st != nil {
				if b, marshalErr := protojson.Marshal(st); marshalErr == nil {
					item.CustomProperties = b
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(item)
	}
}

func writeListArtifactsJSON(w http.ResponseWriter, resp listArtifactsResponse) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func writeListArtifactsError(w http.ResponseWriter, gatewayMux *runtime.ServeMux, r *http.Request, err error) {
	writeRunMetadataHttpError(r.Context(), gatewayMux, w, r, err)
}
