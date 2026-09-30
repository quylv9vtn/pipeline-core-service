// Copyright 2020 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	api "github.com/kubeflow/pipelines/backend/api/v1beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/resource"
)

const (
	RunKey  = "run_id"
	NodeKey = "node_id"
	Follow  = "follow"
)

type RunLogServer struct {
	resourceManager *resource.ResourceManager
	httpClient      *http.Client
}

// Log streaming endpoint
// This endpoint is not exposed through grpc endpoint, since grpc-gateway cannot handle native HTTP content streaming.
func (s *RunLogServer) ReadRunLogV1(w http.ResponseWriter, r *http.Request) {
	glog.Infof("Read run log called")

	vars := mux.Vars(r)

	runId, ok := vars[RunKey]
	if !ok {
		s.writeErrorToResponse(w, http.StatusBadRequest, fmt.Errorf("missing path parameter: '%s')", RunKey))
		return
	}

	nodeId, ok := vars[NodeKey]
	if !ok {
		s.writeErrorToResponse(w, http.StatusBadRequest, fmt.Errorf("missing path parameter: '%s')", NodeKey))
		return
	}

	follow := vars[Follow] == "true" // defaults to false

	if !follow {
		// Buffer first so failures can still return a proper HTTP status.
		var buf bytes.Buffer
		if err := s.resourceManager.ReadLog(context.Background(), runId, nodeId, false, &buf); err != nil {
			s.writeErrorToResponse(w, http.StatusInternalServerError, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Cache-Control", "no-cache, private")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, &buf)
		return
	}

	// Streaming mode: headers go out before the body, so a mid-stream failure
	// can no longer change the status code.
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Cache-Control", "no-cache, private")
	w.WriteHeader(http.StatusOK)
	if err := s.resourceManager.ReadLog(context.Background(), runId, nodeId, true, w); err != nil {
		glog.Errorf("Failed to stream run log for run %v node %v: %+v", runId, nodeId, err)
	}
}

func (s *RunLogServer) writeErrorToResponse(w http.ResponseWriter, code int, err error) {
	glog.Errorf("Failed to read run log. Error: %+v", err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	errorResponse := &api.Error{ErrorMessage: err.Error(), ErrorDetails: fmt.Sprintf("%+v", err)}
	errBytes, jsonErr := json.Marshal(errorResponse)
	if jsonErr != nil {
		_, _ = w.Write([]byte("Error reading run log"))
		return
	}
	_, _ = w.Write(errBytes)
}

func NewRunLogServer(resourceManager *resource.ResourceManager) *RunLogServer {
	return &RunLogServer{resourceManager: resourceManager, httpClient: http.DefaultClient}
}
