// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestDownloadPipelineVersionSucceeds(t *testing.T) {
	clients, resourceManager, _, pipelineVersion := initWithExperimentAndPipelineVersion(t)
	defer clients.Close()

	request := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/apis/v2beta1/pipelines/%s/versions/%s:download", pipelineVersion.PipelineId, pipelineVersion.UUID),
		nil,
	)
	recorder := httptest.NewRecorder()
	router := mux.NewRouter()
	router.HandleFunc(
		"/apis/v2beta1/pipelines/{pipeline_id}/versions/{pipeline_version_id}:download",
		NewPipelineDownloadServer(resourceManager).DownloadPipelineVersion,
	).Methods(http.MethodGet)

	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, string(pipelineVersion.PipelineSpec), recorder.Body.String())
	require.Equal(t, "application/yaml; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Equal(t, "no-cache, private", recorder.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))
	require.Contains(t, recorder.Header().Get("Content-Disposition"), "attachment")
	require.Contains(t, recorder.Header().Get("Content-Disposition"), pipelineVersion.UUID+".yaml")
}

func TestDownloadPipelineVersionRejectsWrongParentPipeline(t *testing.T) {
	clients, resourceManager, _, pipelineVersion := initWithExperimentAndPipelineVersion(t)
	defer clients.Close()

	request := httptest.NewRequest(http.MethodGet, "/apis/v2beta1/pipelines/wrong/versions/version:download", nil)
	request = mux.SetURLVars(request, map[string]string{
		pipelineIDPathKey:        "wrong-pipeline-id",
		pipelineVersionIDPathKey: pipelineVersion.UUID,
	})
	recorder := httptest.NewRecorder()

	NewPipelineDownloadServer(resourceManager).DownloadPipelineVersion(recorder, request)

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.JSONEq(t, `{"code":5,"message":"Pipeline version does not belong to the specified pipeline"}`, recorder.Body.String())
}

func TestDownloadPipelineVersionReturnsNotFound(t *testing.T) {
	clients, resourceManager, _, _ := initWithExperimentAndPipelineVersion(t)
	defer clients.Close()

	request := httptest.NewRequest(http.MethodGet, "/apis/v2beta1/pipelines/pipeline/versions/missing:download", nil)
	request = mux.SetURLVars(request, map[string]string{
		pipelineIDPathKey:        "pipeline",
		pipelineVersionIDPathKey: "missing-version",
	})
	recorder := httptest.NewRecorder()

	NewPipelineDownloadServer(resourceManager).DownloadPipelineVersion(recorder, request)

	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.JSONEq(t, `{"code":5,"message":"Pipeline version not found"}`, recorder.Body.String())
}

func TestDownloadPipelineVersionRequiresBothIDs(t *testing.T) {
	clients, resourceManager, _, _ := initWithExperimentAndPipelineVersion(t)
	defer clients.Close()

	request := httptest.NewRequest(http.MethodGet, "/apis/v2beta1/pipelines//versions/:download", nil)
	recorder := httptest.NewRecorder()

	NewPipelineDownloadServer(resourceManager).DownloadPipelineVersion(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.JSONEq(t, `{"code":3,"message":"pipeline_id and pipeline_version_id are required"}`, recorder.Body.String())
}

func TestSanitizeDownloadFileName(t *testing.T) {
	require.Equal(t, "version_1__bad", sanitizeDownloadFileName("version/1\r\nbad"))
}
