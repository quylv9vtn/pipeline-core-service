// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package swagger

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type swaggerDocument struct {
	Paths       map[string]map[string]swaggerOperation `json:"paths"`
	Definitions map[string]swaggerDefinition           `json:"definitions"`
}

type swaggerDefinition struct {
	Required []string `json:"required"`
}

type swaggerOperation struct {
	Parameters []swaggerParameter     `json:"parameters"`
	Responses  map[string]interface{} `json:"responses"`
}

type swaggerParameter struct {
	Name     string `json:"name"`
	In       string `json:"in"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
	Format   string `json:"format"`
}

func TestUploadPipelineVersionRequiresPipelineID(t *testing.T) {
	for _, fileName := range []string{
		"pipeline.upload.swagger.json",
		"kfp_api_single_file.swagger.json",
	} {
		t.Run(fileName, func(t *testing.T) {
			document := readSwaggerDocument(t, fileName)
			operation := document.Paths["/apis/v2beta1/pipelines/upload_version"]["post"]
			requireRequiredParameter(t, operation, "query", "pipelineid")
			requireRequiredParameter(t, operation, "formData", "uploadfile")
		})
	}
}

func TestUploadEndpointsAcceptOptionalClientProvidedIDs(t *testing.T) {
	for _, fileName := range []string{
		"pipeline.upload.swagger.json",
		"kfp_api_single_file.swagger.json",
	} {
		t.Run(fileName, func(t *testing.T) {
			document := readSwaggerDocument(t, fileName)

			upload := document.Paths["/apis/v2beta1/pipelines/upload"]["post"]
			requireOptionalUUIDParameter(t, upload, "pipeline_id")
			requireOptionalUUIDParameter(t, upload, "pipeline_version_id")

			uploadVersion := document.Paths["/apis/v2beta1/pipelines/upload_version"]["post"]
			requireOptionalUUIDParameter(t, uploadVersion, "pipeline_version_id")
		})
	}
}

func TestDownloadPipelineVersionIsDocumented(t *testing.T) {
	for _, fileName := range []string{
		"pipeline.upload.swagger.json",
		"kfp_api_single_file.swagger.json",
	} {
		t.Run(fileName, func(t *testing.T) {
			document := readSwaggerDocument(t, fileName)
			operation, found := document.Paths["/apis/v2beta1/pipelines/{pipeline_id}/versions/{pipeline_version_id}:download"]["get"]
			require.True(t, found, "pipeline version download operation must be documented")
			requireRequiredParameter(t, operation, "path", "pipeline_id")
			requireRequiredParameter(t, operation, "path", "pipeline_version_id")
		})
	}
}

func TestAuthorizeRequiresAllQueryParameters(t *testing.T) {
	for _, fileName := range []string{
		"auth.swagger.json",
		"kfp_api_single_file.swagger.json",
	} {
		t.Run(fileName, func(t *testing.T) {
			document := readSwaggerDocument(t, fileName)
			operation := document.Paths["/apis/v2beta1/auth"]["get"]
			for _, name := range []string{"namespace", "resources", "verb"} {
				requireRequiredParameter(t, operation, "query", name)
			}
		})
	}
}

func TestProvisionProjectRequiresBodyAndIdentifiers(t *testing.T) {
	for _, fileName := range []string{
		"project.swagger.json",
		"kfp_api_single_file.swagger.json",
	} {
		t.Run(fileName, func(t *testing.T) {
			document := readSwaggerDocument(t, fileName)
			operation, found := document.Paths["/apis/v2beta1/projects"]["post"]
			require.True(t, found, "project provisioning operation must be documented")
			requireRequiredParameter(t, operation, "body", "body")
			require.ElementsMatch(t,
				[]string{"project_id", "name", "namespace"},
				document.Definitions["v2beta1ProjectProvisionRequest"].Required,
			)
		})
	}
}

func TestProjectReadOperationsAreDocumented(t *testing.T) {
	for _, fileName := range []string{
		"project.swagger.json",
		"kfp_api_single_file.swagger.json",
	} {
		t.Run(fileName, func(t *testing.T) {
			document := readSwaggerDocument(t, fileName)
			list, found := document.Paths["/apis/v2beta1/projects"]["get"]
			require.True(t, found, "project list operation must be documented")
			require.Contains(t, list.Responses, "200")

			get, found := document.Paths["/apis/v2beta1/projects/{project_id}"]["get"]
			require.True(t, found, "project get operation must be documented")
			requireRequiredParameter(t, get, "path", "project_id")
		})
	}
}

func TestQuotaLimitReadOperationsAreDocumented(t *testing.T) {
	document := readSwaggerDocument(t, "kfp_api_single_file.swagger.json")
	for _, path := range []string{"/quota/limits", "/apis/v2beta1/quota/limits"} {
		operation, found := document.Paths[path]["get"]
		require.True(t, found, "quota limit GET operation must be documented at %s", path)
		requireRequiredParameter(t, operation, "query", "project_id")
		require.Contains(t, operation.Responses, "200")
	}
	_, found := document.Definitions["quotaLimitsResponse"]
	require.True(t, found, "quota limits response schema must be documented")
}

func TestRunMetadataEndpointsAreDocumented(t *testing.T) {
	for _, fileName := range []string{
		"run.swagger.json",
		"kfp_api_single_file.swagger.json",
	} {
		t.Run(fileName, func(t *testing.T) {
			document := readSwaggerDocument(t, fileName)

			artifacts, found := document.Paths["/apis/v2beta1/runs/{run_id}/artifacts"]["get"]
			require.True(t, found, "run artifacts operation must be documented")
			requireRequiredParameter(t, artifacts, "path", "run_id")

			lineage, found := document.Paths["/apis/v2beta1/runs/{run_id}/lineage"]["get"]
			require.True(t, found, "run lineage operation must be documented")
			requireRequiredParameter(t, lineage, "path", "run_id")

			task, found := document.Paths["/apis/v2beta1/runs/{run_id}/tasks/{task_id}"]["get"]
			require.True(t, found, "run task operation must be documented")
			requireRequiredParameter(t, task, "path", "run_id")
			requireRequiredParameter(t, task, "path", "task_id")

			tasks, found := document.Paths["/apis/v2beta1/runs/{run_id}/tasks"]["get"]
			require.True(t, found, "run tasks list operation must be documented")
			requireRequiredParameter(t, tasks, "path", "run_id")
		})
	}
}

func readSwaggerDocument(t *testing.T, fileName string) swaggerDocument {
	t.Helper()
	data, err := os.ReadFile(fileName)
	require.NoError(t, err)
	var document swaggerDocument
	require.NoError(t, json.Unmarshal(data, &document))
	return document
}

func requireRequiredParameter(t *testing.T, operation swaggerOperation, location, name string) {
	t.Helper()
	for _, parameter := range operation.Parameters {
		if parameter.In == location && parameter.Name == name {
			require.True(t, parameter.Required, "%s parameter %q must be required", location, name)
			return
		}
	}
	t.Fatalf("%s parameter %q was not found", location, name)
}

func requireOptionalUUIDParameter(t *testing.T, operation swaggerOperation, name string) {
	t.Helper()
	for _, parameter := range operation.Parameters {
		if parameter.In == "query" && parameter.Name == name {
			require.False(t, parameter.Required, "query parameter %q must be optional", name)
			require.Equal(t, "string", parameter.Type)
			require.Equal(t, "uuid", parameter.Format)
			return
		}
	}
	t.Fatalf("query parameter %q was not found", name)
}
