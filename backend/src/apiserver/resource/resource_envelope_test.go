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

package resource

import (
	"testing"

	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCalculateResourceEnvelopeParallelAndSequentialTasks(t *testing.T) {
	spec := envelopeTestPipelineSpec(map[string]*pipelinespec.PipelineTaskSpec{
		"a": envelopeTestTask("comp-a"),
		"b": envelopeTestTask("comp-b"),
		"c": {
			ComponentRef:   &pipelinespec.ComponentRef{Name: "comp-c"},
			DependentTasks: []string{"a", "b"},
		},
	}, map[string][6]string{
		"a": {"1", "2Gi", "2", "3Gi"},
		"b": {"2", "1Gi", "3", "2Gi"},
		"c": {"4", "2Gi", "5", "4Gi"},
	})

	envelope, err := CalculateResourceEnvelope(spec)
	require.NoError(t, err)
	assert.Equal(t, "EXACT", envelope.Calculation)
	assert.Equal(t, "DECLARED_CONTAINER_RESOURCES", envelope.Scope)
	// a and b can run together (3 CPU, 3Gi), while c runs after both and
	// requires 4 CPU, 2Gi. Peaks are calculated independently per resource.
	assert.Equal(t, map[string]string{"cpu": "4", "memory": "3Gi"}, envelope.Requests)
	assert.Equal(t, map[string]string{"cpu": "5", "memory": "5Gi"}, envelope.Limits)
	assert.Empty(t, envelope.Warnings)
}

func TestCalculateResourceEnvelopeStaticParallelFor(t *testing.T) {
	task := envelopeTestTask("comp-a")
	task.Iterator = &pipelinespec.PipelineTaskSpec_ParameterIterator{
		ParameterIterator: &pipelinespec.ParameterIteratorSpec{
			Items: &pipelinespec.ParameterIteratorSpec_ItemsSpec{
				Kind: &pipelinespec.ParameterIteratorSpec_ItemsSpec_Raw{Raw: `[1, 2, 3]`},
			},
		},
	}
	spec := envelopeTestPipelineSpec(
		map[string]*pipelinespec.PipelineTaskSpec{"fan-out": task},
		map[string][6]string{"a": {"500m", "1Gi", "1", "2Gi"}},
	)

	envelope, err := CalculateResourceEnvelope(spec)
	require.NoError(t, err)
	assert.Equal(t, "EXACT", envelope.Calculation)
	assert.Equal(t, "1500m", envelope.Requests["cpu"])
	assert.Equal(t, "3Gi", envelope.Requests["memory"])
}

func TestCalculateResourceEnvelopeMarksRuntimeValuesUnknown(t *testing.T) {
	spec := envelopeTestPipelineSpec(
		map[string]*pipelinespec.PipelineTaskSpec{"a": envelopeTestTask("comp-a")},
		map[string][6]string{"a": {"{{$.inputs.parameters['cpu']}}", "1Gi", "", ""}},
	)

	envelope, err := CalculateResourceEnvelope(spec)
	require.NoError(t, err)
	assert.Equal(t, "UNKNOWN", envelope.Calculation)
	assert.Equal(t, map[string]string{"memory": "1Gi"}, envelope.Requests)
	assert.NotEmpty(t, envelope.Warnings)
}

func TestCalculateResourceEnvelopeUsesDynamicIteratorParallelismLimit(t *testing.T) {
	task := envelopeTestTask("comp-a")
	task.Iterator = &pipelinespec.PipelineTaskSpec_ParameterIterator{
		ParameterIterator: &pipelinespec.ParameterIteratorSpec{
			Items: &pipelinespec.ParameterIteratorSpec_ItemsSpec{
				Kind: &pipelinespec.ParameterIteratorSpec_ItemsSpec_InputParameter{InputParameter: "items"},
			},
		},
	}
	task.IteratorPolicy = &pipelinespec.PipelineTaskSpec_IteratorPolicy{ParallelismLimit: 4}
	spec := envelopeTestPipelineSpec(
		map[string]*pipelinespec.PipelineTaskSpec{"fan-out": task},
		map[string][6]string{"a": {"500m", "1Gi", "", ""}},
	)

	envelope, err := CalculateResourceEnvelope(spec)
	require.NoError(t, err)
	assert.Equal(t, "UPPER_BOUND", envelope.Calculation)
	assert.Equal(t, "2", envelope.Requests["cpu"])
	assert.Equal(t, "4Gi", envelope.Requests["memory"])
}

func TestCalculateResourceEnvelopeGPUsByModel(t *testing.T) {
	spec := envelopeTestPipelineSpec(
		map[string]*pipelinespec.PipelineTaskSpec{
			"a": envelopeTestTask("comp-a"),
			"b": envelopeTestTask("comp-b"),
			"c": envelopeTestTask("comp-c"),
		},
		map[string][6]string{
			"a": {"", "", "", "", "nvidia.com/gpu", "1"},
			"b": {"", "", "", "", "nvidia.com/gpu", "2"},
			"c": {"", "", "", "", "amd.com/gpu", "4"},
		},
	)
	platformSpec := envelopeTestPlatformSpec(map[string]string{
		"a": "NVIDIA_A100_80GB",
		"b": "NVIDIA_A100_80GB",
		"c": "AMD_MI300X",
	})

	envelope, err := CalculateResourceEnvelope(spec, platformSpec)
	require.NoError(t, err)
	assert.Equal(t, "v2", envelope.CalculatorVersion)
	assert.Equal(t, []GPUEnvelope{
		{GPUModel: "AMD_MI300X", GPUCount: "4"},
		{GPUModel: "NVIDIA_A100_80GB", GPUCount: "3"},
	}, envelope.GPUs)
	assert.Equal(t, "3", envelope.Limits["nvidia.com/gpu"])
	assert.Equal(t, "4", envelope.Limits["amd.com/gpu"])
}

func envelopeTestTask(component string) *pipelinespec.PipelineTaskSpec {
	return &pipelinespec.PipelineTaskSpec{ComponentRef: &pipelinespec.ComponentRef{Name: component}}
}

func envelopeTestPipelineSpec(tasks map[string]*pipelinespec.PipelineTaskSpec, resources map[string][6]string) *pipelinespec.PipelineSpec {
	components := map[string]*pipelinespec.ComponentSpec{}
	executors := map[string]*pipelinespec.PipelineDeploymentConfig_ExecutorSpec{}
	for name, values := range resources {
		componentName := "comp-" + name
		executorName := "exec-" + name
		components[componentName] = &pipelinespec.ComponentSpec{
			Implementation: &pipelinespec.ComponentSpec_ExecutorLabel{ExecutorLabel: executorName},
		}
		executors[executorName] = &pipelinespec.PipelineDeploymentConfig_ExecutorSpec{
			Spec: &pipelinespec.PipelineDeploymentConfig_ExecutorSpec_Container{
				Container: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec{
					Resources: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec_ResourceSpec{
						ResourceCpuRequest:    values[0],
						ResourceMemoryRequest: values[1],
						ResourceCpuLimit:      values[2],
						ResourceMemoryLimit:   values[3],
						Accelerator: &pipelinespec.PipelineDeploymentConfig_PipelineContainerSpec_ResourceSpec_AcceleratorConfig{
							ResourceType:  values[4],
							ResourceCount: values[5],
						},
					},
				},
			},
		}
	}
	deployment := &pipelinespec.PipelineDeploymentConfig{Executors: executors}
	deploymentJSON, err := protojson.Marshal(deployment)
	if err != nil {
		panic(err)
	}
	deploymentStruct := &structpb.Struct{}
	if err := protojson.Unmarshal(deploymentJSON, deploymentStruct); err != nil {
		panic(err)
	}
	return &pipelinespec.PipelineSpec{
		Root: &pipelinespec.ComponentSpec{
			Implementation: &pipelinespec.ComponentSpec_Dag{Dag: &pipelinespec.DagSpec{Tasks: tasks}},
		},
		Components:     components,
		DeploymentSpec: deploymentStruct,
	}
}

func envelopeTestPlatformSpec(models map[string]string) *pipelinespec.PlatformSpec {
	executors := make(map[string]*structpb.Struct, len(models))
	for name, model := range models {
		executor, err := structpb.NewStruct(map[string]interface{}{
			"nodeSelector": map[string]interface{}{
				"labels": map[string]interface{}{
					"nvidia.com/gpu.product": model,
				},
			},
		})
		if err != nil {
			panic(err)
		}
		executors["exec-"+name] = executor
	}
	return &pipelinespec.PlatformSpec{
		Platforms: map[string]*pipelinespec.SinglePlatformSpec{
			"kubernetes": {
				DeploymentSpec: &pipelinespec.PlatformDeploymentConfig{Executors: executors},
			},
		},
	}
}
