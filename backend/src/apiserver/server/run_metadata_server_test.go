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

import (
	"context"
	"testing"

	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type fakeRunMetadataReader struct {
	context            *pb.Context
	contextTypeName    string
	contextName        string
	contextExecutions  []*pb.Execution
	contextExecutionID int64
	executions         []*pb.Execution
	executionTypes     []*pb.ExecutionType
	executionIDs       []int64
	contextArtifacts   []*pb.Artifact
	contextArtifactID  int64
	artifacts          []*pb.Artifact
	artifactIDs        []int64
	events             []*pb.Event
	eventExecutionIDs  []int64
}

func (f *fakeRunMetadataReader) GetArtifacts(_ context.Context, ids []int64) ([]*pb.Artifact, error) {
	f.artifactIDs = append([]int64(nil), ids...)
	return f.artifacts, nil
}

func (f *fakeRunMetadataReader) GetArtifactsByContext(_ context.Context, contextID int64) ([]*pb.Artifact, error) {
	f.contextArtifactID = contextID
	return f.contextArtifacts, nil
}

func (f *fakeRunMetadataReader) GetArtifactTypes(context.Context, []int64) ([]*pb.ArtifactType, error) {
	return nil, nil
}

func (f *fakeRunMetadataReader) GetContextByTypeAndName(_ context.Context, typeName, contextName string) (*pb.Context, error) {
	f.contextTypeName = typeName
	f.contextName = contextName
	return f.context, nil
}

func (f *fakeRunMetadataReader) GetExecutions(_ context.Context, ids []int64) ([]*pb.Execution, error) {
	f.executionIDs = append([]int64(nil), ids...)
	return f.executions, nil
}

func (f *fakeRunMetadataReader) GetExecutionsByContext(_ context.Context, contextID int64) ([]*pb.Execution, error) {
	f.contextExecutionID = contextID
	return f.contextExecutions, nil
}

func (f *fakeRunMetadataReader) GetExecutionTypes(context.Context, []int64) ([]*pb.ExecutionType, error) {
	return f.executionTypes, nil
}

func (f *fakeRunMetadataReader) GetEventsByExecutionIDs(_ context.Context, ids []int64) ([]*pb.Event, error) {
	f.eventExecutionIDs = append([]int64(nil), ids...)
	return f.events, nil
}

func TestRunMetadataTaskArtifactIDs(t *testing.T) {
	task := &model.Task{
		UUID:        "task-1",
		MLMDInputs:  `{"dataset":{"artifact_ids":[11,12]}}`,
		MLMDOutputs: `{"model":{"artifact_ids":[21]}}`,
	}

	inputs, outputs, err := taskArtifactIDs(task)
	require.NoError(t, err)
	assert.Equal(t, map[int64][]string{11: {"dataset"}, 12: {"dataset"}}, inputs)
	assert.Equal(t, map[int64][]string{21: {"model"}}, outputs)
}

func TestRunMetadataArtifactUsagesPreferMLMDEvent(t *testing.T) {
	task := &model.Task{
		UUID:            "task-1",
		MLMDExecutionID: "101",
		MLMDOutputs:     `{"model":{"artifact_ids":[21]}}`,
	}
	eventType := pb.Event_OUTPUT
	snapshot := &runMetadataSnapshot{
		artifacts: map[int64]*apiv2beta1.RunArtifact{
			21: {ArtifactId: 21},
		},
		events: []*pb.Event{{
			ArtifactId:  proto.Int64(21),
			ExecutionId: proto.Int64(101),
			Type:        &eventType,
			Path: &pb.Event_Path{Steps: []*pb.Event_Path_Step{{
				Value: &pb.Event_Path_Step_Key{Key: "model"},
			}}},
		}},
		taskByExec: map[int64]*model.Task{101: task},
	}

	addArtifactUsages(snapshot, []*model.Task{task})
	require.Len(t, snapshot.artifacts[21].Usages, 1)
	assert.Equal(t, "OUTPUT", snapshot.artifacts[21].Usages[0].EventType)
	assert.Equal(t, "model", snapshot.artifacts[21].Usages[0].Key)
	assert.Equal(t, "task-1", snapshot.artifacts[21].Usages[0].TaskId)
}

func TestLoadRunMetadataUsesPipelineRunContextArtifactsAndEvents(t *testing.T) {
	output := pb.Event_OUTPUT
	reader := &fakeRunMetadataReader{
		context: &pb.Context{Id: proto.Int64(55)},
		contextExecutions: []*pb.Execution{{
			Id: proto.Int64(72),
			CustomProperties: map[string]*pb.Value{
				"task_name": {Value: &pb.Value_StringValue{StringValue: "say-hello"}},
			},
		}},
		contextArtifacts: []*pb.Artifact{{Id: proto.Int64(21), Uri: proto.String("minio://mlpipeline/hello")}},
		events: []*pb.Event{{
			ExecutionId: proto.Int64(72),
			ArtifactId:  proto.Int64(21),
			Type:        &output,
			Path: &pb.Event_Path{Steps: []*pb.Event_Path_Step{{
				Value: &pb.Event_Path_Step_Key{Key: "Output"},
			}}},
		}},
	}
	server := &RunServer{BaseRunServer: &BaseRunServer{options: &RunServerOptions{MetadataReader: reader}}}
	tasks := []*model.Task{{UUID: "task-say-hello", Name: "say-hello", RunID: "run-1"}}

	snapshot, err := server.loadRunMetadata(context.Background(), "run-1", tasks)
	require.NoError(t, err)
	assert.Equal(t, int64(55), reader.contextArtifactID)
	assert.Equal(t, []int64{72}, reader.eventExecutionIDs)
	assert.Empty(t, reader.artifactIDs)
	require.Contains(t, snapshot.artifacts, int64(21))
	assert.Equal(t, "minio://mlpipeline/hello", snapshot.artifacts[21].Uri)
	require.Len(t, snapshot.artifacts[21].Usages, 1)
	assert.Equal(t, "task-say-hello", snapshot.artifacts[21].Usages[0].TaskId)
	assert.Equal(t, int64(72), snapshot.artifacts[21].Usages[0].ExecutionId)
	assert.Equal(t, "OUTPUT", snapshot.artifacts[21].Usages[0].EventType)
}

func TestRunMetadataEventPath(t *testing.T) {
	path := &pb.Event_Path{Steps: []*pb.Event_Path_Step{
		{Value: &pb.Event_Path_Step_Key{Key: "models"}},
		{Value: &pb.Event_Path_Step_Index{Index: 2}},
		{Value: &pb.Event_Path_Step_Key{Key: "weights"}},
	}}
	assert.Equal(t, "models[2].weights", mlmdEventPath(path))
}

func TestRunMetadataValueConversion(t *testing.T) {
	values := mlmdValuesToStruct(map[string]*pb.Value{
		"epochs":  {Value: &pb.Value_IntValue{IntValue: 10}},
		"score":   {Value: &pb.Value_DoubleValue{DoubleValue: 0.98}},
		"enabled": {Value: &pb.Value_BoolValue{BoolValue: true}},
		"name":    {Value: &pb.Value_StringValue{StringValue: "model-a"}},
	})
	require.NotNil(t, values)
	assert.Equal(t, float64(10), values.Fields["epochs"].GetNumberValue())
	assert.Equal(t, 0.98, values.Fields["score"].GetNumberValue())
	assert.True(t, values.Fields["enabled"].GetBoolValue())
	assert.Equal(t, "model-a", values.Fields["name"].GetStringValue())
}

func TestLoadRunExecutionsIncludesTaskAndPodAndUsesBatchRead(t *testing.T) {
	complete := pb.Execution_COMPLETE
	reader := &fakeRunMetadataReader{
		executions: []*pb.Execution{
			{Id: proto.Int64(202), TypeId: proto.Int64(7), Name: proto.String("train"), LastKnownState: &complete},
			{
				Id:             proto.Int64(101),
				TypeId:         proto.Int64(7),
				Name:           proto.String("prepare"),
				LastKnownState: &complete,
				CustomProperties: map[string]*pb.Value{
					"pod_name": {Value: &pb.Value_StringValue{StringValue: "pipeline-prepare-101"}},
				},
			},
		},
		executionTypes: []*pb.ExecutionType{{Id: proto.Int64(7), Name: proto.String("system.ContainerExecution")}},
	}
	server := &RunServer{BaseRunServer: &BaseRunServer{options: &RunServerOptions{MetadataReader: reader}}}
	tasks := []*model.Task{
		{UUID: "task-train", PodName: "pipeline-train-202", MLMDExecutionID: "202"},
		{UUID: "task-prepare", MLMDExecutionID: "101"},
		{UUID: "task-without-execution", PodName: "pipeline-dag", MLMDExecutionID: ""},
	}

	executions, err := server.loadRunExecutions(context.Background(), "run-1", tasks)
	require.NoError(t, err)
	require.Len(t, executions, 2)
	assert.Equal(t, []int64{101, 202}, reader.executionIDs)
	assert.Equal(t, int64(101), executions[0].ExecutionId)
	assert.Equal(t, "task-prepare", executions[0].TaskId)
	assert.Equal(t, "pipeline-prepare-101", executions[0].PodName)
	assert.Equal(t, "system.ContainerExecution", executions[0].TypeName)
	assert.Equal(t, int64(202), executions[1].ExecutionId)
	assert.Equal(t, "task-train", executions[1].TaskId)
	assert.Equal(t, "pipeline-train-202", executions[1].PodName)
	assert.Equal(t, pipelineRunContextTypeName, reader.contextTypeName)
	assert.Equal(t, "run-1", reader.contextName)
}

func TestLoadRunExecutionsUsesPipelineRunContextAndIncludesRootExecution(t *testing.T) {
	complete := pb.Execution_COMPLETE
	reader := &fakeRunMetadataReader{
		context: &pb.Context{Id: proto.Int64(55)},
		contextExecutions: []*pb.Execution{
			{Id: proto.Int64(71), TypeId: proto.Int64(8), Name: proto.String("root"), LastKnownState: &complete},
			{
				Id:             proto.Int64(72),
				TypeId:         proto.Int64(7),
				Name:           proto.String("say-hello"),
				LastKnownState: &complete,
				CustomProperties: map[string]*pb.Value{
					"task_name": {Value: &pb.Value_StringValue{StringValue: "say-hello"}},
					"pod_name":  {Value: &pb.Value_StringValue{StringValue: "hello-pipeline-say-hello"}},
				},
			},
		},
		executionTypes: []*pb.ExecutionType{
			{Id: proto.Int64(7), Name: proto.String("system.ContainerExecution")},
			{Id: proto.Int64(8), Name: proto.String("system.DAGExecution")},
		},
	}
	server := &RunServer{BaseRunServer: &BaseRunServer{options: &RunServerOptions{MetadataReader: reader}}}
	tasks := []*model.Task{{UUID: "task-say-hello", Name: "say-hello", RunID: "run-cache-hit"}}

	executions, err := server.loadRunExecutions(context.Background(), "run-cache-hit", tasks)
	require.NoError(t, err)
	require.Len(t, executions, 2)
	assert.Equal(t, int64(55), reader.contextExecutionID)
	assert.Empty(t, reader.executionIDs)
	assert.Equal(t, int64(71), executions[0].ExecutionId)
	assert.Empty(t, executions[0].TaskId)
	assert.Equal(t, "system.DAGExecution", executions[0].TypeName)
	assert.Equal(t, int64(72), executions[1].ExecutionId)
	assert.Equal(t, "task-say-hello", executions[1].TaskId)
	assert.Equal(t, "hello-pipeline-say-hello", executions[1].PodName)
}

func TestLoadRunExecutionSelectsRequestedTaskExecutionFromRunContext(t *testing.T) {
	reader := &fakeRunMetadataReader{
		context: &pb.Context{Id: proto.Int64(55)},
		contextExecutions: []*pb.Execution{
			{Id: proto.Int64(71)},
			{Id: proto.Int64(72)},
		},
	}
	server := &RunServer{BaseRunServer: &BaseRunServer{options: &RunServerOptions{MetadataReader: reader}}}
	task := &model.Task{UUID: "task-say-hello", RunID: "run-1", MLMDExecutionID: "72"}

	execution, err := server.loadRunExecution(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, int64(72), execution.ExecutionId)
	assert.Equal(t, "task-say-hello", execution.TaskId)
}

func TestEnrichTaskPodNamesFromExecutions(t *testing.T) {
	tasks := []*apiv2beta1.PipelineTaskDetail{
		{TaskId: "task-1"},
		{TaskId: "task-2", PodName: "db-pod"},
	}
	executions := []*apiv2beta1.RunExecution{
		{TaskId: "task-1", PodName: "mlmd-pod"},
		{TaskId: "task-2", PodName: "different-mlmd-pod"},
	}

	enrichTaskPodNamesFromExecutions(tasks, executions)

	assert.Equal(t, "mlmd-pod", tasks[0].PodName)
	assert.Equal(t, "db-pod", tasks[1].PodName)
}

func TestEnrichTasksFromExecutionsBackfillsExecutionAndPod(t *testing.T) {
	tasks := []*apiv2beta1.PipelineTaskDetail{
		{TaskId: "task-1"},
		{TaskId: "task-2", ExecutionId: 201, PodName: "db-pod"},
	}
	executions := []*apiv2beta1.RunExecution{
		{ExecutionId: 101, TaskId: "task-1", PodName: "mlmd-pod"},
		{ExecutionId: 202, TaskId: "task-2", PodName: "different-mlmd-pod"},
	}

	enrichTasksFromExecutions(tasks, executions)

	assert.Equal(t, int64(101), tasks[0].ExecutionId)
	assert.Equal(t, "mlmd-pod", tasks[0].PodName)
	assert.Equal(t, int64(202), tasks[1].ExecutionId)
	assert.Equal(t, "db-pod", tasks[1].PodName)
}

func TestArtifactsForExecutionSeparatesInputsAndOutputs(t *testing.T) {
	snapshot := &runMetadataSnapshot{artifacts: map[int64]*apiv2beta1.RunArtifact{
		11: {
			ArtifactId: 11,
			Usages: []*apiv2beta1.RunArtifactUsage{
				{ExecutionId: 72, EventType: "INPUT"},
				{ExecutionId: 73, EventType: "OUTPUT"},
			},
		},
		12: {ArtifactId: 12, Usages: []*apiv2beta1.RunArtifactUsage{{ExecutionId: 72, EventType: "OUTPUT"}}},
		13: {ArtifactId: 13, Usages: []*apiv2beta1.RunArtifactUsage{{ExecutionId: 72, EventType: "INTERNAL_OUTPUT"}}},
	}}

	inputs, outputs := artifactsForExecution(snapshot, 72)

	require.Len(t, inputs, 1)
	assert.Equal(t, int64(11), inputs[0].ArtifactId)
	require.Len(t, outputs, 2)
	assert.Equal(t, int64(12), outputs[0].ArtifactId)
	assert.Equal(t, int64(13), outputs[1].ArtifactId)
}

func TestToApiPipelineTaskDetailIncludesPodName(t *testing.T) {
	task := &model.Task{UUID: "task-1", RunID: "run-1", PodName: "pipeline-task-123"}

	detail := toApiPipelineTaskDetail(task)

	assert.Equal(t, "pipeline-task-123", detail.PodName)
}
