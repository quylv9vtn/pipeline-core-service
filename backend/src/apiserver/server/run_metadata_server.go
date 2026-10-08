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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	apiv2beta1 "github.com/kubeflow/pipelines/backend/api/v2beta1/go_client"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	pb "github.com/kubeflow/pipelines/third_party/ml-metadata/go/ml_metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RunMetadataReader is deliberately narrower than the complete MLMD API. The
// API server only needs these read operations and never exposes MLMD writes.
type RunMetadataReader interface {
	GetArtifacts(ctx context.Context, ids []int64) ([]*pb.Artifact, error)
	GetArtifactsByContext(ctx context.Context, contextID int64) ([]*pb.Artifact, error)
	GetArtifactTypes(ctx context.Context, typeIds []int64) ([]*pb.ArtifactType, error)
	GetContextByTypeAndName(ctx context.Context, typeName, contextName string) (*pb.Context, error)
	GetExecutions(ctx context.Context, ids []int64) ([]*pb.Execution, error)
	GetExecutionsByContext(ctx context.Context, contextID int64) ([]*pb.Execution, error)
	GetExecutionTypes(ctx context.Context, typeIds []int64) ([]*pb.ExecutionType, error)
	GetEventsByExecutionIDs(ctx context.Context, executionIds []int64) ([]*pb.Event, error)
	// ListAllArtifacts returns a page of all artifacts from MLMD using native
	// MLMD pagination. nextPageToken is empty when there are no more pages.
	ListAllArtifacts(ctx context.Context, pageSize int32, pageToken string) (artifacts []*pb.Artifact, nextPageToken string, err error)
}

const (
	runMetadataRequestTimeout  = 15 * time.Second
	pipelineRunContextTypeName = "system.PipelineRun"
)

type runMetadataSnapshot struct {
	artifacts  map[int64]*apiv2beta1.RunArtifact
	events     []*pb.Event
	taskByExec map[int64]*model.Task
}

func (s *RunServer) metadataReader() (RunMetadataReader, error) {
	if s.options == nil || s.options.MetadataReader == nil {
		return nil, status.Error(codes.Unavailable, "ML Metadata client is not configured")
	}
	return s.options.MetadataReader, nil
}

// ListRunArtifacts lists the MLMD artifacts used by executions belonging to a
// run. Calling getRun first is important: it performs namespace-aware RBAC and
// also supplies the authoritative run-to-task boundary.
func (s *RunServer) ListRunArtifacts(ctx context.Context, request *apiv2beta1.ListRunArtifactsRequest) (*apiv2beta1.ListRunArtifactsResponse, error) {
	if request.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	run, err := s.getRun(ctx, request.GetRunId())
	if err != nil {
		return nil, util.Wrap(err, "Failed to authorize or retrieve the run")
	}

	snapshot, err := s.loadRunMetadata(ctx, run.UUID, run.RunDetails.TaskDetails)
	if err != nil {
		return nil, util.Wrap(err, "Failed to read run artifacts from ML Metadata")
	}
	artifacts := sortedRunArtifacts(snapshot.artifacts)
	return &apiv2beta1.ListRunArtifactsResponse{RunId: run.UUID, Artifacts: artifacts}, nil
}

// ListRunExecutions lists the MLMD executions referenced by tasks in a run.
// The run lookup is deliberately performed before the MLMD read so namespace
// authorization is enforced and execution IDs cannot be used across runs.
func (s *RunServer) ListRunExecutions(ctx context.Context, request *apiv2beta1.ListRunExecutionsRequest) (*apiv2beta1.ListRunExecutionsResponse, error) {
	if request.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	run, err := s.getRun(ctx, request.GetRunId())
	if err != nil {
		return nil, util.Wrap(err, "Failed to authorize or retrieve the run")
	}
	executions, err := s.loadRunExecutions(ctx, run.UUID, run.RunDetails.TaskDetails)
	if err != nil {
		return nil, util.Wrap(err, "Failed to read run executions from ML Metadata")
	}
	return &apiv2beta1.ListRunExecutionsResponse{
		RunId:      run.UUID,
		Executions: executions,
		TotalSize:  int32(len(executions)),
	}, nil
}

// GetRunExecution returns one execution only when a task in the requested run
// references it. This prevents callers from probing the cluster-wide MLMD
// store with execution IDs from another project.
func (s *RunServer) GetRunExecution(ctx context.Context, request *apiv2beta1.GetRunExecutionRequest) (*apiv2beta1.RunExecution, error) {
	if request.GetRunId() == "" || request.GetExecutionId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "run_id and a positive execution_id are required")
	}
	run, err := s.getRun(ctx, request.GetRunId())
	if err != nil {
		return nil, util.Wrap(err, "Failed to authorize or retrieve the run")
	}
	executions, err := s.loadRunExecutions(ctx, run.UUID, run.RunDetails.TaskDetails)
	if err != nil {
		return nil, util.Wrap(err, "Failed to read run execution from ML Metadata")
	}
	for _, execution := range executions {
		if execution.GetExecutionId() == request.GetExecutionId() {
			snapshot, metadataErr := s.loadRunMetadata(ctx, run.UUID, run.RunDetails.TaskDetails)
			if metadataErr != nil {
				return nil, util.Wrap(metadataErr, "Failed to read execution artifacts from ML Metadata")
			}
			execution.InputArtifacts, execution.OutputArtifacts = artifactsForExecution(snapshot, execution.GetExecutionId())
			return execution, nil
		}
	}
	return nil, util.NewResourceNotFoundError("Execution", strconv.FormatInt(request.GetExecutionId(), 10))
}

// ListRunTasks returns task summaries from the pipeline DB and enriches their
// execution IDs and pod names from the authoritative PipelineRun MLMD context.
func (s *RunServer) ListRunTasks(ctx context.Context, request *apiv2beta1.ListRunTasksRequest) (*apiv2beta1.ListRunTasksResponse, error) {
	if request.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	run, err := s.getRun(ctx, request.GetRunId())
	if err != nil {
		return nil, util.Wrap(err, "Failed to authorize or retrieve the run")
	}
	tasks := toApiPipelineTaskDetails(run.RunDetails.TaskDetails)
	executions, err := s.loadRunExecutions(ctx, run.UUID, run.RunDetails.TaskDetails)
	if err != nil {
		return nil, util.Wrap(err, "Failed to read task executions from ML Metadata")
	}
	enrichTasksFromExecutions(tasks, executions)
	sort.Slice(tasks, func(i, j int) bool {
		leftCreatedAt := tasks[i].GetCreateTime().GetSeconds()
		rightCreatedAt := tasks[j].GetCreateTime().GetSeconds()
		if leftCreatedAt == rightCreatedAt {
			return tasks[i].GetTaskId() < tasks[j].GetTaskId()
		}
		return leftCreatedAt < rightCreatedAt
	})
	return &apiv2beta1.ListRunTasksResponse{RunId: run.UUID, Tasks: tasks, TotalSize: int32(len(tasks))}, nil
}

// GetRunTask returns a task from the pipeline DB, enriched with its execution
// and artifacts from MLMD. A task ID from another run is intentionally reported
// as not found so that cross-run identifiers are not disclosed.
func (s *RunServer) GetRunTask(ctx context.Context, request *apiv2beta1.GetRunTaskRequest) (*apiv2beta1.RunTask, error) {
	if request.GetRunId() == "" || request.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id and task_id are required")
	}
	if _, err := s.getRun(ctx, request.GetRunId()); err != nil {
		return nil, util.Wrap(err, "Failed to authorize or retrieve the run")
	}
	task, err := s.resourceManager.GetTask(request.GetTaskId())
	if err != nil {
		return nil, util.Wrap(err, "Failed to retrieve the task")
	}
	if task.RunID != request.GetRunId() {
		return nil, util.NewResourceNotFoundError("Task", request.GetTaskId())
	}

	snapshot, err := s.loadRunMetadata(ctx, task.RunID, []*model.Task{task})
	if err != nil {
		return nil, util.Wrap(err, "Failed to read task metadata from ML Metadata")
	}
	result := &apiv2beta1.RunTask{Task: toApiPipelineTaskDetail(task)}
	executions, err := s.loadRunExecutions(ctx, task.RunID, []*model.Task{task})
	if err != nil {
		return nil, util.Wrap(err, "Failed to read task execution from ML Metadata")
	}
	for _, execution := range executions {
		if execution.GetTaskId() == task.UUID {
			result.Execution = execution
			result.Task.ExecutionId = execution.GetExecutionId()
			if result.Task.GetPodName() == "" {
				result.Task.PodName = execution.GetPodName()
			}
			break
		}
	}

	inputIDs, outputIDs, err := taskArtifactIDs(task)
	if err != nil {
		return nil, util.Wrap(err, "Failed to parse task artifact references")
	}
	for artifactID, artifact := range snapshot.artifacts {
		for _, usage := range artifact.Usages {
			if usage.TaskId != task.UUID {
				continue
			}
			switch mlmdEventDirectionByName(usage.EventType) {
			case "INPUT":
				inputIDs[artifactID] = appendUniqueString(inputIDs[artifactID], usage.Key)
			case "OUTPUT":
				outputIDs[artifactID] = appendUniqueString(outputIDs[artifactID], usage.Key)
			}
		}
	}
	result.InputArtifacts = selectArtifacts(snapshot.artifacts, inputIDs)
	result.OutputArtifacts = selectArtifacts(snapshot.artifacts, outputIDs)
	return result, nil
}

// GetRunLineage returns a directed graph. INPUT edges point artifact -> task;
// OUTPUT edges point task -> artifact. The edge type retains the exact MLMD
// event type so callers can distinguish declared, internal and pending edges.
func (s *RunServer) GetRunLineage(ctx context.Context, request *apiv2beta1.GetRunLineageRequest) (*apiv2beta1.RunLineage, error) {
	if request.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	run, err := s.getRun(ctx, request.GetRunId())
	if err != nil {
		return nil, util.Wrap(err, "Failed to authorize or retrieve the run")
	}
	tasks := run.RunDetails.TaskDetails
	snapshot, err := s.loadRunMetadata(ctx, run.UUID, tasks)
	if err != nil {
		return nil, util.Wrap(err, "Failed to read run lineage from ML Metadata")
	}

	executions, err := s.loadRunExecutions(ctx, run.UUID, tasks)
	if err != nil {
		return nil, util.Wrap(err, "Failed to read run executions from ML Metadata")
	}
	taskDetails := toApiPipelineTaskDetails(tasks)
	enrichTaskPodNamesFromExecutions(taskDetails, executions)
	taskDetailByID := make(map[string]*apiv2beta1.PipelineTaskDetail, len(taskDetails))
	for _, taskDetail := range taskDetails {
		taskDetailByID[taskDetail.GetTaskId()] = taskDetail
	}

	lineage := &apiv2beta1.RunLineage{RunId: run.UUID}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].UUID < tasks[j].UUID })
	for _, task := range tasks {
		lineage.Nodes = append(lineage.Nodes, &apiv2beta1.RunLineageNode{
			NodeId: "task:" + task.UUID,
			Node:   &apiv2beta1.RunLineageNode_Task{Task: taskDetailByID[task.UUID]},
		})
	}
	for _, artifact := range sortedRunArtifacts(snapshot.artifacts) {
		lineage.Nodes = append(lineage.Nodes, &apiv2beta1.RunLineageNode{
			NodeId: fmt.Sprintf("artifact:%d", artifact.ArtifactId),
			Node:   &apiv2beta1.RunLineageNode_Artifact{Artifact: artifact},
		})
	}

	seenEdges := make(map[string]bool)
	for artifactID, artifact := range snapshot.artifacts {
		for _, usage := range artifact.Usages {
			if usage.TaskId == "" {
				continue
			}
			direction := mlmdEventDirectionByName(usage.EventType)
			if direction != "INPUT" && direction != "OUTPUT" {
				continue
			}
			artifactNode := fmt.Sprintf("artifact:%d", artifactID)
			taskNode := "task:" + usage.TaskId
			source, target := artifactNode, taskNode
			if direction == "OUTPUT" {
				source, target = taskNode, artifactNode
			}
			dedupeKey := fmt.Sprintf("%s|%s|%s|%s", source, target, usage.EventType, usage.Key)
			if seenEdges[dedupeKey] {
				continue
			}
			seenEdges[dedupeKey] = true
			lineage.Edges = append(lineage.Edges, &apiv2beta1.RunLineageEdge{
				SourceNodeId: source,
				TargetNodeId: target,
				TaskId:       usage.TaskId,
				ExecutionId:  usage.ExecutionId,
				ArtifactId:   artifactID,
				Key:          usage.Key,
				EventType:    usage.EventType,
				RecordedAt:   usage.RecordedAt,
			})
		}
	}
	sort.Slice(lineage.Edges, func(i, j int) bool {
		left := lineage.Edges[i].SourceNodeId + "|" + lineage.Edges[i].TargetNodeId + "|" + lineage.Edges[i].Key
		right := lineage.Edges[j].SourceNodeId + "|" + lineage.Edges[j].TargetNodeId + "|" + lineage.Edges[j].Key
		return left < right
	})
	return lineage, nil
}

func (s *RunServer) loadRunMetadata(ctx context.Context, runID string, tasks []*model.Task) (*runMetadataSnapshot, error) {
	reader, err := s.metadataReader()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, runMetadataRequestTimeout)
	defer cancel()
	snapshot := &runMetadataSnapshot{
		artifacts:  make(map[int64]*apiv2beta1.RunArtifact),
		taskByExec: make(map[int64]*model.Task),
	}
	taskIndex := newTaskMetadataIndex(tasks)
	artifactIDSet := make(map[int64]bool)
	executionIDSet := make(map[int64]bool)
	artifactsByID := make(map[int64]*pb.Artifact)
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if executionID, parseErr := strconv.ParseInt(task.MLMDExecutionID, 10, 64); parseErr == nil && executionID > 0 {
			snapshot.taskByExec[executionID] = task
			executionIDSet[executionID] = true
		}
		inputs, outputs, parseErr := taskArtifactIDs(task)
		if parseErr != nil {
			return nil, parseErr
		}
		for id := range inputs {
			artifactIDSet[id] = true
		}
		for id := range outputs {
			artifactIDSet[id] = true
		}
	}
	if runID != "" {
		runContext, contextErr := reader.GetContextByTypeAndName(ctx, pipelineRunContextTypeName, runID)
		if contextErr != nil {
			return nil, contextErr
		}
		if runContext != nil && runContext.GetId() > 0 {
			contextExecutions, contextExecutionsErr := reader.GetExecutionsByContext(ctx, runContext.GetId())
			if contextExecutionsErr != nil {
				return nil, contextExecutionsErr
			}
			for _, execution := range contextExecutions {
				if execution == nil || execution.GetId() <= 0 {
					continue
				}
				executionIDSet[execution.GetId()] = true
				if task := taskForExecution(taskIndex, execution); task != nil {
					snapshot.taskByExec[execution.GetId()] = task
				}
			}
			contextArtifacts, contextArtifactsErr := reader.GetArtifactsByContext(ctx, runContext.GetId())
			if contextArtifactsErr != nil {
				return nil, contextArtifactsErr
			}
			for _, artifact := range contextArtifacts {
				if artifact != nil && artifact.GetId() > 0 {
					artifactsByID[artifact.GetId()] = artifact
					artifactIDSet[artifact.GetId()] = true
				}
			}
		}
	}
	executionIDs := sortedInt64Keys(executionIDSet)
	if len(executionIDs) > 0 {
		snapshot.events, err = reader.GetEventsByExecutionIDs(ctx, executionIDs)
		if err != nil {
			return nil, err
		}
		for _, event := range snapshot.events {
			if event.GetArtifactId() > 0 {
				artifactIDSet[event.GetArtifactId()] = true
			}
		}
	}
	if len(artifactIDSet) == 0 {
		return snapshot, nil
	}

	missingArtifactIDs := make([]int64, 0, len(artifactIDSet))
	for _, artifactID := range sortedInt64Keys(artifactIDSet) {
		if artifactsByID[artifactID] == nil {
			missingArtifactIDs = append(missingArtifactIDs, artifactID)
		}
	}
	if len(missingArtifactIDs) > 0 {
		artifacts, artifactErr := reader.GetArtifacts(ctx, missingArtifactIDs)
		if artifactErr != nil {
			return nil, artifactErr
		}
		for _, artifact := range artifacts {
			if artifact != nil && artifact.GetId() > 0 {
				artifactsByID[artifact.GetId()] = artifact
			}
		}
	}
	artifacts := make([]*pb.Artifact, 0, len(artifactsByID))
	for _, artifact := range artifactsByID {
		artifacts = append(artifacts, artifact)
	}
	typeIDSet := make(map[int64]bool)
	for _, artifact := range artifacts {
		if artifact.GetTypeId() > 0 {
			typeIDSet[artifact.GetTypeId()] = true
		}
	}
	typeNames := make(map[int64]string)
	if len(typeIDSet) > 0 {
		types, typeErr := reader.GetArtifactTypes(ctx, sortedInt64Keys(typeIDSet))
		if typeErr != nil {
			return nil, typeErr
		}
		for _, artifactType := range types {
			typeNames[artifactType.GetId()] = artifactType.GetName()
		}
	}
	for _, artifact := range artifacts {
		typeName := artifact.GetType()
		if typeName == "" {
			typeName = typeNames[artifact.GetTypeId()]
		}
		snapshot.artifacts[artifact.GetId()] = &apiv2beta1.RunArtifact{
			ArtifactId:       artifact.GetId(),
			TypeId:           artifact.GetTypeId(),
			TypeName:         typeName,
			Name:             artifact.GetName(),
			Uri:              artifact.GetUri(),
			ExternalId:       artifact.GetExternalId(),
			State:            artifact.GetState().String(),
			Properties:       mlmdValuesToStruct(artifact.GetProperties()),
			CustomProperties: mlmdValuesToStruct(artifact.GetCustomProperties()),
			CreatedAt:        timestampFromMillis(artifact.GetCreateTimeSinceEpoch()),
			LastUpdatedAt:    timestampFromMillis(artifact.GetLastUpdateTimeSinceEpoch()),
		}
	}
	addArtifactUsages(snapshot, tasks)
	return snapshot, nil
}

func (s *RunServer) loadRunExecution(ctx context.Context, task *model.Task) (*apiv2beta1.RunExecution, error) {
	executions, err := s.loadRunExecutions(ctx, task.RunID, []*model.Task{task})
	if err != nil {
		return nil, err
	}
	executionID, _ := strconv.ParseInt(task.MLMDExecutionID, 10, 64)
	for _, execution := range executions {
		if execution.GetExecutionId() == executionID {
			return execution, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "ML Metadata execution %d was not found", executionID)
}

func (s *RunServer) loadRunExecutions(ctx context.Context, runID string, tasks []*model.Task) ([]*apiv2beta1.RunExecution, error) {
	reader, err := s.metadataReader()
	if err != nil {
		return nil, err
	}
	taskIndex := newTaskMetadataIndex(tasks)
	fallbackExecutionIDs := make([]int64, 0, len(tasks))
	seenFallbackExecutionIDs := make(map[int64]bool)
	for _, task := range tasks {
		if task == nil {
			continue
		}
		executionID, parseErr := strconv.ParseInt(task.MLMDExecutionID, 10, 64)
		if parseErr != nil || executionID <= 0 {
			continue
		}
		if seenFallbackExecutionIDs[executionID] {
			continue
		}
		seenFallbackExecutionIDs[executionID] = true
		fallbackExecutionIDs = append(fallbackExecutionIDs, executionID)
	}

	ctx, cancel := context.WithTimeout(ctx, runMetadataRequestTimeout)
	defer cancel()
	executionByID := make(map[int64]*pb.Execution)
	if runID != "" {
		runContext, contextErr := reader.GetContextByTypeAndName(ctx, pipelineRunContextTypeName, runID)
		if contextErr != nil {
			return nil, contextErr
		}
		if runContext != nil && runContext.GetId() > 0 {
			contextExecutions, contextExecutionsErr := reader.GetExecutionsByContext(ctx, runContext.GetId())
			if contextExecutionsErr != nil {
				return nil, contextExecutionsErr
			}
			for _, execution := range contextExecutions {
				if execution != nil && execution.GetId() > 0 {
					executionByID[execution.GetId()] = execution
				}
			}
		}
	}

	missingExecutionIDs := make([]int64, 0, len(fallbackExecutionIDs))
	for _, executionID := range fallbackExecutionIDs {
		if executionByID[executionID] == nil {
			missingExecutionIDs = append(missingExecutionIDs, executionID)
		}
	}
	if len(missingExecutionIDs) > 0 {
		sort.Slice(missingExecutionIDs, func(i, j int) bool { return missingExecutionIDs[i] < missingExecutionIDs[j] })
		fallbackExecutions, fallbackErr := reader.GetExecutions(ctx, missingExecutionIDs)
		if fallbackErr != nil {
			return nil, fallbackErr
		}
		for _, execution := range fallbackExecutions {
			if execution != nil && execution.GetId() > 0 {
				executionByID[execution.GetId()] = execution
			}
		}
	}
	if len(executionByID) == 0 {
		return []*apiv2beta1.RunExecution{}, nil
	}
	executions := make([]*pb.Execution, 0, len(executionByID))
	for _, execution := range executionByID {
		executions = append(executions, execution)
	}
	typeIDSet := make(map[int64]bool)
	for _, execution := range executions {
		if execution.GetTypeId() > 0 && execution.GetType() == "" {
			typeIDSet[execution.GetTypeId()] = true
		}
	}
	typeNames := make(map[int64]string)
	if len(typeIDSet) > 0 {
		types, typeErr := reader.GetExecutionTypes(ctx, sortedInt64Keys(typeIDSet))
		if typeErr != nil {
			return nil, typeErr
		}
		for _, executionType := range types {
			typeNames[executionType.GetId()] = executionType.GetName()
		}
	}
	sort.Slice(executions, func(i, j int) bool { return executions[i].GetId() < executions[j].GetId() })
	result := make([]*apiv2beta1.RunExecution, 0, len(executions))
	for _, execution := range executions {
		task := taskForExecution(taskIndex, execution)
		podName := executionPodName(execution)
		typeName := execution.GetType()
		if typeName == "" {
			typeName = typeNames[execution.GetTypeId()]
		}
		taskID := ""
		if task != nil {
			taskID = task.UUID
			if task.PodName != "" {
				podName = task.PodName
			}
		}
		result = append(result, &apiv2beta1.RunExecution{
			ExecutionId:      execution.GetId(),
			TypeId:           execution.GetTypeId(),
			TypeName:         typeName,
			Name:             execution.GetName(),
			ExternalId:       execution.GetExternalId(),
			State:            execution.GetLastKnownState().String(),
			Properties:       mlmdValuesToStruct(execution.GetProperties()),
			CustomProperties: mlmdValuesToStruct(execution.GetCustomProperties()),
			CreatedAt:        timestampFromMillis(execution.GetCreateTimeSinceEpoch()),
			LastUpdatedAt:    timestampFromMillis(execution.GetLastUpdateTimeSinceEpoch()),
			TaskId:           taskID,
			PodName:          podName,
		})
	}
	return result, nil
}

type taskMetadataIndex struct {
	byExecutionID map[int64]*model.Task
	byPodName     map[string]*model.Task
	byName        map[string]*model.Task
}

func newTaskMetadataIndex(tasks []*model.Task) *taskMetadataIndex {
	index := &taskMetadataIndex{
		byExecutionID: make(map[int64]*model.Task),
		byPodName:     make(map[string]*model.Task),
		byName:        make(map[string]*model.Task),
	}
	duplicatePodNames := make(map[string]bool)
	duplicateTaskNames := make(map[string]bool)
	for _, task := range tasks {
		if task == nil {
			continue
		}
		addUniqueTaskMapping(index.byPodName, duplicatePodNames, task.PodName, task)
		addUniqueTaskMapping(index.byName, duplicateTaskNames, task.Name, task)
		if executionID, err := strconv.ParseInt(task.MLMDExecutionID, 10, 64); err == nil && executionID > 0 {
			if index.byExecutionID[executionID] == nil {
				index.byExecutionID[executionID] = task
			}
		}
	}
	return index
}

func taskForExecution(index *taskMetadataIndex, execution *pb.Execution) *model.Task {
	if index == nil || execution == nil {
		return nil
	}
	if task := index.byExecutionID[execution.GetId()]; task != nil {
		return task
	}
	if podName := executionPodName(execution); podName != "" {
		if task := index.byPodName[podName]; task != nil {
			return task
		}
	}
	for _, key := range []string{"task_name", "display_name"} {
		if value := execution.GetCustomProperties()[key]; value != nil {
			if task := index.byName[value.GetStringValue()]; task != nil {
				return task
			}
		}
	}
	return nil
}

func addUniqueTaskMapping(mapping map[string]*model.Task, duplicates map[string]bool, key string, task *model.Task) {
	if key == "" || duplicates[key] {
		return
	}
	if mapping[key] != nil {
		delete(mapping, key)
		duplicates[key] = true
		return
	}
	mapping[key] = task
}

func executionPodName(execution *pb.Execution) string {
	for _, key := range []string{"pod_name", "kfp_pod_name"} {
		if value := execution.GetCustomProperties()[key]; value != nil && value.GetStringValue() != "" {
			return value.GetStringValue()
		}
	}
	return ""
}

func enrichTaskPodNamesFromExecutions(tasks []*apiv2beta1.PipelineTaskDetail, executions []*apiv2beta1.RunExecution) {
	podNameByTaskID := make(map[string]string, len(executions))
	for _, execution := range executions {
		if execution.GetTaskId() != "" && execution.GetPodName() != "" {
			podNameByTaskID[execution.GetTaskId()] = execution.GetPodName()
		}
	}
	for _, task := range tasks {
		if task.GetPodName() == "" {
			task.PodName = podNameByTaskID[task.GetTaskId()]
		}
	}
}

func enrichTasksFromExecutions(tasks []*apiv2beta1.PipelineTaskDetail, executions []*apiv2beta1.RunExecution) {
	executionByTaskID := make(map[string]*apiv2beta1.RunExecution, len(executions))
	for _, execution := range executions {
		if execution.GetTaskId() != "" {
			executionByTaskID[execution.GetTaskId()] = execution
		}
	}
	for _, task := range tasks {
		execution := executionByTaskID[task.GetTaskId()]
		if execution == nil {
			continue
		}
		task.ExecutionId = execution.GetExecutionId()
		if task.GetPodName() == "" {
			task.PodName = execution.GetPodName()
		}
	}
}

func needsExecutionPodName(tasks []*model.Task) bool {
	for _, task := range tasks {
		if task != nil && task.PodName == "" && task.MLMDExecutionID != "" {
			return true
		}
	}
	return false
}

func addArtifactUsages(snapshot *runMetadataSnapshot, tasks []*model.Task) {
	seen := make(map[string]bool)
	for _, event := range snapshot.events {
		task := snapshot.taskByExec[event.GetExecutionId()]
		artifact := snapshot.artifacts[event.GetArtifactId()]
		if artifact == nil {
			continue
		}
		taskID := ""
		if task != nil {
			taskID = task.UUID
		}
		usage := &apiv2beta1.RunArtifactUsage{
			TaskId:      taskID,
			ExecutionId: event.GetExecutionId(),
			Key:         mlmdEventPath(event.GetPath()),
			EventType:   event.GetType().String(),
			RecordedAt:  timestampFromMillis(event.GetMillisecondsSinceEpoch()),
		}
		key := artifactUsageKey(artifact.ArtifactId, usage)
		if !seen[key] {
			artifact.Usages = append(artifact.Usages, usage)
			seen[key] = true
		}
	}

	// Persistence data is a fallback for old/incomplete MLMD event records.
	for _, task := range tasks {
		inputs, outputs, err := taskArtifactIDs(task)
		if err != nil {
			continue
		}
		executionID, _ := strconv.ParseInt(task.MLMDExecutionID, 10, 64)
		addFallback := func(refs map[int64][]string, eventType string) {
			for artifactID, keys := range refs {
				artifact := snapshot.artifacts[artifactID]
				if artifact == nil {
					continue
				}
				for _, keyName := range keys {
					if hasDirectionalUsage(artifact.Usages, task.UUID, keyName, eventType) {
						continue
					}
					usage := &apiv2beta1.RunArtifactUsage{TaskId: task.UUID, ExecutionId: executionID, Key: keyName, EventType: eventType}
					key := artifactUsageKey(artifactID, usage)
					if !seen[key] {
						artifact.Usages = append(artifact.Usages, usage)
						seen[key] = true
					}
				}
			}
		}
		addFallback(inputs, "INPUT")
		addFallback(outputs, "OUTPUT")
	}
	for _, artifact := range snapshot.artifacts {
		sort.Slice(artifact.Usages, func(i, j int) bool {
			left := artifact.Usages[i].TaskId + "|" + artifact.Usages[i].EventType + "|" + artifact.Usages[i].Key
			right := artifact.Usages[j].TaskId + "|" + artifact.Usages[j].EventType + "|" + artifact.Usages[j].Key
			return left < right
		})
	}
}

func taskArtifactIDs(task *model.Task) (map[int64][]string, map[int64][]string, error) {
	parse := func(raw model.LargeText) (map[int64][]string, error) {
		result := make(map[int64][]string)
		if raw == "" {
			return result, nil
		}
		refs := make(map[string]*apiv2beta1.ArtifactList)
		if err := json.Unmarshal([]byte(raw), &refs); err != nil {
			return nil, err
		}
		for key, list := range refs {
			for _, id := range list.GetArtifactIds() {
				if id > 0 {
					result[id] = append(result[id], key)
				}
			}
		}
		return result, nil
	}
	inputs, err := parse(task.MLMDInputs)
	if err != nil {
		return nil, nil, fmt.Errorf("task %s inputs: %w", task.UUID, err)
	}
	outputs, err := parse(task.MLMDOutputs)
	if err != nil {
		return nil, nil, fmt.Errorf("task %s outputs: %w", task.UUID, err)
	}
	return inputs, outputs, nil
}

func mlmdValuesToStruct(values map[string]*pb.Value) *structpb.Struct {
	if len(values) == 0 {
		return nil
	}
	fields := make(map[string]*structpb.Value, len(values))
	for key, value := range values {
		fields[key] = mlmdValueToStructValue(value)
	}
	return &structpb.Struct{Fields: fields}
}

func mlmdValueToStructValue(value *pb.Value) *structpb.Value {
	if value == nil {
		return structpb.NewNullValue()
	}
	switch typed := value.GetValue().(type) {
	case *pb.Value_IntValue:
		return structpb.NewNumberValue(float64(typed.IntValue))
	case *pb.Value_DoubleValue:
		return structpb.NewNumberValue(typed.DoubleValue)
	case *pb.Value_StringValue:
		return structpb.NewStringValue(typed.StringValue)
	case *pb.Value_BoolValue:
		return structpb.NewBoolValue(typed.BoolValue)
	case *pb.Value_StructValue:
		return structpb.NewStructValue(typed.StructValue)
	case *pb.Value_ProtoValue:
		encoded, err := protojson.Marshal(typed.ProtoValue)
		if err == nil {
			var decoded interface{}
			if err = json.Unmarshal(encoded, &decoded); err == nil {
				if converted, convertErr := structpb.NewValue(decoded); convertErr == nil {
					return converted
				}
			}
		}
		return structpb.NewStringValue(typed.ProtoValue.String())
	default:
		return structpb.NewNullValue()
	}
}

func timestampFromMillis(milliseconds int64) *timestamppb.Timestamp {
	if milliseconds <= 0 {
		return nil
	}
	return timestamppb.New(timeFromMillis(milliseconds))
}

func timeFromMillis(milliseconds int64) (resultTime time.Time) {
	return time.Unix(milliseconds/1000, (milliseconds%1000)*int64(time.Millisecond))
}

func mlmdEventPath(path *pb.Event_Path) string {
	if path == nil || len(path.GetSteps()) == 0 {
		return ""
	}
	var result strings.Builder
	for index, step := range path.GetSteps() {
		switch value := step.GetValue().(type) {
		case *pb.Event_Path_Step_Key:
			if result.Len() > 0 {
				result.WriteString(".")
			}
			result.WriteString(value.Key)
		case *pb.Event_Path_Step_Index:
			result.WriteString(fmt.Sprintf("[%d]", value.Index))
		default:
			if index > 0 {
				result.WriteString(".")
			}
		}
	}
	return result.String()
}

func mlmdEventDirection(eventType pb.Event_Type) string {
	switch eventType {
	case pb.Event_DECLARED_INPUT, pb.Event_INPUT, pb.Event_INTERNAL_INPUT:
		return "INPUT"
	case pb.Event_DECLARED_OUTPUT, pb.Event_OUTPUT, pb.Event_INTERNAL_OUTPUT, pb.Event_PENDING_OUTPUT:
		return "OUTPUT"
	default:
		return ""
	}
}

func hasDirectionalUsage(usages []*apiv2beta1.RunArtifactUsage, taskID, key, direction string) bool {
	for _, usage := range usages {
		if usage.TaskId == taskID && usage.Key == key && mlmdEventDirectionByName(usage.EventType) == direction {
			return true
		}
	}
	return false
}

func mlmdEventDirectionByName(eventType string) string {
	switch eventType {
	case "DECLARED_INPUT", "INPUT", "INTERNAL_INPUT":
		return "INPUT"
	case "DECLARED_OUTPUT", "OUTPUT", "INTERNAL_OUTPUT", "PENDING_OUTPUT":
		return "OUTPUT"
	default:
		return eventType
	}
}

func artifactUsageKey(artifactID int64, usage *apiv2beta1.RunArtifactUsage) string {
	return fmt.Sprintf("%d|%s|%d|%s|%s", artifactID, usage.TaskId, usage.ExecutionId, usage.EventType, usage.Key)
}

func sortedInt64Keys(values map[int64]bool) []int64 {
	result := make([]int64, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func sortedRunArtifacts(artifacts map[int64]*apiv2beta1.RunArtifact) []*apiv2beta1.RunArtifact {
	result := make([]*apiv2beta1.RunArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		result = append(result, artifact)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ArtifactId < result[j].ArtifactId })
	return result
}

func artifactsForExecution(snapshot *runMetadataSnapshot, executionID int64) (inputs, outputs []*apiv2beta1.RunArtifact) {
	if snapshot == nil {
		return nil, nil
	}
	for _, artifact := range sortedRunArtifacts(snapshot.artifacts) {
		isInput, isOutput := false, false
		for _, usage := range artifact.GetUsages() {
			if usage.GetExecutionId() != executionID {
				continue
			}
			switch mlmdEventDirectionByName(usage.GetEventType()) {
			case "INPUT":
				isInput = true
			case "OUTPUT":
				isOutput = true
			}
		}
		if isInput {
			inputs = append(inputs, artifact)
		}
		if isOutput {
			outputs = append(outputs, artifact)
		}
	}
	return inputs, outputs
}

func selectArtifacts(artifacts map[int64]*apiv2beta1.RunArtifact, ids map[int64][]string) []*apiv2beta1.RunArtifact {
	result := make([]*apiv2beta1.RunArtifact, 0, len(ids))
	for _, id := range sortedInt64Keys(func() map[int64]bool {
		set := make(map[int64]bool, len(ids))
		for artifactID := range ids {
			set[artifactID] = true
		}
		return set
	}()) {
		if artifact := artifacts[id]; artifact != nil {
			result = append(result, artifact)
		}
	}
	return result
}

func appendUniqueString(values []string, candidate string) []string {
	for _, value := range values {
		if value == candidate {
			return values
		}
	}
	return append(values, candidate)
}
