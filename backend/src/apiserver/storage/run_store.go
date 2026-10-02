// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/list"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/json"
)

var runColumns = []string{
	"UUID",
	"ExperimentUUID",
	"DisplayName",
	"Name",
	"StorageState",
	"Namespace",
	"ServiceAccount",
	"Description",
	"CreatedAtInSec",
	"ScheduledAtInSec",
	"FinishedAtInSec",
	"PipelineId",
	"PipelineVersionId",
	"PipelineName",
	"PipelineSpecManifest",
	"WorkflowSpecManifest",
	"RuntimeParameters",
	"PipelineRoot",
	"PipelineRuntimeManifest",
	"WorkflowRuntimeManifest",
	"JobUUID",
	"State",
	"StateHistory",
	"PipelineContextId",
	"PipelineRunContextId",
}

type RunStoreInterface interface {
	// Creates a run entry. Does not create children tasks.
	CreateRun(run *model.Run) (*model.Run, error)

	// Fetches a run.
	GetRun(runId string) (*model.Run, error)

	// Fetches runs with specified options. Joins with children tasks.
	ListRuns(filterContext *model.FilterContext, opts *list.Options) ([]*model.Run, int, string, error)

	// Updates a run.
	// Note: only state, runtime manifest can be updated. Does not update dependent tasks.
	UpdateRun(run *model.Run) (err error)

	// Archives a run.
	ArchiveRun(runId string) error

	// Un-archives a run.
	UnarchiveRun(runId string) error

	// Deletes a run.
	DeleteRun(runId string) error

	// Terminates a run.
	TerminateRun(runId string) error

	// ListRunIDsByPipelineVersion returns the IDs of all runs that reference
	// the given pipeline version.
	ListRunIDsByPipelineVersion(pipelineVersionId string) ([]string, error)

	// ListRunIDsByJob returns the IDs of all runs that were spawned by the
	// given recurring run (job).
	ListRunIDsByJob(jobId string) ([]string, error)

	// CountRunsByExperiment returns the number of runs associated with the given experiment.
	CountRunsByExperiment(experimentId string) (int, error)
}

type RunStore struct {
	db   *DB
	time util.TimeInterface
}

// Runs two SQL queries in a transaction to return a list of matching runs, as well as their
// total_size. The total_size does not reflect the page size, but it does reflect the number of runs
// matching the supplied filters and resource references.
func (s *RunStore) ListRuns(
	filterContext *model.FilterContext, opts *list.Options,
) ([]*model.Run, int, string, error) {
	errorF := func(err error) ([]*model.Run, int, string, error) {
		return nil, 0, "", util.NewInternalServerError(err, "Failed to list runs: %v", err)
	}

	rowsSql, rowsArgs, err := s.buildSelectRunsQuery(false, opts, filterContext)
	if err != nil {
		return errorF(err)
	}

	sizeSql, sizeArgs, err := s.buildSelectRunsQuery(true, opts, filterContext)
	if err != nil {
		return errorF(err)
	}

	// Use a transaction to make sure we're returning the total_size of the same rows queried
	tx, err := s.db.Begin()
	if err != nil {
		glog.Error("Failed to start transaction to list runs")
		return errorF(err)
	}

	rows, err := tx.Query(rowsSql, rowsArgs...)
	if err != nil {
		return errorF(err)
	}
	if err := rows.Err(); err != nil {
		return errorF(err)
	}
	runs, err := s.scanRowsToRuns(rows)
	if err != nil {
		tx.Rollback()
		return errorF(err)
	}
	defer rows.Close()

	sizeRow, err := tx.Query(sizeSql, sizeArgs...)
	if err != nil {
		tx.Rollback()
		return errorF(err)
	}
	if err := sizeRow.Err(); err != nil {
		tx.Rollback()
		return errorF(err)
	}
	total_size, err := list.ScanRowToTotalSize(sizeRow)
	if err != nil {
		tx.Rollback()
		return errorF(err)
	}
	defer sizeRow.Close()

	err = tx.Commit()
	if err != nil {
		glog.Error("Failed to commit transaction to list runs")
		return errorF(err)
	}

	if len(runs) <= opts.PageSize {
		return runs, total_size, "", nil
	}

	npt, err := opts.NextPageToken(runs[opts.PageSize])
	return runs[:opts.PageSize], total_size, npt, err
}

func (s *RunStore) buildSelectRunsQuery(selectCount bool, opts *list.Options,
	filterContext *model.FilterContext,
) (string, []interface{}, error) {
	var filteredSelectBuilder sq.SelectBuilder
	var err error

	refKey := filterContext.ReferenceKey
	if refKey != nil && refKey.Type == model.ExperimentResourceType && (refKey.ID != "" || common.IsMultiUserMode()) {
		// for performance reasons need to special treat experiment ID filter on runs
		// currently only the run table have experiment UUID column
		filteredSelectBuilder, err = list.FilterOnExperiment("run_details", runColumns,
			selectCount, refKey.ID)
	} else if refKey != nil && refKey.Type == model.NamespaceResourceType && (refKey.ID != "" || common.IsMultiUserMode()) {
		filteredSelectBuilder, err = list.FilterOnNamespace("run_details", runColumns,
			selectCount, refKey.ID)
	} else if refKey != nil && refKey.Type == model.JobResourceType && refKey.ID != "" {
		filteredSelectBuilder = sq.Select(runColumns...).From("run_details").Where(sq.Eq{"JobUUID": refKey.ID})
		if selectCount {
			filteredSelectBuilder = sq.Select("count(*)").From("run_details").Where(sq.Eq{"JobUUID": refKey.ID})
		}
	} else if refKey == nil || (refKey.ID == "" && !common.IsMultiUserMode()) {
		filteredSelectBuilder = sq.Select(runColumns...).From("run_details")
		if selectCount {
			filteredSelectBuilder = sq.Select("count(*)").From("run_details")
		}
	} else {
		return "", nil, util.NewInvalidInputError("Unsupported V2 run reference filter: %s", refKey.Type)
	}
	if err != nil {
		return "", nil, util.NewInternalServerError(err, "Failed to list runs: %v", err)
	}

	sqlBuilder := opts.AddFilterToSelect(filteredSelectBuilder)

	// If we're not just counting, then also add select columns and perform a left join
	// to get resource reference information. Also add pagination.
	if !selectCount {
		sqlBuilder = opts.AddPaginationToSelect(sqlBuilder)
		sqlBuilder = s.addTasks(sqlBuilder)
		sqlBuilder = opts.AddSortingToSelect(sqlBuilder)
	}
	sql, args, err := sqlBuilder.ToSql()
	if err != nil {
		return "", nil, util.NewInternalServerError(err, "Failed to list runs: %v", err)
	}
	return sql, args, err
}

// GetRun Get the run manifest from Workflow CRD.
func (s *RunStore) GetRun(runId string) (*model.Run, error) {
	sql, args, err := s.addTasks(
		sq.Select(runColumns...).
			From("run_details").
			Where(sq.Eq{"UUID": runId}).
			Limit(1)).
		ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to get run: %v", err.Error())
	}
	r, err := s.db.Query(sql, args...)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to get run: %v", err.Error())
	}
	defer r.Close()
	runs, err := s.scanRowsToRuns(r)

	if err != nil || len(runs) > 1 {
		return nil, util.NewInternalServerError(err, "Failed to get run: %v", err.Error())
	}
	if len(runs) == 0 {
		return nil, util.NewResourceNotFoundError("Run", fmt.Sprint(runId))
	}
	if string(runs[0].WorkflowRuntimeManifest) == "" && string(runs[0].WorkflowSpecManifest) != "" {
		// This can only happen when workflow reporting is failed.
		return nil, util.NewResourceNotFoundError("Failed to get run: %s", runId)
	}
	return runs[0], nil
}

// Applies a func f to every string in a given string slice.
func apply(f func(string) string, vs []string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func (s *RunStore) addTasks(filteredSelectBuilder sq.SelectBuilder) sq.SelectBuilder {
	rdGroupBy := apply(func(column string) string { return "rd." + column }, runColumns)
	tasksConcatQuery := s.db.Concat([]string{`"["`, s.db.GroupConcat("tasks.Payload", ","), `"]"`}, "")
	columnsAfterJoiningTasks := append(
		apply(func(column string) string { return "rd." + column }, runColumns),
		tasksConcatQuery+" AS taskDetails")
	return sq.
		Select(columnsAfterJoiningTasks...).
		FromSelect(filteredSelectBuilder, "rd").
		// Only expose the task columns required by this query. In particular,
		// hiding tasks.UUID keeps the Run pagination key UUID unambiguous when
		// the outer query reapplies sorting after the join.
		LeftJoin("(SELECT RunUUID, Payload FROM tasks) AS tasks ON rd.UUID=tasks.RunUUID").
		GroupBy(rdGroupBy...)
}

func (s *RunStore) scanRowsToRuns(rows *sql.Rows) ([]*model.Run, error) {
	var runs []*model.Run
	for rows.Next() {
		var uuid, experimentUUID, displayName, name, storageState, namespace, serviceAccount, description, pipelineId,
			pipelineName, pipelineSpecManifest, workflowSpecManifest, pipelineRuntimeManifest,
			workflowRuntimeManifest string
		var createdAtInSec, scheduledAtInSec, finishedAtInSec, pipelineContextId, pipelineRunContextId sql.NullInt64
		var tasksInString, runtimeParameters, pipelineRoot, jobId, state, stateHistory, pipelineVersionId sql.NullString
		err := rows.Scan(
			&uuid,
			&experimentUUID,
			&displayName,
			&name,
			&storageState,
			&namespace,
			&serviceAccount,
			&description,
			&createdAtInSec,
			&scheduledAtInSec,
			&finishedAtInSec,
			&pipelineId,
			&pipelineVersionId,
			&pipelineName,
			&pipelineSpecManifest,
			&workflowSpecManifest,
			&runtimeParameters,
			&pipelineRoot,
			&pipelineRuntimeManifest,
			&workflowRuntimeManifest,
			&jobId,
			&state,
			&stateHistory,
			&pipelineContextId,
			&pipelineRunContextId,
			&tasksInString,
		)
		if err != nil {
			glog.Errorf("Failed to scan row into a run: %v", err)
			return runs, nil
		}
		tasks, err := parseTaskDetails(tasksInString)
		if err != nil {
			return nil, util.NewInternalServerError(err, "Failed to parse task details")
		}
		jId := jobId.String
		pvId := pipelineVersionId.String
		runtimeConfig := parseRuntimeConfig(runtimeParameters, pipelineRoot)
		var stateHistoryNew []*model.RuntimeStatus
		if stateHistory.Valid {
			json.Unmarshal([]byte(stateHistory.String), &stateHistoryNew)
		}
		run := &model.Run{
			UUID:           uuid,
			ExperimentId:   experimentUUID,
			DisplayName:    displayName,
			K8SName:        name,
			StorageState:   model.StorageState(storageState),
			Namespace:      namespace,
			ServiceAccount: serviceAccount,
			Description:    string(description),
			RecurringRunId: jId,
			RunDetails: model.RunDetails{
				CreatedAtInSec:          createdAtInSec.Int64,
				ScheduledAtInSec:        scheduledAtInSec.Int64,
				FinishedAtInSec:         finishedAtInSec.Int64,
				State:                   model.RuntimeState(state.String),
				PipelineRuntimeManifest: model.LargeText(pipelineRuntimeManifest),
				WorkflowRuntimeManifest: model.LargeText(workflowRuntimeManifest),
				PipelineContextId:       pipelineContextId.Int64,
				PipelineRunContextId:    pipelineRunContextId.Int64,
				TaskDetails:             tasks,
				StateHistory:            stateHistoryNew,
			},
			PipelineSpec: model.PipelineSpec{
				PipelineId:           pipelineId,
				PipelineVersionId:    pvId,
				PipelineName:         pipelineName,
				PipelineSpecManifest: model.LargeText(pipelineSpecManifest),
				WorkflowSpecManifest: model.LargeText(workflowSpecManifest),
				RuntimeConfig:        runtimeConfig,
			},
		}
		run = run.ToV2()
		runs = append(runs, run)
	}
	return runs, nil
}

func parseRuntimeConfig(runtimeParameters sql.NullString, pipelineRoot sql.NullString) model.RuntimeConfig {
	var runtimeParametersString, pipelineRootString string
	if runtimeParameters.Valid {
		runtimeParametersString = runtimeParameters.String
	}
	if pipelineRoot.Valid {
		pipelineRootString = pipelineRoot.String
	}
	return model.RuntimeConfig{Parameters: model.LargeText(runtimeParametersString), PipelineRoot: model.LargeText(pipelineRootString)}
}

func parseTaskDetails(tasksInString sql.NullString) ([]*model.Task, error) {
	if !tasksInString.Valid {
		return nil, nil
	}
	var taskDetails []*model.Task
	if err := json.Unmarshal([]byte(tasksInString.String), &taskDetails); err != nil {
		return nil, util.Wrapf(err, "Failed to parse task details '%s'", tasksInString.String)
	}
	return taskDetails, nil
}

func (s *RunStore) CreateRun(r *model.Run) (*model.Run, error) {
	r = r.ToV2()
	if r.StorageState == "" || r.StorageState == model.StorageStateUnspecified || r.StorageState == model.StorageStateUnspecifiedV1 {
		r.StorageState = model.StorageStateAvailable
	}

	if !r.StorageState.IsValid() {
		return nil, util.NewInvalidInputError("Invalid value for StorageState field: %q", r.StorageState)
	}

	if len(r.RunDetails.StateHistory) == 0 || r.RunDetails.StateHistory[len(r.RunDetails.StateHistory)-1].State != r.RunDetails.State {
		r.RunDetails.StateHistory = append(r.RunDetails.StateHistory, &model.RuntimeStatus{
			UpdateTimeInSec: s.time.Now().Unix(),
			State:           r.RunDetails.State,
		})
	}

	stateHistoryString := ""
	if history, err := json.Marshal(r.RunDetails.StateHistory); err == nil {
		stateHistoryString = string(history)
	} else {
		return nil, util.NewInternalServerError(err, "Failed to marshal state history in a new run")
	}
	runSql, runArgs, err := sq.
		Insert("run_details").
		SetMap(sq.Eq{
			"UUID":                    r.UUID,
			"ExperimentUUID":          r.ExperimentId,
			"DisplayName":             r.DisplayName,
			"Name":                    r.K8SName,
			"StorageState":            r.StorageState.ToString(),
			"Namespace":               r.Namespace,
			"ServiceAccount":          r.ServiceAccount,
			"Description":             r.Description,
			"CreatedAtInSec":          r.RunDetails.CreatedAtInSec,
			"ScheduledAtInSec":        r.RunDetails.ScheduledAtInSec,
			"FinishedAtInSec":         r.RunDetails.FinishedAtInSec,
			"WorkflowRuntimeManifest": r.RunDetails.WorkflowRuntimeManifest,
			"PipelineRuntimeManifest": r.RunDetails.PipelineRuntimeManifest,
			"PipelineId":              r.PipelineSpec.PipelineId,
			"PipelineName":            r.PipelineSpec.PipelineName,
			"PipelineSpecManifest":    r.PipelineSpec.PipelineSpecManifest,
			"WorkflowSpecManifest":    r.PipelineSpec.WorkflowSpecManifest,
			"RuntimeParameters":       r.PipelineSpec.RuntimeConfig.Parameters,
			"PipelineRoot":            r.PipelineSpec.RuntimeConfig.PipelineRoot,
			"PipelineVersionId":       r.PipelineSpec.PipelineVersionId,
			"JobUUID":                 r.RecurringRunId,
			"State":                   r.RunDetails.State.ToString(),
			"StateHistory":            stateHistoryString,
		}).ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create query to store run to run table: '%v/%v",
			r.Namespace, r.DisplayName)
	}

	// Use a transaction to make sure both run and its resource references are stored.
	tx, err := s.db.Begin()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create a new transaction to create run")
	}

	_, err = tx.Exec(runSql, runArgs...)
	if err != nil {
		tx.Rollback()
		return nil, util.NewInternalServerError(err, "Failed to store run %v to table", r.DisplayName)
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return nil, util.NewInternalServerError(err, "Failed to store run %v and its resource references to table", r.DisplayName)
	}
	return r, nil
}

func (s *RunStore) UpdateRun(run *model.Run) error {
	tx, err := s.db.Begin()
	if err != nil {
		return util.NewInternalServerError(err, "transaction creation failed")
	}
	if len(run.RunDetails.StateHistory) == 0 || run.RunDetails.StateHistory[len(run.RunDetails.StateHistory)-1].State != run.RunDetails.State {
		run.RunDetails.StateHistory = append(run.RunDetails.StateHistory, &model.RuntimeStatus{
			UpdateTimeInSec: s.time.Now().Unix(),
			State:           run.RunDetails.State,
		})
	}
	stateHistoryString := ""
	if historyString, err := json.Marshal(run.RunDetails.StateHistory); err == nil {
		stateHistoryString = string(historyString)
	}
	sql, args, err := sq.
		Update("run_details").
		SetMap(sq.Eq{
			"State":                   run.State.ToString(),
			"StateHistory":            stateHistoryString,
			"FinishedAtInSec":         run.FinishedAtInSec,
			"WorkflowRuntimeManifest": run.WorkflowRuntimeManifest,
		}).
		Where(sq.Eq{"UUID": run.UUID}).
		ToSql()
	if err != nil {
		tx.Rollback()
		return util.NewInternalServerError(err,
			"Failed to create query to update run %s", run.UUID)
	}
	result, err := tx.Exec(sql, args...)
	if err != nil {
		tx.Rollback()
		return util.NewInternalServerError(err,
			"Failed to update run %s", run.UUID)
	}
	r, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return util.NewInternalServerError(err,
			"Failed to update run %s", run.UUID)
	}
	if r > 1 {
		tx.Rollback()
		return util.NewInternalServerError(errors.New("Failed to update run"), "Failed to update run %s. More than 1 rows affected", run.UUID)
	} else if r == 0 {
		tx.Rollback()
		return util.Wrap(util.NewResourceNotFoundError("Run", run.UUID), "Failed to update run")
	}

	if err := tx.Commit(); err != nil {
		return util.NewInternalServerError(err, "failed to commit transaction for run %s", run.UUID)
	}
	return nil
}

func (s *RunStore) ArchiveRun(runId string) error {
	sql, args, err := sq.
		Update("run_details").
		SetMap(sq.Eq{
			"StorageState": model.StorageStateArchived.ToString(),
		}).
		Where(sq.Eq{"UUID": runId}).
		ToSql()
	if err != nil {
		return util.NewInternalServerError(err,
			"Failed to create query to archive run %s. error: '%v'", runId, err.Error())
	}

	_, err = s.db.Exec(sql, args...)
	if err != nil {
		return util.NewInternalServerError(err,
			"Failed to archive run %s. error: '%v'", runId, err.Error())
	}

	return nil
}

func (s *RunStore) UnarchiveRun(runId string) error {
	sql, args, err := sq.
		Update("run_details").
		SetMap(sq.Eq{
			"StorageState": model.StorageStateAvailable.ToString(),
		}).
		Where(sq.Eq{"UUID": runId}).
		ToSql()
	if err != nil {
		return util.NewInternalServerError(err,
			"Failed to create query to unarchive run %s. error: '%v'", runId, err.Error())
	}

	_, err = s.db.Exec(sql, args...)
	if err != nil {
		return util.NewInternalServerError(err,
			"Failed to unarchive run %s. error: '%v'", runId, err.Error())
	}

	return nil
}

func (s *RunStore) DeleteRun(id string) error {
	runSql, runArgs, err := sq.Delete("run_details").Where(sq.Eq{"UUID": id}).ToSql()
	if err != nil {
		return util.NewInternalServerError(err,
			"Failed to create query to delete run: %s", id)
	}
	// Use a transaction to make sure both run and its resource references are stored.
	tx, err := s.db.Begin()
	if err != nil {
		return util.NewInternalServerError(err, "Failed to create a new transaction to delete run")
	}
	_, err = tx.Exec(runSql, runArgs...)
	if err != nil {
		tx.Rollback()
		return util.NewInternalServerError(err, "Failed to delete run %s from table", id)
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return util.NewInternalServerError(err, "Failed to delete run %v and its resource references from table", id)
	}
	return nil
}

// ListRunIDsByPipelineVersion returns the UUIDs of all runs that reference the
// given pipeline version.
func (s *RunStore) ListRunIDsByPipelineVersion(pipelineVersionId string) ([]string, error) {
	sql, args, err := sq.Select("UUID").From("run_details").
		Where(sq.Eq{"PipelineVersionId": pipelineVersionId}).ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to build query to list run IDs by pipeline version %s", pipelineVersionId)
	}
	rows, err := s.db.Query(sql, args...)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to list run IDs by pipeline version %s", pipelineVersionId)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, util.NewInternalServerError(scanErr, "Failed to scan run ID for pipeline version %s", pipelineVersionId)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ListRunIDsByJob returns the UUIDs of all runs spawned by the given recurring
// run (job).
func (s *RunStore) ListRunIDsByJob(jobId string) ([]string, error) {
	sql, args, err := sq.Select("UUID").From("run_details").
		Where(sq.Eq{"JobUUID": jobId}).ToSql()
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to build query to list run IDs by job %s", jobId)
	}
	rows, err := s.db.Query(sql, args...)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to list run IDs by job %s", jobId)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, util.NewInternalServerError(scanErr, "Failed to scan run ID for job %s", jobId)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// CountRunsByExperiment returns the number of runs associated with the given experiment.
func (s *RunStore) CountRunsByExperiment(experimentId string) (int, error) {
	sql, args, err := sq.Select("count(*)").From("run_details").
		Where(sq.Eq{"ExperimentUUID": experimentId}).ToSql()
	if err != nil {
		return 0, util.NewInternalServerError(err, "Failed to build query to count runs for experiment %s", experimentId)
	}
	var count int
	if err := s.db.QueryRow(sql, args...).Scan(&count); err != nil {
		return 0, util.NewInternalServerError(err, "Failed to count runs for experiment %s", experimentId)
	}
	return count, nil
}

// Returns a new RunStore.
func NewRunStore(db *DB, time util.TimeInterface) *RunStore {
	return &RunStore{
		db:   db,
		time: time,
	}
}

func (s *RunStore) TerminateRun(runId string) error {
	// TODO(gkcalat): append CANCELLING to StateHistory
	result, err := s.db.Exec(`
		UPDATE run_details
		SET State = ?
		WHERE UUID = ? AND (State = ? OR State = ? OR State = ? OR State = ?)`,
		model.RuntimeStateCancelling.ToString(),
		runId,
		model.RuntimeStatePaused.ToString(),
		model.RuntimeStatePending.ToString(),
		model.RuntimeStateRunning.ToString(),
		model.RuntimeStateUnspecified.ToString(),
	)
	if err != nil {
		return util.NewInternalServerError(err,
			"Failed to terminate a run %s. Error: '%v'", runId, err.Error())
	}

	if r, _ := result.RowsAffected(); r != 1 {
		return util.NewInvalidInputError("Failed to terminate a run %s. Row not found", runId)
	}
	return nil
}
