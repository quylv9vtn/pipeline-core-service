package storage

import (
	"database/sql"
	"encoding/json"
	"math"

	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
)

type QuotaStoreInterface interface {
	SetLimit(projectID string, limits model.QuotaLimit) error
	GetUsage(projectID string) (model.QuotaLimit, error)
	GetUsageBulk(projectIDs []string) (map[string]model.QuotaLimit, error)
	Reserve(projectID, runID string, allocation model.QuotaAllocation) error
	Release(runID string) error
}

type QuotaStore struct{ db *DB }

func NewQuotaStore(db *DB) *QuotaStore { return &QuotaStore{db: db} }

func (s *QuotaStore) SetLimit(projectID string, limits model.QuotaLimit) error {
	_, err := s.db.Exec(`
		INSERT INTO quota_limits (ProjectID, CPULimit, MemoryLimitGiB, GPU2080Limit, GPUH200Limit)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(ProjectID) DO UPDATE SET CPULimit=excluded.CPULimit,
		MemoryLimitGiB=excluded.MemoryLimitGiB, GPU2080Limit=excluded.GPU2080Limit,
		GPUH200Limit=excluded.GPUH200Limit`, projectID, limits.CPULimit, limits.MemoryLimitGiB, limits.GPU2080Limit, limits.GPUH200Limit)
	if err != nil {
		// MySQL uses a different upsert syntax; retry through the dialect helper.
		query := `INSERT INTO quota_limits (ProjectID, CPULimit, MemoryLimitGiB, GPU2080Limit, GPUH200Limit) VALUES (?, ?, ?, ?, ?)`
		query = s.db.Upsert(query, "ProjectID", true, "CPULimit", "MemoryLimitGiB", "GPU2080Limit", "GPUH200Limit")
		_, err = s.db.Exec(query, projectID, limits.CPULimit, limits.MemoryLimitGiB, limits.GPU2080Limit, limits.GPUH200Limit)
	}
	if err != nil {
		return util.NewInternalServerError(err, "failed to store quota limit")
	}
	return nil
}

func (s *QuotaStore) GetUsage(projectID string) (model.QuotaLimit, error) {
	var q model.QuotaLimit
	err := s.db.QueryRow(`SELECT ProjectID, CPULimit, MemoryLimitGiB, GPU2080Limit, GPUH200Limit, CPUUsed, MemoryUsedGiB, GPU2080Used, GPUH200Used FROM quota_limits WHERE ProjectID = ?`, projectID).
		Scan(&q.ProjectID, &q.CPULimit, &q.MemoryLimitGiB, &q.GPU2080Limit, &q.GPUH200Limit, &q.CPUUsed, &q.MemoryUsedGiB, &q.GPU2080Used, &q.GPUH200Used)
	if err != nil {
		if err == sql.ErrNoRows {
			q.ProjectID = projectID
			return q, nil
		}
		return q, util.NewInternalServerError(err, "failed to read quota usage")
	}
	return q, nil
}

func (s *QuotaStore) GetUsageBulk(projectIDs []string) (map[string]model.QuotaLimit, error) {
	result := make(map[string]model.QuotaLimit, len(projectIDs))
	for _, projectID := range projectIDs {
		usage, err := s.GetUsage(projectID)
		if err != nil {
			return nil, err
		}
		result[projectID] = usage
	}
	return result, nil
}

func (s *QuotaStore) Reserve(projectID, runID string, allocation model.QuotaAllocation) error {
	tx, err := s.db.Begin()
	if err != nil {
		return util.NewInternalServerError(err, "failed to begin quota reservation")
	}
	defer tx.Rollback()

	var q model.QuotaLimit
	row := tx.QueryRow(s.db.SelectForUpdate(`SELECT ProjectID, CPULimit, MemoryLimitGiB, GPU2080Limit, GPUH200Limit, CPUUsed, MemoryUsedGiB, GPU2080Used, GPUH200Used FROM quota_limits WHERE ProjectID = ?`), projectID)
	err = row.Scan(&q.ProjectID, &q.CPULimit, &q.MemoryLimitGiB, &q.GPU2080Limit, &q.GPUH200Limit, &q.CPUUsed, &q.MemoryUsedGiB, &q.GPU2080Used, &q.GPUH200Used)
	if err != nil && err != sql.ErrNoRows {
		return util.NewInternalServerError(err, "failed to lock quota")
	}
	if err != nil {
		// No configured limit means quota enforcement is not enabled for this project.
		return tx.Commit()
	}
	if exceeded(q, allocation) {
		details, _ := json.Marshal(quotaDetails(q, allocation))
		return util.NewResourceExhaustedError(nil, `{"error":"quota_exceeded","message":"Insufficient quota","details":%s}`, details)
	}
	_, err = tx.Exec(`UPDATE quota_limits SET CPUUsed=CPUUsed+?, MemoryUsedGiB=MemoryUsedGiB+?, GPU2080Used=GPU2080Used+?, GPUH200Used=GPUH200Used+? WHERE ProjectID=?`, allocation.CPU, allocation.MemoryGiB, allocation.GPU2080, allocation.GPUH200, projectID)
	if err != nil {
		return util.NewInternalServerError(err, "failed to update quota usage")
	}
	_, err = tx.Exec(`INSERT INTO quota_allocations (RunUUID, ProjectID, CPU, MemoryGiB, GPU2080, GPUH200) VALUES (?, ?, ?, ?, ?, ?)`, runID, projectID, allocation.CPU, allocation.MemoryGiB, allocation.GPU2080, allocation.GPUH200)
	if err != nil {
		return util.NewInternalServerError(err, "failed to store quota allocation")
	}
	if err := tx.Commit(); err != nil {
		return util.NewInternalServerError(err, "failed to commit quota reservation")
	}
	return nil
}

func (s *QuotaStore) Release(runID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return util.NewInternalServerError(err, "failed to begin quota release")
	}
	defer tx.Rollback()
	var a model.QuotaAllocation
	err = tx.QueryRow(s.db.SelectForUpdate(`SELECT RunUUID, ProjectID, CPU, MemoryGiB, GPU2080, GPUH200 FROM quota_allocations WHERE RunUUID = ?`), runID).
		Scan(&a.RunID, &a.ProjectID, &a.CPU, &a.MemoryGiB, &a.GPU2080, &a.GPUH200)
	if err != nil && err == sql.ErrNoRows {
		return tx.Commit()
	}
	if err != nil {
		return util.NewInternalServerError(err, "failed to lock quota allocation")
	}
	_, err = tx.Exec(`UPDATE quota_limits SET CPUUsed=MAX(0, CPUUsed-?), MemoryUsedGiB=MAX(0, MemoryUsedGiB-?), GPU2080Used=MAX(0, GPU2080Used-?), GPUH200Used=MAX(0, GPUH200Used-?) WHERE ProjectID=?`, a.CPU, a.MemoryGiB, a.GPU2080, a.GPUH200, a.ProjectID)
	if err != nil {
		return util.NewInternalServerError(err, "failed to release quota usage")
	}
	if _, err = tx.Exec(`DELETE FROM quota_allocations WHERE RunUUID=?`, runID); err != nil {
		return util.NewInternalServerError(err, "failed to delete quota allocation")
	}
	if err = tx.Commit(); err != nil {
		return util.NewInternalServerError(err, "failed to commit quota release")
	}
	return nil
}

func QuantityToCeilCPU(value string) int64 {
	q, err := k8sresource.ParseQuantity(value)
	if err != nil {
		return 0
	}
	return int64(math.Ceil(q.AsApproximateFloat64()))
}
func QuantityToCeilGiB(value string) int64 {
	q, err := k8sresource.ParseQuantity(value)
	if err != nil {
		return 0
	}
	return int64(math.Ceil(q.AsApproximateFloat64() / (1024 * 1024 * 1024)))
}

func exceeded(q model.QuotaLimit, a model.QuotaAllocation) bool {
	return q.CPUUsed+a.CPU > q.CPULimit || q.MemoryUsedGiB+a.MemoryGiB > q.MemoryLimitGiB || q.GPU2080Used+a.GPU2080 > q.GPU2080Limit || q.GPUH200Used+a.GPUH200 > q.GPUH200Limit
}
func quotaDetails(q model.QuotaLimit, a model.QuotaAllocation) []model.QuotaExceededDetail {
	details := []model.QuotaExceededDetail{}
	add := func(unit string, limit, used, requested int64) {
		if used+requested > limit {
			details = append(details, model.QuotaExceededDetail{QuotaUnit: unit, Limit: limit, Used: used, Requested: requested})
		}
	}
	add("ai_pipeline_cpu", q.CPULimit, q.CPUUsed, a.CPU)
	add("ai_pipeline_memory", q.MemoryLimitGiB, q.MemoryUsedGiB, a.MemoryGiB)
	add("ai_pipeline_gpu_rtx2080", q.GPU2080Limit, q.GPU2080Used, a.GPU2080)
	add("ai_pipeline_gpu_h200", q.GPUH200Limit, q.GPUH200Used, a.GPUH200)
	return details
}
