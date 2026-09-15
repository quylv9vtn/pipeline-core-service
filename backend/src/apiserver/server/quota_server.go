package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/apiserver/resource"
)

type QuotaServer struct{ resourceManager *resource.ResourceManager }

type quotaLimitRequest struct {
	ProjectID string `json:"project_id"`
	Limits    struct {
		CPU     int64 `json:"ai_pipeline_cpu"`
		Memory  int64 `json:"ai_pipeline_memory"`
		GPU2080 int64 `json:"ai_pipeline_gpu_rtx2080"`
		GPUH200 int64 `json:"ai_pipeline_gpu_h200"`
	} `json:"limits"`
}

type quotaUsageResponse struct {
	ProjectID string           `json:"project_id"`
	Used      map[string]int64 `json:"used"`
}

type quotaLimitsResponse struct {
	ProjectID string           `json:"project_id"`
	Limits    map[string]int64 `json:"limits"`
}

type quotaBulkRequest struct {
	ProjectIDs []string `json:"project_ids"`
}
type quotaBulkResponse struct {
	Results []quotaUsageResponse `json:"results"`
}

func NewQuotaServer(resourceManager *resource.ResourceManager) *QuotaServer {
	return &QuotaServer{resourceManager: resourceManager}
}

func (s *QuotaServer) PutLimits(w http.ResponseWriter, r *http.Request) {
	var request quotaLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeQuotaError(w, http.StatusBadRequest, "invalid JSON request")
		return
	}
	if _, err := uuid.Parse(strings.TrimSpace(request.ProjectID)); err != nil || request.Limits.CPU < 0 || request.Limits.Memory < 0 || request.Limits.GPU2080 < 0 || request.Limits.GPUH200 < 0 {
		writeQuotaError(w, http.StatusBadRequest, "project_id must be a UUID and limits must be non-negative")
		return
	}
	limits := model.QuotaLimit{ProjectID: request.ProjectID, CPUMilliLimit: request.Limits.CPU, MemoryMiBLimit: request.Limits.Memory, GPU2080Limit: request.Limits.GPU2080, GPUH200Limit: request.Limits.GPUH200}
	if err := s.resourceManager.SetQuotaLimit(request.ProjectID, limits); err != nil {
		writeQuotaError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *QuotaServer) GetUsage(w http.ResponseWriter, r *http.Request) {
	projectID := r.URL.Query().Get("project_id")
	if _, err := uuid.Parse(projectID); err != nil {
		writeQuotaError(w, http.StatusBadRequest, "project_id must be a UUID")
		return
	}
	usage, err := s.resourceManager.GetQuotaUsage(projectID)
	if err != nil {
		writeQuotaError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeQuotaJSON(w, quotaUsageResponse{ProjectID: projectID, Used: quotaUsed(usage)})
}

func (s *QuotaServer) GetLimits(w http.ResponseWriter, r *http.Request) {
	projectID := r.URL.Query().Get("project_id")
	if _, err := uuid.Parse(projectID); err != nil {
		writeQuotaError(w, http.StatusBadRequest, "project_id must be a UUID")
		return
	}
	limits, err := s.resourceManager.GetQuotaLimits(projectID)
	if err != nil {
		writeQuotaError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeQuotaJSON(w, quotaLimitsResponse{ProjectID: projectID, Limits: quotaLimits(limits)})
}

func (s *QuotaServer) GetUsageBulk(w http.ResponseWriter, r *http.Request) {
	var request quotaBulkRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ProjectIDs) > 100 {
		writeQuotaError(w, http.StatusBadRequest, "project_ids must contain at most 100 UUIDs")
		return
	}
	for _, projectID := range request.ProjectIDs {
		if _, err := uuid.Parse(projectID); err != nil {
			writeQuotaError(w, http.StatusBadRequest, "project_ids must contain UUIDs")
			return
		}
	}
	usage, err := s.resourceManager.GetQuotaUsageBulk(request.ProjectIDs)
	if err != nil {
		writeQuotaError(w, http.StatusInternalServerError, err.Error())
		return
	}
	response := quotaBulkResponse{Results: make([]quotaUsageResponse, 0, len(request.ProjectIDs))}
	for _, projectID := range request.ProjectIDs {
		response.Results = append(response.Results, quotaUsageResponse{ProjectID: projectID, Used: quotaUsed(usage[projectID])})
	}
	writeQuotaJSON(w, response)
}

// quotaUsed maps model fields to the canonical API quota-unit keys.
// CPU is in millicores (e.g. 2000 = 2 cores); memory is in MiB (e.g. 1024 = 1 GiB).
func quotaUsed(usage model.QuotaLimit) map[string]int64 {
	return map[string]int64{"ai_pipeline_cpu": usage.CPUMilliUsed, "ai_pipeline_memory": usage.MemoryMiBUsed, "ai_pipeline_gpu_rtx2080": usage.GPU2080Used, "ai_pipeline_gpu_h200": usage.GPUH200Used}
}

func quotaLimits(limits model.QuotaLimit) map[string]int64 {
	return map[string]int64{"ai_pipeline_cpu": limits.CPUMilliLimit, "ai_pipeline_memory": limits.MemoryMiBLimit, "ai_pipeline_gpu_rtx2080": limits.GPU2080Limit, "ai_pipeline_gpu_h200": limits.GPUH200Limit}
}
func writeQuotaJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func writeQuotaError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
