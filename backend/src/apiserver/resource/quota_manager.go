package resource

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/apiserver/storage"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const projectIDNamespaceLabel = "project-id"

func (r *ResourceManager) reserveRunQuota(ctx context.Context, run *model.Run) error {
	projectID, err := r.projectIDForNamespace(ctx, run.Namespace)
	if err != nil || projectID == "" || r.quotaStore == nil {
		return err
	}
	allocation, err := r.runQuotaAllocation(run)
	if err != nil {
		return err
	}
	allocation.RunID = run.UUID
	allocation.ProjectID = projectID
	return r.quotaStore.Reserve(projectID, run.UUID, allocation)
}

func (r *ResourceManager) releaseRunQuota(runID string) error {
	if r.quotaStore == nil {
		return nil
	}
	return r.quotaStore.Release(runID)
}

func (r *ResourceManager) projectIDForNamespace(ctx context.Context, namespace string) (string, error) {
	if namespace == "" || r.k8sCoreClient == nil {
		return "", nil
	}
	ns, err := r.k8sCoreClient.GetClientSet().CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		// Namespaces created before project provisioning do not participate in
		// project quota enforcement.
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", util.Wrap(err, "failed to resolve project metadata for namespace")
	}
	return strings.TrimSpace(ns.Labels[projectIDNamespaceLabel]), nil
}

func (r *ResourceManager) runQuotaAllocation(run *model.Run) (model.QuotaAllocation, error) {
	allocation := model.QuotaAllocation{}
	if run.PipelineSpec.PipelineVersionId == "" {
		return allocation, nil
	}
	version, err := r.pipelineStore.GetPipelineVersion(run.PipelineSpec.PipelineVersionId)
	if err != nil {
		return allocation, util.Wrap(err, "failed to load pipeline resource envelope")
	}
	if version.ResourceEnvelope == "" {
		return allocation, nil
	}
	var envelope struct {
		Requests map[string]string `json:"requests"`
		Limits   map[string]string `json:"limits"`
		GPUs     []struct {
			GPUModel string `json:"gpu_model"`
			GPUCount string `json:"gpu_count"`
		} `json:"gpus"`
	}
	if err := json.Unmarshal([]byte(version.ResourceEnvelope), &envelope); err != nil {
		return allocation, util.Wrap(err, "failed to parse pipeline resource envelope")
	}
	resources := envelope.Requests
	if len(resources) == 0 {
		resources = envelope.Limits
	}
	allocation.CPU = storage.QuantityToCeilCPU(resources["cpu"])
	allocation.MemoryGiB = storage.QuantityToCeilGiB(resources["memory"])
	for _, gpu := range envelope.GPUs {
		count := storage.QuantityToCeilCPU(gpu.GPUCount)
		modelName := strings.ToLower(gpu.GPUModel)
		switch {
		case strings.Contains(modelName, "rtx") && strings.Contains(modelName, "2080"):
			allocation.GPU2080 += count
		case strings.Contains(modelName, "h200"):
			allocation.GPUH200 += count
		}
	}
	return allocation, nil
}

func (r *ResourceManager) SetQuotaLimit(projectID string, limits model.QuotaLimit) error {
	return r.quotaStore.SetLimit(projectID, limits)
}

func (r *ResourceManager) GetQuotaUsage(projectID string) (model.QuotaLimit, error) {
	return r.quotaStore.GetUsage(projectID)
}

func (r *ResourceManager) GetQuotaLimits(projectID string) (model.QuotaLimit, error) {
	return r.quotaStore.GetUsage(projectID)
}

func (r *ResourceManager) GetQuotaUsageBulk(projectIDs []string) (map[string]model.QuotaLimit, error) {
	return r.quotaStore.GetUsageBulk(projectIDs)
}
