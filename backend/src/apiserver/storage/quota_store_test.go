package storage

import (
	"testing"

	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/stretchr/testify/assert"
)

func TestQuotaResourceQuantitiesRoundUp(t *testing.T) {
	assert.Equal(t, int64(500), QuantityToMilliCPU("500m"))
	assert.Equal(t, int64(1500), QuantityToMilliCPU("1500m"))
	assert.Equal(t, int64(1024), QuantityToCeilMiB("1Gi"))
	assert.Equal(t, int64(1290), QuantityToCeilMiB("1290Mi"))
}

func TestQuotaDetailsIncludesEveryExceededUnit(t *testing.T) {
	current := model.QuotaLimit{CPUMilliLimit: 4, MemoryMiBLimit: 8, GPU2080Limit: 1, GPUH200Limit: 2, CPUMilliUsed: 3, MemoryMiBUsed: 8, GPU2080Used: 1, GPUH200Used: 0}
	requested := model.QuotaAllocation{CPUMilli: 2, MemoryMiB: 1, GPU2080: 1, GPUH200: 3}
	details := quotaDetails(current, requested)
	assert.Len(t, details, 4)
	assert.Equal(t, "ai_pipeline_cpu", details[0].QuotaUnit)
	assert.Equal(t, int64(3), details[0].Used)
	assert.Equal(t, int64(2), details[0].Requested)
}
