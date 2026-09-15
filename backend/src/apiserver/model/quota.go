package model

// QuotaLimit stores the configured and currently reserved resources for one
// project in the Core instance's region.
type QuotaLimit struct {
	ProjectID        string `gorm:"column:ProjectID;type:varchar(191);primaryKey"`
	CPUMilliLimit    int64  `gorm:"column:CPUMilliLimit;not null;default:0"`
	MemoryMiBLimit   int64  `gorm:"column:MemoryMiBLimit;not null;default:0"`
	GPU2080Limit     int64  `gorm:"column:GPU2080Limit;not null;default:0"`
	GPUH200Limit     int64  `gorm:"column:GPUH200Limit;not null;default:0"`
	CPUMilliUsed     int64  `gorm:"column:CPUMilliUsed;not null;default:0"`
	MemoryMiBUsed    int64  `gorm:"column:MemoryMiBUsed;not null;default:0"`
	GPU2080Used      int64  `gorm:"column:GPU2080Used;not null;default:0"`
	GPUH200Used      int64  `gorm:"column:GPUH200Used;not null;default:0"`
}

func (QuotaLimit) TableName() string { return "quota_limits" }

// QuotaAllocation is the reservation attached to an active run.
type QuotaAllocation struct {
	RunID      string `gorm:"column:RunUUID;type:varchar(191);primaryKey"`
	ProjectID  string `gorm:"column:ProjectID;type:varchar(191);not null;index"`
	CPUMilli   int64  `gorm:"column:CPUMilli;not null;default:0"`
	MemoryMiB  int64  `gorm:"column:MemoryMiB;not null;default:0"`
	GPU2080    int64  `gorm:"column:GPU2080;not null;default:0"`
	GPUH200    int64  `gorm:"column:GPUH200;not null;default:0"`
}

func (QuotaAllocation) TableName() string { return "quota_allocations" }

type QuotaExceededDetail struct {
	QuotaUnit string `json:"quota_unit"`
	Limit     int64  `json:"limit"`
	Used      int64  `json:"used"`
	Requested int64  `json:"requested"`
}
