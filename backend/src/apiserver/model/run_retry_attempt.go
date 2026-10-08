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

package model

// RunRetryAttempt records a single retry of a pipeline run.
// The original (first) execution is NOT recorded here; only explicit retries
// triggered via RetryRun. AttemptNumber starts at 1 and increments with each retry.
type RunRetryAttempt struct {
	// Auto-increment primary key.
	ID int64 `gorm:"column:ID; primaryKey; autoIncrement"`
	// Foreign key to run_details.UUID.
	RunUUID string `gorm:"column:RunUUID; not null; index; type:varchar(191)"`
	// 1-based counter; incremented each time RetryRun is called.
	AttemptNumber int `gorm:"column:AttemptNumber; not null"`
	// Unix timestamp when RetryRun was triggered.
	TriggeredAt int64 `gorm:"column:TriggeredAt; not null; default:0"`
	// User who triggered the retry (Kubeflow user identity header). Empty in single-user mode.
	TriggeredBy string `gorm:"column:TriggeredBy; default:''"`
	// Unix timestamp when the workflow transitioned to RUNNING for this attempt.
	// 0 until the run reaches RUNNING state.
	StartedAt int64 `gorm:"column:StartedAt; default:0"`
	// Unix timestamp when the workflow reached a terminal state. 0 if not finished.
	FinishedAt int64 `gorm:"column:FinishedAt; default:0"`
	// Terminal state of this attempt (SUCCEEDED, FAILED, CANCELED, …). Empty if not finished.
	FinalState string `gorm:"column:FinalState; default:''"`
}

func (RunRetryAttempt) TableName() string { return "run_retry_history" }
