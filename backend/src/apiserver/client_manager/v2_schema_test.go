// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package clientmanager

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPruneV1Schema(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	statements := []string{
		`CREATE TABLE pipelines (UUID TEXT PRIMARY KEY, Namespace TEXT, DefaultVersionId TEXT, Parameters TEXT)`,
		`CREATE TABLE pipeline_versions (UUID TEXT PRIMARY KEY, Parameters TEXT)`,
		`CREATE TABLE run_details (UUID TEXT PRIMARY KEY, Namespace TEXT, RuntimeParameters TEXT, Parameters TEXT)`,
		`CREATE TABLE jobs (UUID TEXT PRIMARY KEY, Namespace TEXT, RuntimeParameters TEXT, Parameters TEXT)`,
		`CREATE TABLE run_metrics (RunUUID TEXT)`,
		`CREATE TABLE resource_references (ResourceUUID TEXT)`,
	}
	for _, statement := range statements {
		require.NoError(t, db.Exec(statement).Error)
	}

	require.NoError(t, pruneV1Schema(db))
	require.False(t, db.Migrator().HasTable("run_metrics"))
	require.False(t, db.Migrator().HasTable("resource_references"))
	assertExactColumn(t, db, "pipelines", "DefaultVersionId", false)
	assertExactColumn(t, db, "pipelines", "Parameters", false)
	assertExactColumn(t, db, "pipeline_versions", "Parameters", false)
	assertExactColumn(t, db, "run_details", "Parameters", false)
	assertExactColumn(t, db, "jobs", "Parameters", false)
	assertExactColumn(t, db, "pipelines", "Namespace", true)
	assertExactColumn(t, db, "run_details", "RuntimeParameters", true)

	// The migration is intentionally safe to run on every startup.
	require.NoError(t, pruneV1Schema(db))
}

func assertExactColumn(t *testing.T, db *gorm.DB, table, column string, expected bool) {
	t.Helper()
	actual, err := hasExactColumn(db, table, column)
	require.NoError(t, err)
	require.Equal(t, expected, actual, "%s.%s", table, column)
}
