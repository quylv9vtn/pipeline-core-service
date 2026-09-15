// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestMySQLDialect_GroupConcat_WithSeparator(t *testing.T) {
	mysqlDialect := NewMySQLDialect()

	actualQuery := mysqlDialect.GroupConcat(`col1,",",col2`, ";")

	expectedQuery := `GROUP_CONCAT(col1,",",col2 SEPARATOR ";")`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestMySQLDialect_GroupConcat_WithoutSeparator(t *testing.T) {
	mysqlDialect := NewMySQLDialect()

	actualQuery := mysqlDialect.GroupConcat(`col1,",",col2`, "")

	expectedQuery := `GROUP_CONCAT(col1,",",col2)`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestMySQLDialect_Concat_WithSeparator(t *testing.T) {
	mysqlDialect := NewMySQLDialect()

	actualQuery := mysqlDialect.Concat([]string{"col1", "col2"}, ",")

	expectedQuery := `CONCAT(col1,",",col2)`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestMySQLDialect_Concat_WithoutSeparator(t *testing.T) {
	mysqlDialect := NewMySQLDialect()

	actualQuery := mysqlDialect.Concat([]string{"col1", "col2"}, "")

	expectedQuery := `CONCAT(col1,col2)`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestSQLiteDialect_GroupConcat_WithSeparator(t *testing.T) {
	sqliteDialect := NewSQLiteDialect()

	actualQuery := sqliteDialect.GroupConcat(`col1||","||col2`, ";")

	expectedQuery := `GROUP_CONCAT(col1||","||col2, ";")`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestSQLiteDialect_GroupConcat_WithoutSeparator(t *testing.T) {
	sqliteDialect := NewSQLiteDialect()

	actualQuery := sqliteDialect.GroupConcat(`col1||","||col2`, "")

	expectedQuery := `GROUP_CONCAT(col1||","||col2)`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestSQLiteDialect_Concat_WithSeparator(t *testing.T) {
	sqliteDialect := NewSQLiteDialect()

	actualQuery := sqliteDialect.Concat([]string{"col1", "col2"}, ",")

	expectedQuery := `col1||","||col2`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestSQLiteDialect_Concat_WithoutSeparator(t *testing.T) {
	sqliteDialect := NewSQLiteDialect()

	actualQuery := sqliteDialect.Concat([]string{"col1", "col2"}, "")

	expectedQuery := `col1||col2`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestSQLiteDialect_Upsert(t *testing.T) {
	sqliteDialect := NewSQLiteDialect()
	actualQuery := sqliteDialect.Upsert(`insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow")`, "namespace", true, []string{"uuid", "name"}...)
	expectedQuery := `insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow") ON CONFLICT(namespace) DO UPDATE SET uuid=excluded.uuid,name=excluded.name`
	assert.Equal(t, expectedQuery, actualQuery)
	actualQuery2 := sqliteDialect.Upsert(`insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow")`, "namespace", false, []string{"uuid", "name"}...)
	expectedQuery2 := `insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow") ON CONFLICT(namespace) DO UPDATE SET uuid=uuid,name=name`
	assert.Equal(t, expectedQuery2, actualQuery2)
}

func TestMySQLDialect_Upsert(t *testing.T) {
	mysqlDialect := NewMySQLDialect()
	actualQuery := mysqlDialect.Upsert(`insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow")`, "namespace", true, []string{"uuid", "name"}...)
	expectedQuery := `insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow") ON DUPLICATE KEY UPDATE uuid=VALUES(uuid),name=VALUES(name)`
	assert.Equal(t, expectedQuery, actualQuery)
	actualQuery2 := mysqlDialect.Upsert(`insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow")`, "namespace", false, []string{"uuid", "name"}...)
	expectedQuery2 := `insert into table (uuid, name, namespace) values ("a", "item1", "kubeflow"),("b", "item1", "kubeflow") ON DUPLICATE KEY UPDATE uuid=uuid,name=name`
	assert.Equal(t, expectedQuery2, actualQuery2)
}

func TestMySQLDialect_UpdateWithJointOrFrom(t *testing.T) {
	mysqlDialect := NewMySQLDialect()
	actualQuery := mysqlDialect.UpdateWithJointOrFrom(
		"target_table",
		"other_table",
		"State = ?",
		"target_table.Name = other_table.Name",
		"target_table.status = ?")
	expectedQuery := `UPDATE target_table INNER JOIN other_table ON target_table.Name = other_table.Name SET State = ? WHERE target_table.status = ?`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestSQLiteDialect_UpdateWithJointOrFrom(t *testing.T) {
	sqliteDialect := NewSQLiteDialect()
	actualQuery := sqliteDialect.UpdateWithJointOrFrom(
		"target_table",
		"other_table",
		"State = ?",
		"target_table.Name = other_table.Name",
		"target_table.status = ?")
	expectedQuery := `UPDATE target_table SET State = ? FROM other_table WHERE target_table.Name = other_table.Name AND target_table.status = ?`
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestPostgreSQLDialect_Rebind(t *testing.T) {
	dialect := NewPostgreSQLDialect()
	actual := dialect.Rebind("SELECT * FROM pipelines WHERE Namespace = ? AND Status = ?")
	assert.Equal(t, "SELECT * FROM pipelines WHERE \"Namespace\" = $1 AND \"Status\" = $2", actual)
	actual = dialect.Rebind(`SELECT CONCAT("[", Payload, "]") FROM resource_references WHERE ResourceType='Run'`)
	assert.Equal(t, `SELECT CONCAT('[', "Payload", ']') FROM resource_references WHERE "ResourceType"='Run'`, actual)
	actual = dialect.Rebind("SELECT UUID FROM experiments WHERE uuid = ?")
	assert.Equal(t, `SELECT "UUID" FROM experiments WHERE "UUID" = $1`, actual)
}

func TestPostgreSQLDialect_RebindUpsert(t *testing.T) {
	dialect := NewPostgreSQLDialect()
	actual := dialect.Rebind("INSERT INTO tasks (UUID, RunUUID, Name) VALUES (?, ?, ?) ON CONFLICT(UUID) DO UPDATE SET Name=excluded.Name RETURNING UUID")
	assert.Equal(t, `INSERT INTO tasks ("UUID", "RunUUID", "Name") VALUES ($1, $2, $3) ON CONFLICT("UUID") DO UPDATE SET "Name"=excluded."Name" RETURNING "UUID"`, actual)
}

func TestPostgreSQLDialect_GroupConcat(t *testing.T) {
	dialect := NewPostgreSQLDialect()
	assert.Equal(t, "STRING_AGG(r.Payload, ',')", dialect.GroupConcat("r.Payload", ","))
}

func TestPostgreSQLDialect_IsDuplicateError(t *testing.T) {
	dialect := NewPostgreSQLDialect()
	assert.True(t, dialect.IsDuplicateError(&pgconn.PgError{Code: "23505"}))
	assert.True(t, dialect.IsDuplicateError(fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "23505"})))
	assert.False(t, dialect.IsDuplicateError(&pgconn.PgError{Code: "23503"}))
	assert.False(t, dialect.IsDuplicateError(fmt.Errorf("not a PostgreSQL error")))
}
