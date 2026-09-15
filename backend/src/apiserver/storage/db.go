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
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// DB a struct wrapping plain sql library with SQL dialect, to solve any feature
// difference between MySQL, which is used in production, and Sqlite, which is used
// for unit testing.
type DB struct {
	*sql.DB
	SQLDialect
}

type Tx struct {
	*sql.Tx
	dialect SQLDialect
}

// NewDB creates a DB.
func NewDB(db *sql.DB, dialect SQLDialect) *DB {
	return &DB{db, dialect}
}

// SQLDialect abstracts common sql queries which vary in different dialect.
// It is used to bridge the difference between mysql (production) and sqlite
// (test).
type SQLDialect interface {
	Rebind(query string) string
	// GroupConcat builds query to group concatenate `expr` in each row and use `separator`
	// to join rows in a group.
	GroupConcat(expr string, separator string) string

	// Concat builds query to concatenete a list of `exprs` into a single string with
	// a separator in between.
	Concat(exprs []string, separator string) string

	// Check whether the error is a SQL duplicate entry error or not
	IsDuplicateError(err error) bool

	// Modifies the SELECT clause in query to return one that locks the selected
	// row for update.
	SelectForUpdate(query string) string

	// Inserts new rows and updates duplicates based on the key column.
	Upsert(query string, key string, overwrite bool, columns ...string) string

	// Updates a table using UPDATE with JOIN (mysql/production) or UPDATE FROM (sqlite/test).
	UpdateWithJointOrFrom(targetTable, joinTable, setClause, joinClause, whereClause string) string
}

func (d *DB) Begin() (*Tx, error) {
	tx, err := d.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx, dialect: d.SQLDialect}, nil
}

func (d *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return d.DB.Query(d.Rebind(query), args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return d.DB.QueryRow(d.Rebind(query), args...)
}

func (d *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return d.DB.Exec(d.Rebind(query), args...)
}

func (t *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.Tx.Query(t.dialect.Rebind(query), args...)
}

func (t *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return t.Tx.QueryRow(t.dialect.Rebind(query), args...)
}

func (t *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return t.Tx.Exec(t.dialect.Rebind(query), args...)
}

// MySQLDialect implements SQLDialect with mysql dialect implementation.
type MySQLDialect struct{}

func (d MySQLDialect) Rebind(query string) string { return query }

func (d MySQLDialect) GroupConcat(expr string, separator string) string {
	var buffer bytes.Buffer
	buffer.WriteString("GROUP_CONCAT(")
	buffer.WriteString(expr)
	if separator != "" {
		buffer.WriteString(fmt.Sprintf(" SEPARATOR \"%s\"", separator))
	}
	buffer.WriteString(")")
	return buffer.String()
}

func (d MySQLDialect) Concat(exprs []string, separator string) string {
	separatorSQL := ","
	if separator != "" {
		separatorSQL = fmt.Sprintf(`,"%s",`, separator)
	}
	return fmt.Sprintf("CONCAT(%s)", strings.Join(exprs, separatorSQL))
}

func (d MySQLDialect) IsDuplicateError(err error) bool {
	sqlError, ok := err.(*mysql.MySQLError)
	return ok && sqlError.Number == mysqlerr.ER_DUP_ENTRY
}

func (d MySQLDialect) UpdateWithJointOrFrom(targetTable, joinTable, setClause, joinClause, whereClause string) string {
	return fmt.Sprintf("UPDATE %s INNER JOIN %s ON %s SET %s WHERE %s", targetTable, joinTable, joinClause, setClause, whereClause)
}

// SQLiteDialect implements SQLDialect with sqlite dialect implementation.
type SQLiteDialect struct{}

func (d SQLiteDialect) Rebind(query string) string { return query }

func (d SQLiteDialect) GroupConcat(expr string, separator string) string {
	var buffer bytes.Buffer
	buffer.WriteString("GROUP_CONCAT(")
	buffer.WriteString(expr)
	if separator != "" {
		buffer.WriteString(fmt.Sprintf(", \"%s\"", separator))
	}
	buffer.WriteString(")")
	return buffer.String()
}

func (d SQLiteDialect) Concat(exprs []string, separator string) string {
	separatorSQL := "||"
	if separator != "" {
		separatorSQL = fmt.Sprintf(`||"%s"||`, separator)
	}
	return strings.Join(exprs, separatorSQL)
}

func (d MySQLDialect) SelectForUpdate(query string) string {
	return query + " FOR UPDATE"
}

func (d SQLiteDialect) SelectForUpdate(query string) string {
	return query
}

func (d MySQLDialect) Upsert(query string, key string, overwrite bool, columns ...string) string {
	return fmt.Sprintf("%v ON DUPLICATE KEY UPDATE %v", query, prepareUpdateSuffixMySQL(columns, overwrite))
}

func (d SQLiteDialect) Upsert(query string, key string, overwrite bool, columns ...string) string {
	return fmt.Sprintf("%v ON CONFLICT(%v) DO UPDATE SET %v", query, key, prepareUpdateSuffixSQLite(columns, overwrite))
}

func (d SQLiteDialect) IsDuplicateError(err error) bool {
	sqlError, ok := err.(sqlite3.Error)
	return ok && sqlError.Code == sqlite3.ErrConstraint
}

func (d SQLiteDialect) UpdateWithJointOrFrom(targetTable, joinTable, setClause, joinClause, whereClause string) string {
	return fmt.Sprintf("UPDATE %s SET %s FROM %s WHERE %s AND %s", targetTable, setClause, joinTable, joinClause, whereClause)
}

func NewMySQLDialect() MySQLDialect {
	return MySQLDialect{}
}

func NewSQLiteDialect() SQLiteDialect {
	return SQLiteDialect{}
}

// PostgreSQLDialect implements the SQL differences used by the legacy stores.
type PostgreSQLDialect struct{}

func (d PostgreSQLDialect) Rebind(query string) string {
	var b strings.Builder
	parameter := 1
	for i := 0; i < len(query); {
		if query[i] == '\'' {
			// Preserve PostgreSQL/MySQL single-quoted literals verbatim.
			b.WriteByte(query[i])
			i++
			for i < len(query) {
				b.WriteByte(query[i])
				if query[i] == '\'' {
					if i+1 < len(query) && query[i+1] == '\'' {
						b.WriteByte(query[i+1])
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		if query[i] == '?' {
			fmt.Fprintf(&b, "$%d", parameter)
			parameter++
			i++
			continue
		}
		if query[i] == '"' {
			// Legacy KFP SQL uses MySQL double-quoted string literals.
			end := strings.IndexByte(query[i+1:], '"')
			if end >= 0 {
				literal := query[i+1 : i+1+end]
				b.WriteByte('\'')
				b.WriteString(strings.ReplaceAll(literal, "'", "''"))
				b.WriteByte('\'')
				i += end + 2
				continue
			}
		}
		if isSQLIdentifierStart(query[i]) {
			end := i + 1
			for end < len(query) && isSQLIdentifierPart(query[end]) {
				end++
			}
			identifier := query[i:end]
			canonicalIdentifier := canonicalPostgreSQLIdentifier(identifier)
			if canonicalIdentifier != "" {
				b.WriteByte('"')
				b.WriteString(canonicalIdentifier)
				b.WriteByte('"')
			} else if shouldQuotePostgreSQLIdentifier(identifier) {
				b.WriteByte('"')
				b.WriteString(identifier)
				b.WriteByte('"')
			} else {
				b.WriteString(identifier)
			}
			i = end
			continue
		}
		b.WriteByte(query[i])
		i++
	}
	return b.String()
}

func canonicalPostgreSQLIdentifier(identifier string) string {
	// Some legacy stores use lower-case field keys while the historical schema
	// defines the column with quoted upper-case spelling.
	switch identifier {
	case "uuid":
		return "UUID"
	default:
		return ""
	}
}

func isSQLIdentifierStart(c byte) bool {
	return c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func isSQLIdentifierPart(c byte) bool {
	return isSQLIdentifierStart(c) || c >= '0' && c <= '9'
}

func shouldQuotePostgreSQLIdentifier(identifier string) bool {
	if identifier == strings.ToLower(identifier) {
		return false
	}
	keywords := map[string]struct{}{
		"ALL": {}, "AND": {}, "AS": {}, "ASC": {}, "BY": {}, "CASE": {},
		"CONCAT": {}, "CONFLICT": {}, "COUNT": {}, "DELETE": {}, "DESC": {}, "DISTINCT": {},
		"DO": {}, "ELSE": {}, "END": {}, "EXCLUDED": {}, "EXISTS": {}, "FALSE": {}, "FOR": {}, "FROM": {},
		"GROUP": {}, "HAVING": {}, "IN": {}, "INNER": {}, "INSERT": {}, "INTO": {},
		"IS": {}, "JOIN": {}, "LEFT": {}, "LIKE": {}, "LIMIT": {}, "NOT": {},
		"NOTHING": {}, "NULL": {}, "ON": {}, "OR": {}, "ORDER": {}, "OVER": {}, "PARTITION": {},
		"RIGHT": {}, "ROW_NUMBER": {}, "SELECT": {}, "SET": {}, "STRING_AGG": {},
		"RETURNING": {}, "THEN": {}, "TRUE": {}, "UNION": {}, "UPDATE": {}, "VALUES": {}, "WHEN": {},
		"WHERE": {},
	}
	_, keyword := keywords[strings.ToUpper(identifier)]
	return !keyword
}

func (d PostgreSQLDialect) GroupConcat(expr string, separator string) string {
	if separator == "" {
		separator = ","
	}
	return fmt.Sprintf("STRING_AGG(%s, '%s')", expr, strings.ReplaceAll(separator, "'", "''"))
}

func (d PostgreSQLDialect) Concat(exprs []string, separator string) string {
	joiner := " || "
	if separator != "" {
		joiner = fmt.Sprintf(" || '%s' || ", strings.ReplaceAll(separator, "'", "''"))
	}
	return strings.Join(exprs, joiner)
}

func (d PostgreSQLDialect) IsDuplicateError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
func (d PostgreSQLDialect) SelectForUpdate(query string) string { return query + " FOR UPDATE" }
func (d PostgreSQLDialect) Upsert(query string, key string, overwrite bool, columns ...string) string {
	return fmt.Sprintf("%v ON CONFLICT(%v) DO UPDATE SET %v", query, key, prepareUpdateSuffixSQLite(columns, overwrite))
}
func (d PostgreSQLDialect) UpdateWithJointOrFrom(targetTable, joinTable, setClause, joinClause, whereClause string) string {
	return fmt.Sprintf("UPDATE %s SET %s FROM %s WHERE %s AND %s", targetTable, setClause, joinTable, joinClause, whereClause)
}

func NewPostgreSQLDialect() PostgreSQLDialect { return PostgreSQLDialect{} }

func prepareUpdateSuffixMySQL(columns []string, overwrite bool) string {
	columnsExtended := make([]string, 0)
	if overwrite {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=VALUES(%[1]v)", c))
		}
	} else {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=%[1]v", c))
		}
	}
	return strings.Join(columnsExtended, ",")
}

func prepareUpdateSuffixSQLite(columns []string, overwrite bool) string {
	columnsExtended := make([]string, 0)
	if overwrite {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=excluded.%[1]v", c))
		}
	} else {
		for _, c := range columns {
			columnsExtended = append(columnsExtended, fmt.Sprintf("%[1]v=%[1]v", c))
		}
	}
	return strings.Join(columnsExtended, ",")
}
