package redshift

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/Clever/pathio"
	_ "github.com/lib/pq" // Postgres driver.

	"github.com/Clever/redshifter/s3filepath"
)

type dbExecCloser interface {
	Close() error
	Begin() (*sql.Tx, error)
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Redshift wraps a dbExecCloser and can be used to perform operations on a redshift database.
type Redshift struct {
	dbExecCloser
}

// Table is our representation of a Redshift table
type Table struct {
	Name    string    `yaml:"dest"`
	Columns []ColInfo `yaml:"columns"`
	Meta    Meta      `yaml:"meta"`
}

// Meta holds information that might be not in Redshift or annoying to access
// in this case, we want to know the schema a table is part of
// and the column which corresponds to the timestamp at which the data was gathered
type Meta struct {
	DataDateColumn string `yaml:"datadatecolumn"`
	Schema         string `yaml:"schema"`
}

// ColInfo is a struct that contains information about a column in a Redshift database.
// SortOrdinal and DistKey only make sense for Redshift
type ColInfo struct {
	Ordinal     int    `yaml:"ordinal"`
	Name        string `yaml:"dest"`
	Type        string `yaml:"type"`
	DefaultVal  string `yaml:"defaultval"`
	NotNull     bool   `yaml:"notnull"`
	PrimaryKey  bool   `yaml:"primarykey"`
	DistKey     bool   `yaml:"distkey"`
	SortOrdinal int    `yaml:"sortord"`
}

// A helper to make sure the CSV copy works properly
type sortableColumns []ColInfo

func (c sortableColumns) Len() int           { return len(c) }
func (c sortableColumns) Less(i, j int) bool { return c[i].Ordinal < c[j].Ordinal }
func (c sortableColumns) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

var (
	// map between the config file and the redshift internal representations for types
	typeMapping = map[string]string{
		"boolean":   "boolean",
		"float":     "float",
		"int":       "int",
		"timestamp": "timestamp without time zone", // timestamp with timezone is not supported in redshift
		"text":      "character varying(256)",      // unfortunately redshift turns text -> varchar 256
	}

	// TODO: use parameter placeholder syntax instead
	existQueryFormat = `SELECT table_name
			  FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'`

	// returns one row per column with the attributes:
	// name, type, default_val, not_null, primary_key, dist_key, and sort_ordinal
	// need to pass a schema and table name as the parameters
	schemaQueryFormat = `SELECT
  f.attnum AS ordinal,
  f.attname AS name,
  pg_catalog.format_type(f.atttypid,f.atttypmod) AS col_type,
  CASE
      WHEN f.atthasdef = 't' THEN d.adsrc
      ELSE ''
  END AS default_val,
  f.attnotnull AS not_null,
  p.contype IS NOT NULL AND p.contype = 'p' AS primary_key,
  f.attisdistkey AS dist_key,
  f.attsortkeyord AS sort_ord
FROM pg_attribute f
  JOIN pg_class c ON c.oid = f.attrelid
  LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
WHERE c.relkind = 'r'::char
    AND n.nspname = '%s'  -- Replace with schema name
    AND c.relname = '%s'  -- Replace with table name
     AND f.attnum > 0 ORDER BY f.attnum`
)

// NewRedshift returns a pointer to a new redshift object using configuration values passed in
// on instantiation and the AWS env vars we assume exist
// Don't need to pass s3 info unless doing a COPY operation
func NewRedshift(host, port, db, user, password string, timeout int) (*Redshift, error) {
	flag.Parse()
	source := fmt.Sprintf("host=%s port=%s dbname=%s connect_timeout=%d", host, port, db, timeout)
	log.Println("Connecting to Redshift Source: ", source)
	source += fmt.Sprintf(" user=%s password=%s", user, password)
	sqldb, err := sql.Open("postgres", source)
	if err != nil {
		return nil, err
	}
	return &Redshift{sqldb}, nil
}

func (r *Redshift) logAndExec(cmd string) (sql.Result, error) {
	log.Printf("Executing Redshift command: %s", cmd)
	return r.Exec(cmd)
}

// GetTableFromConf returns the redshift table representation of the s3 conf file
// It opens, unmarshalls, and does very very simple validation of the conf file
// This belongs here - s3filepath should not have to know about redshift tables
func (r *Redshift) GetTableFromConf(f s3filepath.S3File) (Table, error) {
	var tempSchema map[string]Table
	var emptyTable Table

	log.Printf("Parsing file: %s", f.ConfFile)
	reader, err := pathio.Reader(f.ConfFile)
	if err != nil {
		return emptyTable, fmt.Errorf("error opening conf file: %s", err)
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return emptyTable, err
	}
	if err := yaml.Unmarshal(data, &tempSchema); err != nil {
		return emptyTable, fmt.Errorf("Warning: could not parse file %s, err: %s\n", f.ConfFile, err)
	}

	// data we want is nested in a map - possible to have multiple tables in a conf file
	t, ok := tempSchema[f.Table]
	if !ok {
		return emptyTable, fmt.Errorf("can't find table in conf")
	}
	if t.Meta.Schema != f.Schema {
		return emptyTable, fmt.Errorf("mismatched schema, conf: %s, file: %s", t.Meta.Schema, f.Schema)
	}
	if t.Meta.DataDateColumn == "" {
		return emptyTable, fmt.Errorf("Data Date Column must be set!")
	}

	return t, nil
}

// GetTableMetadata looks for a table and returns both the Table representation
// of the db table and the last data in the table, if that exists
// if the table does not exist it returns an empty table but does not error
func (r *Redshift) GetTableMetadata(schema, tableName, dataDateCol string) (Table, time.Time, error) {
	var retTable Table
	var lastData time.Time
	var cols []ColInfo

	// does the table exist?
	var placeholder string
	q := fmt.Sprintf(existQueryFormat, schema, tableName)
	if err := r.QueryRow(q).Scan(&placeholder); err != nil {
		if err == sql.ErrNoRows {
			return retTable, lastData, nil
		}
		return retTable, lastData, fmt.Errorf("issue just checking if the table exists: %s", err)
	}

	// table exists, what are the columns?
	rows, err := r.Query(fmt.Sprintf(schemaQueryFormat, schema, tableName))
	if err != nil {
		return retTable, lastData, fmt.Errorf("issue running column query: %s, err: %s", schemaQueryFormat, err)
	}
	defer rows.Close()
	for rows.Next() {
		var c ColInfo
		if err := rows.Scan(&c.Ordinal, &c.Name, &c.Type, &c.DefaultVal, &c.NotNull,
			&c.PrimaryKey, &c.DistKey, &c.SortOrdinal,
		); err != nil {
			return retTable, lastData, fmt.Errorf("issue scanning column, err: %s", err)
		}

		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return retTable, lastData, fmt.Errorf("issue iterating over columns, err: %s", err)
	}

	// turn into Table struct
	retTable = Table{
		Name:    tableName,
		Columns: cols,
		Meta: Meta{
			DataDateColumn: dataDateCol,
			Schema:         schema,
		},
	}

	// what's the last data in the table?
	lastDataQuery := fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY %s DESC LIMIT 1",
		dataDateCol, schema, tableName, dataDateCol)
	if err = r.QueryRow(lastDataQuery).Scan(&lastData); err != nil {
		return retTable, lastData, fmt.Errorf("issue running query: %s, err: %s", lastDataQuery, err)
	}
	return retTable, lastData, nil
}

func getColumnSQL(c ColInfo) string {
	// note that we are relying on redshift to fail if we have created multiple sort keys
	// currently we don't support that
	defaultVal := ""
	if c.DefaultVal != "" {
		defaultVal = fmt.Sprintf("DEFAULT %s", c.DefaultVal)
	}
	notNull := ""
	if c.NotNull {
		notNull = "NOT NULL"
	}
	primaryKey := ""
	if c.PrimaryKey {
		primaryKey = "PRIMARY KEY"
	}
	sortKey := ""
	if c.SortOrdinal == 1 {
		sortKey = "SORTKEY"
	}
	distKey := ""
	if c.DistKey {
		distKey = "DISTKEY"
	}

	return fmt.Sprintf("%s %s %s %s %s %s %s", c.Name, typeMapping[c.Type], defaultVal, notNull, sortKey, primaryKey, distKey)
}

// RunCreateTable runs the full create table command in the provided transaction, given a
// redshift representation of the table.
func (r *Redshift) RunCreateTable(tx *sql.Tx, table Table) error {
	var columnSQL []string
	for _, c := range table.Columns {
		columnSQL = append(columnSQL, getColumnSQL(c))
	}
	args := []interface{}{strings.Join(columnSQL, ",")}
	// for some reason prepare here was unable to succeed, perhaps look at this later
	createSQL := fmt.Sprintf(`CREATE TABLE "%s"."%s" (%s)`, table.Meta.Schema, table.Name, strings.Join(columnSQL, ","))
	createStmt, err := tx.Prepare(createSQL)
	if err != nil {
		return fmt.Errorf("issue preparing statement: %s", err)
	}

	log.Printf("Running command: %s with args: %v", createSQL, args)
	_, err = createStmt.Exec()
	return err
}

// RunUpdateTable figures out what columns we need to add to the target table based on the
// input table, and completes this action in the transaction provided
// Note: only supports adding columns currently, not updating existing columns or removing them
func (r *Redshift) RunUpdateTable(tx *sql.Tx, targetTable, inputTable Table) error {
	columnOps := []string{}
	for _, inCol := range inputTable.Columns {
		var existingCol ColInfo
		// yes, n^2, but how many are we really going to have, and I want to keep the ordering
		// ignore if there are extra columns
		for _, targetCol := range targetTable.Columns {
			if inCol.Name == targetCol.Name {
				mismatchedTemplate := "mismatched column: %s property: %s, input: %v, target: %v"
				if inCol.Ordinal != targetCol.Ordinal {
					return fmt.Errorf(mismatchedTemplate, inCol.Name, "Ordinal", inCol.Ordinal, targetCol.Ordinal)
				}
				if typeMapping[inCol.Type] != targetCol.Type {
					return fmt.Errorf(mismatchedTemplate, inCol.Name, "Type", typeMapping[inCol.Type], targetCol.Type)
				}
				if inCol.DefaultVal != targetCol.DefaultVal {
					return fmt.Errorf(mismatchedTemplate, inCol.Name, "DefaultVal", inCol.DefaultVal, targetCol.DefaultVal)
				}
				if inCol.NotNull != targetCol.NotNull {
					return fmt.Errorf(mismatchedTemplate, inCol.Name, "NotNull", inCol.NotNull, targetCol.NotNull)
				}
				if inCol.PrimaryKey != targetCol.PrimaryKey {
					return fmt.Errorf(mismatchedTemplate, inCol.Name, "PrimaryKey", inCol.PrimaryKey, targetCol.PrimaryKey)
				}
				if inCol.DistKey != targetCol.DistKey {
					return fmt.Errorf(mismatchedTemplate, inCol.Name, "DistKey", inCol.DistKey, targetCol.DistKey)
				}
				if inCol.SortOrdinal != targetCol.SortOrdinal {
					return fmt.Errorf(mismatchedTemplate, inCol.Name, "SortOrdinal", inCol.SortOrdinal, targetCol.SortOrdinal)
				}

				existingCol = targetCol
				break
			}
		}
		var emptyCol ColInfo
		if existingCol == emptyCol {
			columnOps = append(columnOps, fmt.Sprintf("ADD COLUMN %s ", getColumnSQL(inCol)))
		}
	}
	if len(columnOps) == 0 {
		log.Println("no update necessary")
		return nil
	}

	alterSQL := fmt.Sprintf(`ALTER TABLE "%s"."%s" %s`, targetTable.Meta.Schema, targetTable.Name, strings.Join(columnOps, ","))

	log.Printf("Running command: %s", alterSQL)
	_, err := tx.Exec(alterSQL)
	return err
}

// RunJSONCopy copies JSON data present in an S3 file into a redshift table.
// this is meant to be run in a transaction, so the first arg must be a sql.Tx
// if not using jsonPaths, set s3File.JSONPaths to "auto"
func (r *Redshift) RunJSONCopy(tx *sql.Tx, f s3filepath.S3File, creds, gzip bool) error {
	var credSQL string
	var credArgs []interface{}
	if creds {
		credSQL = `CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`
		credArgs = []interface{}{f.Bucket.AccessID(), f.Bucket.SecretKey()}
	}
	gzipSQL := ""
	if gzip {
		gzipSQL = "GZIP"
	}
	copySQL := fmt.Sprintf(`COPY "%s"."%s" FROM '%s' WITH %s JSON '%s' REGION '%s' TIMEFORMAT 'auto' STATUPDATE ON COMPUPDATE ON %s`, f.Schema, f.Table, f.GetDataFilename(), gzipSQL, f.JSONPaths, f.Bucket.Region(), credSQL)
	fullCopySQL := fmt.Sprintf(fmt.Sprintf(copySQL, credArgs...))
	log.Printf("Running command: %s", copySQL)
	_, err := tx.Exec(fullCopySQL)
	return err
}

// RunTruncate deletes all items from a table, given a transaction, a schema string and a table name
// you shuold run vacuum and analyze soon after doing this for performance reasons
func (r *Redshift) RunTruncate(tx *sql.Tx, schema, table string) error {
	truncStmt, err := tx.Prepare(`DELETE FROM "?"."?"`)
	if err != nil {
		return err
	}
	_, err = truncStmt.Exec(schema, table)
	return err
}

// VacuumAnalyze performs VACUUM FULL; ANALYZE on the redshift database. This is useful for
// recreating the indices after a database has been modified and updating the query planner.
func (r *Redshift) VacuumAnalyze() error {
	_, err := r.logAndExec("VACUUM FULL; ANALYZE")
	return err
}
