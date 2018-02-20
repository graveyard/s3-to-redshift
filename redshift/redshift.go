package redshift

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/Clever/pathio"
	multierror "github.com/hashicorp/go-multierror"
	// Use our own version of the postgres library so we get keep-alive support.
	// See https://github.com/Clever/pq/pull/1
	_ "github.com/Clever/pq"

	"github.com/Clever/s3-to-redshift/s3filepath"
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
	Name        string `yaml:"dest"`
	Type        string `yaml:"type"`
	DefaultVal  string `yaml:"defaultval"`
	NotNull     bool   `yaml:"notnull"`
	PrimaryKey  bool   `yaml:"primarykey"`
	DistKey     bool   `yaml:"distkey"`
	SortOrdinal int    `yaml:"sortord"`
}

const (
	// TODO: use parameter placeholder syntax instead
	existQueryFormat = `SELECT table_name
			  FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'`

	// returns one row per column with the attributes:
	// name, type, default_val, not_null, primary_key, dist_key, and sort_ordinal
	// need to pass a schema and table name as the parameters
	schemaQueryFormat = `SELECT
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

var (
	// map between the config file and the redshift internal representations for types
	typeMapping = map[string]string{
		"boolean":   "boolean",
		"float":     "double precision",
		"int":       "integer",
		"date":      "date",
		"timestamp": "timestamp without time zone", // timestamp with timezone is not supported in redshift
		"text":      "character varying(256)",      // unfortunately redshift turns text -> varchar 256
		"longtext":  "character varying(65535)",    // when you actually need more than 256 characters
	}
)

// NewRedshift returns a pointer to a new redshift object using configuration values passed in
// on instantiation and the AWS env vars we assume exist
// Don't need to pass s3 info unless doing a COPY operation
func NewRedshift(host, port, db, user, password string, timeout int) (*Redshift, error) {
	source := fmt.Sprintf("host=%s port=%s dbname=%s keepalive=1 connect_timeout=%d", host, port, db, timeout)
	log.Println("Connecting to Redshift Source: ", source)
	source += fmt.Sprintf(" user=%s password=%s", user, password)
	sqldb, err := sql.Open("postgres", source)
	if err != nil {
		return nil, err
	}
	return &Redshift{sqldb}, nil
}

// GetTableFromConf returns the redshift table representation of the s3 conf file
// It opens, unmarshalls, and does very very simple validation of the conf file
// This belongs here - s3filepath should not have to know about redshift tables
func (r *Redshift) GetTableFromConf(f s3filepath.S3File) (*Table, error) {
	var tempSchema map[string]Table

	log.Printf("Parsing file: %s", f.ConfFile)
	reader, err := pathio.Reader(f.ConfFile)
	if err != nil {
		return nil, fmt.Errorf("error opening conf file: %s", err)
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, &tempSchema); err != nil {
		return nil, fmt.Errorf("warning: could not parse file %s, err: %s", f.ConfFile, err)
	}

	// data we want is nested in a map - possible to have multiple tables in a conf file
	t, ok := tempSchema[f.Table]
	if !ok {
		return nil, fmt.Errorf("can't find table in conf")
	}
	if t.Meta.Schema != f.Schema {
		return nil, fmt.Errorf("mismatched schema, conf: %s, file: %s", t.Meta.Schema, f.Schema)
	}
	if t.Meta.DataDateColumn == "" {
		return nil, fmt.Errorf("data date column must be set")
	}

	return &t, nil
}

// GetTableMetadata looks for a table and returns both the Table representation
// of the db table and the last data in the table, if that exists
// if the table does not exist it returns an empty table but does not error
func (r *Redshift) GetTableMetadata(schema, tableName, dataDateCol string) (*Table, *time.Time, error) {
	var cols []ColInfo

	// does the table exist?
	var placeholder string
	q := fmt.Sprintf(existQueryFormat, schema, tableName)
	if err := r.QueryRow(q).Scan(&placeholder); err != nil {
		// If the table doesn't exist, log it, but don't return an
		// error since this is not an application error.
		// The correct behavior is to create a new table.
		if err == sql.ErrNoRows {
			log.Printf("schema: %s, table: %s does not exist", schema, tableName)
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("issue just checking if the table exists: %s", err)
	}

	// table exists, what are the columns?
	rows, err := r.Query(fmt.Sprintf(schemaQueryFormat, schema, tableName))
	if err != nil {
		return nil, nil, fmt.Errorf("issue running column query: %s, err: %s", schemaQueryFormat, err)
	}
	defer rows.Close()
	for rows.Next() {
		var c ColInfo
		if err := rows.Scan(&c.Name, &c.Type, &c.DefaultVal, &c.NotNull,
			&c.PrimaryKey, &c.DistKey, &c.SortOrdinal,
		); err != nil {
			return nil, nil, fmt.Errorf("issue scanning column, err: %s", err)
		}

		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("issue iterating over columns, err: %s", err)
	}

	// turn into Table struct
	retTable := Table{
		Name:    tableName,
		Columns: cols,
		Meta: Meta{
			DataDateColumn: dataDateCol,
			Schema:         schema,
		},
	}

	// what's the last data in the table?
	lastDataQuery := fmt.Sprintf(`SELECT "%s" FROM "%s"."%s" ORDER BY "%s" DESC LIMIT 1`,
		dataDateCol, schema, tableName, dataDateCol)
	var lastData time.Time
	err = r.QueryRow(lastDataQuery).Scan(&lastData)
	if err != nil && err == sql.ErrNoRows {
		return &retTable, nil, nil
	} else if err != nil {
		return nil, nil, fmt.Errorf("issue running query: %s, err: %s", lastDataQuery, err)
	}
	return &retTable, &lastData, nil
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

	return fmt.Sprintf(" \"%s\" %s %s %s %s %s %s", c.Name, typeMapping[c.Type], defaultVal, notNull, sortKey, primaryKey, distKey)
}

// CreateTable runs the full create table command in the provided transaction, given a
// redshift representation of the table.
func (r *Redshift) CreateTable(tx *sql.Tx, table Table) error {
	var columnSQL []string
	for _, c := range table.Columns {
		columnSQL = append(columnSQL, getColumnSQL(c))
	}
	args := []interface{}{strings.Join(columnSQL, ",")}
	// for some reason prepare here was unable to succeed, perhaps look at this later
	createSQL := fmt.Sprintf(`CREATE TABLE "%s"."%s" (%s)`, table.Meta.Schema, table.Name, strings.Join(columnSQL, ","))

	if match, _ := regexp.MatchString("SORTKEY|DISTKEY", createSQL); !match {
		return fmt.Errorf("both SORTKEY and DISTKEY should be specified in create table: %s. Either create your own table if you truly don't want those keys, or update the config to contain both", createSQL)
	}

	createStmt, err := tx.Prepare(createSQL)
	if err != nil {
		return fmt.Errorf("issue preparing statement: %s", err)
	}

	log.Printf("Running command: %s with args: %v", createSQL, args)
	_, err = createStmt.Exec()
	return err
}

// UpdateTable figures out what columns we need to add to the target table based on the
// input table, and completes this action in the transaction provided
// Note: only supports adding columns currently, not updating existing columns or removing them
func (r *Redshift) UpdateTable(tx *sql.Tx, inputTable, targetTable Table) error {

	columnOps, err := checkSchemas(inputTable, targetTable)
	if err != nil {
		return fmt.Errorf("mismatched schema: %s", err)
	}

	// postgres only allows adding one column at a time
	for _, op := range columnOps {
		alterStmt, err := tx.Prepare(op)
		if err != nil {
			return fmt.Errorf("issue preparing statement: '%s' - err: %s", op, err)
		}

		log.Printf("Running command: %s", op)
		_, err = alterStmt.Exec()
		if err != nil {
			return fmt.Errorf("issue running statement %s: %s", op, err)
		}
	}
	return nil
}

// checkSchemas takes in two tables and compares their column schemas to make sure they're compatible.
// If they have any mismatched columns they are returned in the errors array. If the input table has
// columns at the end that the target table does not then the appropriate alter tables sql commands are
// returned.
func checkSchemas(inputTable, targetTable Table) ([]string, error) {
	// If the schema is mongo then we know the input files are json so ordering doesn't matter. At
	// some point we could handle this in a more general way by checking if the input files are json.
	// This wouldn't be too hard, but we would have to peak in the manifest file to check if all the
	// files references are json.
	if targetTable.Meta.Schema == "mongo" {
		return checkColumnsWithoutOrdering(inputTable, targetTable)
	}
	return checkColumnsAndOrdering(inputTable, targetTable)
}

func checkColumnsAndOrdering(inputTable, targetTable Table) ([]string, error) {
	var columnOps []string
	var errors error
	if len(inputTable.Columns) < len(targetTable.Columns) {
		errors = multierror.Append(errors, fmt.Errorf("target table has more columns than the input table"))
	}

	for idx, inCol := range inputTable.Columns {
		if len(targetTable.Columns) <= idx {
			log.Printf("Missing column -- running alter table\n")
			alterSQL := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN %s`, targetTable.Meta.Schema, targetTable.Name, getColumnSQL(inCol))
			columnOps = append(columnOps, alterSQL)
			continue
		}

		targetCol := targetTable.Columns[idx]
		err := checkColumn(inCol, targetCol)
		if err != nil {
			errors = multierror.Append(errors, err)
		}

	}
	return columnOps, errors
}

func checkColumnsWithoutOrdering(inputTable, targetTable Table) ([]string, error) {
	var columnOps []string
	var errors error

	for _, inCol := range inputTable.Columns {
		foundMatching := false
		for _, targetCol := range targetTable.Columns {
			if inCol.Name == targetCol.Name {
				foundMatching = true
				if err := checkColumn(inCol, targetCol); err != nil {
					errors = multierror.Append(errors, err)
				}
			}
		}
		if !foundMatching {
			log.Printf("Missing column -- running alter table\n")
			alterSQL := fmt.Sprintf(`ALTER TABLE "%s"."%s" ADD COLUMN %s`,
				targetTable.Meta.Schema, targetTable.Name, getColumnSQL(inCol))
			columnOps = append(columnOps, alterSQL)
		}
	}
	return columnOps, errors
}

func checkColumn(inCol ColInfo, targetCol ColInfo) error {
	var errors error
	mismatchedTemplate := "mismatched column: %s property: %s, input: %v, target: %v"
	if inCol.Name != targetCol.Name {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "Name", inCol.Name, targetCol.Name))
	}
	if typeMapping[inCol.Type] != targetCol.Type {
		if strings.HasPrefix(typeMapping[inCol.Type], "character varying") && strings.HasPrefix(typeMapping[inCol.Type], "character varying") {
			// If they are both varchars but differing values, we will ignore this
		} else {
			errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "Type", typeMapping[inCol.Type], targetCol.Type))
		}
	}
	if inCol.DefaultVal != targetCol.DefaultVal {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "DefaultVal", inCol.DefaultVal, targetCol.DefaultVal))
	}
	if inCol.NotNull != targetCol.NotNull {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "NotNull", inCol.NotNull, targetCol.NotNull))
	}
	if inCol.PrimaryKey != targetCol.PrimaryKey {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "PrimaryKey", inCol.PrimaryKey, targetCol.PrimaryKey))
	}
	// for distkey & sortkey it's ok if the source doesn't have them, but they should at least not disagree
	if inCol.DistKey && inCol.DistKey != targetCol.DistKey {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "DistKey", inCol.DistKey, targetCol.DistKey))
	}
	if inCol.SortOrdinal != 0 && inCol.SortOrdinal != targetCol.SortOrdinal {
		errors = multierror.Append(errors, fmt.Errorf(mismatchedTemplate, inCol.Name, "SortOrdinal", inCol.SortOrdinal, targetCol.SortOrdinal))
	}
	return errors
}

// Copy copies either CSV or JSON data present in an S3 file into a redshift table.
// It also supports CSV or JSON data pointed at by a manifest file, if you pass in a manifest file.
// this is meant to be run in a transaction, so the first arg must be a sql.Tx
// if not using jsonPaths, set s3File.JSONPaths to "auto"
func (r *Redshift) Copy(tx *sql.Tx, f s3filepath.S3File, delimiter string, creds, gzip bool) error {
	var credSQL string
	if creds {
		credSQL = fmt.Sprintf(`CREDENTIALS 'aws_iam_role=%s'`, f.Bucket.RedshiftRoleARN)
	}
	gzipSQL := ""
	if gzip {
		gzipSQL = "GZIP"
	}
	manifestSQL := ""
	if f.Suffix == "manifest" {
		manifestSQL = "manifest"
	}

	// default to CSV
	jsonSQL := ""
	jsonPathsSQL := ""
	// always removequotes, UNLOAD should add quotes
	// always say escape for CSVs, UNLOAD should always escape
	delimSQL := fmt.Sprintf("DELIMITER AS '%s' REMOVEQUOTES ESCAPE TRIMBLANKS EMPTYASNULL ACCEPTANYDATE", delimiter)
	// figure out if we're doing JSON - no delim means JSON
	if delimiter == "" {
		jsonSQL = "JSON"
		jsonPathsSQL = "'auto'"
		delimSQL = ""
	}
	copySQL := fmt.Sprintf(`COPY "%s"."%s" FROM '%s' WITH %s %s %s REGION '%s' TIMEFORMAT 'auto' TRUNCATECOLUMNS STATUPDATE ON COMPUPDATE ON %s %s %s`,
		f.Schema, f.Table, f.GetDataFilename(), gzipSQL, jsonSQL, jsonPathsSQL, f.Bucket.Region, manifestSQL, credSQL, delimSQL)
	log.Printf("Running command: %s", copySQL)
	// can't use prepare b/c of redshift-specific syntax that postgres does not like
	_, err := tx.Exec(copySQL)
	return err
}

// Truncate deletes all items from a table, given a transaction, a schema string and a table name
// you should run vacuum and analyze soon after doing this for performance reasons
func (r *Redshift) Truncate(tx *sql.Tx, schema, table string) error {
	// We run 'DELETE FROM' instead of 'TRUNCATE' because 'TRUNCATE' can't be run in a transaction.
	// See http://docs.aws.amazon.com/redshift/latest/dg/r_TRUNCATE.html.
	truncStmt, err := tx.Prepare(fmt.Sprintf(`DELETE FROM "%s"."%s"`, schema, table))
	if err != nil {
		return err
	}
	_, err = truncStmt.Exec()
	return err
}

// TruncateInTimeRange deletes all items within a specific time range - that is,
// matching `dataDate` when rounded to a certain granularity `timeGranularity`
// NOTE: this assumes that "time" is a column in the table
func (r *Redshift) TruncateInTimeRange(tx *sql.Tx, schema, table, dataDateCol string,
	start, end time.Time) error {
	truncSQL := fmt.Sprintf(`
		DELETE FROM "%s"."%s"
		WHERE "%s" >= '%s' AND "%s" < '%s'
		`, schema, table, dataDateCol, start.Format("2006-01-02 15:04:05"),
		dataDateCol, end.Format("2006-01-02 15:04:05"))
	truncStmt, err := tx.Prepare(truncSQL)
	if err != nil {
		return err
	}

	log.Printf("Refreshing with the latest data. Running command: %s", truncSQL)
	_, err = truncStmt.Exec()
	return err
}
