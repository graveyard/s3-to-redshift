package redshift

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/Clever/pathio"

	"github.com/Clever/redshifter/s3filepath"
	"github.com/facebookgo/errgroup"
)

type dbExecCloser interface {
	Close() error
	Begin() (*sql.Tx, error)
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// S3Info holds the information necessary to copy data from s3 buckets
type S3Info struct {
	Region    string
	AccessID  string
	SecretKey string
}

// Redshift wraps a dbExecCloser and can be used to perform operations on a redshift database.
type Redshift struct {
	dbExecCloser
}

// Table is our representation of a Redshift table
// the main difference is an added metadata section and YAML unmarshalling guidance
type Table struct {
	Name    string    `yaml:"dest"`
	Columns []ColInfo `yaml:"columns"`
	Meta    Meta      `yaml:"meta"`
}

// Meta holds information that might be not in Redshift or annoying to access
// in this case, we want to know the schema a table is part of
// and the column which corresponds to the timestamp at which the data was gathered
// NOTE: this will be useful for the s3-to-redshift worker, but is currently not very useful
// same with the yaml info
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
		"timestamp": "timestamp",              // timestamp with timezone is not supported in redshift
		"text":      "character varying(256)", // unfortunately redshift turns text -> varchar 256
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
	source := fmt.Sprintf("host=%s port=%d dbname=%s connect_timeout=%d", host, port, db, timeout)
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

// it's a little awkward to turn the moSQL format into what I want,
// this belongs here - redshift should not have to know about s3 files really
func (r *Redshift) GetTableFromConf(f s3filepath.S3File) (Table, error) {
	var tempSchema map[string]Table
	var emptyTable Table

	confFile := f.GetConfigFilename()
	log.Printf("Parsing file: %s", confFile)
	reader, err := pathio.Reader(confFile)
	if err != nil {
		return emptyTable, fmt.Errorf("error opening conf file: %s", err)
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return emptyTable, err
	}
	if err := yaml.Unmarshal(data, &tempSchema); err != nil {
		return emptyTable, fmt.Errorf("Warning: could not parse file %s, err: %s\n", confFile, err)
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

// RunJSONCopy copies JSON data present in an S3 file into a redshift table.
// this is meant to be run in a transaction, so the first arg must be a sql.Tx
// if not using jsonPaths, set to "auto"
func (r *Redshift) RunJSONCopy(tx *sql.Tx, schema, table, filename, jsonPaths string, creds, gzip bool) error {
	var credSQL string
	var credArgs []interface{}
	if creds {
		credSQL = `CREDENTIALS 'aws_access_key_id=?;aws_secret_access_key=?'`
		credArgs = []interface{}{r.s3Info.AccessID, r.s3Info.SecretKey}
	}
	gzipSQL := ""
	if gzip {
		gzipSQL = "GZIP"
	}
	copySQL := `COPY "?"."?" FROM '?' WITH ? JSON '?' REGION '?' TIMEFORMAT 'auto' STATUPDATE ON COMPUPDATE ON %s`
	copyStmt, err := r.Prepare(fmt.Sprintf(copySQL, credSQL))
	if err != nil {
		return err
	}
	args := []interface{}{schema, table, filename, gzipSQL, jsonPaths, r.s3Info.Region}
	log.Printf("Running command: %s with args: %v", copySQL, args)
	_, err = tx.Stmt(copyStmt).Exec(append(args, credArgs...)...)
	return err
}

// RunCSVCopy copies gzipped CSV data from an S3 file into a redshift table
// this is meant to be run in a transaction, so the first arg must be a sql.Tx
func (r *Redshift) RunCSVCopy(tx *sql.Tx, schema, table, file string, ts Table, delimiter rune, creds, gzip bool) error {
	var credSQL string
	var credArgs []interface{}
	if creds {
		credSQL = `CREDENTIALS 'aws_access_key_id=?;aws_secret_access_key=?'`
		credArgs = []interface{}{r.s3Info.AccessID, r.s3Info.SecretKey}
	}
	gzipSQL := ""
	if gzip {
		gzipSQL = "GZIP"
	}

	cols := sortableColumns{}
	cols = append(cols, ts.Columns...)
	sort.Sort(cols)
	colStrings := []string{}
	for _, ci := range cols {
		colStrings = append(colStrings, ci.Name)
	}

	copySQL := fmt.Sprintf(`COPY "?"."?" (?) FROM '?' WITH REGION '?' ? CSV DELIMITER '?'`)
	opts := "IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE STATUPDATE ON COMPUPDATE ON"
	fullCopySQL := fmt.Sprintf("%s %s %s", copySQL, opts, credSQL)
	copyStmt, err := r.Prepare(fullCopySQL)
	if err != nil {
		return err
	}
	args := []interface{}{schema, table, strings.Join(colStrings, ", "), file, r.s3Info.Region, gzipSQL, delimiter}
	log.Printf("Running command: %s with args: %v", fullCopySQL, args)
	_, err = tx.Stmt(copyStmt).Exec(append(args, credArgs...)...)
	return err
}

// RunTruncate deletes all items from a table, given a transaction, a schema string and a table name
// you shuold run vacuum and analyze soon after doing this for performance reasons
func (r *Redshift) RunTruncate(tx *sql.Tx, schema, table string) error {
	truncStmt, err := r.Prepare(`DELETE FROM "?"."?"`)
	if err != nil {
		return err
	}
	_, err = tx.Stmt(truncStmt).Exec(schema, table)
	return err
}

// RefreshTable refreshes a single table by truncating it and COPY-ing gzipped CSV data into it
// This is done within a transaction for safety
func (r *Redshift) refreshTable(schema, name, file string, ts Table, delim rune) error {
	tx, err := r.Begin()
	if err != nil {
		return err
	}
	if err = r.RunTruncate(tx, schema, name); err != nil {
		tx.Rollback()
		return err
	}
	if err = r.RunCSVCopy(tx, schema, name, file, ts, delim, true, true); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// RefreshTables refreshes multiple tables in parallel and returns an error if any of the copies
// fail.
func (r *Redshift) RefreshTables(
	tables map[string]Table, awsRegion, awsAccessID, awsSecretKey, bucket, schema string, delim rune) error {
	group := new(errgroup.Group)
	for name, ts := range tables {
		group.Add(1)
		go func(name string, ts Table) {
			file := fmt.Sprintf("s3://%s/%s.txt.gz", bucket, name) // taken from postgres library
			s3File := s3filepath.S3File{awsRegion, awsAccessID, awsSecretKey, bucket, schema, name, file, "", delim, time.Time{}}
			if err := r.refreshTable(s3File, ts, delim); err != nil {
				group.Error(err)
			}
			group.Done()
		}(name, ts)
	}
	errs := new(errgroup.Group)
	if err := group.Wait(); err != nil {
		errs.Error(err)
	}
	// Use errs.Wait() to group the two errors into a single error object.
	return errs.Wait()
}

// VacuumAnalyze performs VACUUM FULL; ANALYZE on the redshift database. This is useful for
// recreating the indices after a database has been modified and updating the query planner.
func (r *Redshift) VacuumAnalyze() error {
	_, err := r.logAndExec("VACUUM FULL; ANALYZE")
	return err
}

// VacuumAnalyzeTable performs VACUUM FULL; ANALYZE on a specific table. This is useful for
// recreating the indices after a database has been modified and updating the query planner.
func (r *Redshift) VacuumAnalyzeTable(schema, table string) error {
	_, err := r.logAndExec(fmt.Sprintf(`VACUUM FULL "%s"."%s"; ANALYZE "%s"."%s"`, schema, table, schema, table))
	return err
}
