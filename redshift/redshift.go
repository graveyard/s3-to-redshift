package redshift

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/lib/pq" // Postgres driver.

	"github.com/Clever/redshifter/s3filepath"
)

type dbExecCloser interface {
	Close() error
	Begin() (*sql.Tx, error)
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
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

var ()

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
	// can't use prepare b/c of redshift-specific syntax that postgres does not like
	_, err := tx.Exec(fullCopySQL)
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

// VacuumAnalyze performs VACUUM FULL; ANALYZE on the redshift database. This is useful for
// recreating the indices after a database has been modified and updating the query planner.
func (r *Redshift) VacuumAnalyze() error {
	log.Printf("Executing 'VACCUM FULL; ANALYZE'")
	_, err := r.Exec("VACUUM FULL; ANALYZE")
	return err
}
