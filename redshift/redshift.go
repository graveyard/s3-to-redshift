package redshift

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/Clever/redshifter/postgres"
	"github.com/facebookgo/errgroup"
)

type dbExecCloser interface {
	Close() error
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
	s3Info S3Info
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
	DataDateColumn string `yaml:"data_date_column"`
	Schema         string `yaml:"schema"`
}

// ColInfo is a struct that contains information about a column in a Redshift database.
// SortKey and DistKey only make sense for Redshift
type ColInfo struct {
	Ordinal    int    `yaml:"ordinal"`
	Name       string `yaml:"dest"`
	Type       string `yaml:"type"`
	DefaultVal string `yaml:"defaultval"`
	NotNull    bool   `yaml:"notnull"`
	PrimaryKey bool   `yaml:"primarykey"`
	DistKey    bool   `yaml:"distkey"`
	SortOrd    int    `yaml:"sortord"`
}

// Just a helper to make sure the CSV copy works properly
type SortableColumns []ColInfo

func (c SortableColumns) Len() int           { return len(c) }
func (c SortableColumns) Less(i, j int) bool { return c[i].Ordinal < c[j].Ordinal }
func (c SortableColumns) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

var (
)

// NewRedshift returns a pointer to a new redshift object using configuration values passed in
// on instantiation and the AWS env vars we assume exist
// Don't need to pass s3 info unless doing a COPY operation
func NewRedshift(host, port, db, user, password string, timeout int, s3Info S3Info) (*Redshift, error) {
	flag.Parse()
	source := fmt.Sprintf("host=%s port=%d dbname=%s connect_timeout=%d", host, port, db, timeout)
	log.Println("Connecting to Redshift Source: ", source)
	source += fmt.Sprintf(" user=%s password=%s", user, password)
	sqldb, err := sql.Open("postgres", source)
	if err != nil {
		return nil, err
	}
	return &Redshift{sqldb, s3Info}, nil
}

func (r *Redshift) logAndExec(cmd string) (sql.Result, error) {
	log.Print("Executing Redshift command: ", cmd) // TODO: filter out creds, perhaps
	return r.Exec(cmd)
}

// GetJSONCopySQL copies JSON data present in an S3 file into a redshift table.
// if not using jsonPaths, set to "auto"
func (r *Redshift) GetJSONCopySQL(schema, table, filename, jsonPaths string, creds, gzip bool) string {
	credSQL := ""
	if creds {
		credSQL = fmt.Sprintf(`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`, r.s3Info.AccessID, r.s3Info.SecretKey)
	}
	gzipSQL := ""
	if gzip {
		gzipSQL = "GZIP"
	}
	copyCmd := fmt.Sprintf(
		`COPY "%s"."%s" FROM '%s' WITH %s JSON '%s' REGION '%s' TIMEFORMAT 'auto' COMPUPDATE ON %s`,
		schema, table, filename, gzipSQL, jsonPaths, r.s3Info.Region, credSQL)
	return copyCmd
}

// GetCSVCopySQL copies gzipped CSV data from an S3 file into a redshift table.
func (r *Redshift) GetCSVCopySQL(schema, table, file string, ts Table, delimiter rune, creds, gzip bool) string {
	credSQL := ""
	if creds {
		credSQL = fmt.Sprintf(`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`, r.s3Info.AccessID, r.s3Info.SecretKey)
	}
	gzipSQL := ""
	if gzip {
		gzipSQL = "GZIP"
	}

	cols := SortableColumns{}
	cols = append(cols, ts.Columns...)
	sort.Sort(cols)
	colStrings := []string{}
	for _, ci := range cols {
		colStrings = append(colStrings, ci.Name)
	}
	copyCmd := fmt.Sprintf(
		`COPY "%s"."%s" (%s) FROM '%s' WITH REGION '%s' %s CSV DELIMITER '%c'`,
		schema, table, strings.Join(colStrings, ", "), file, r.s3Info.Region, gzipSQL, delimiter)
	opts := "IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON"
	fullCopyCmd := fmt.Sprintf("%s %s %s", copyCmd, opts, credSQL)
	return fullCopyCmd
}

// SafeExec allows execution of SQL in a transaction block
// While it seems a little dangerous to export such a powerful function, it is very difficult
// to control execution control and actually effectively use this library without this ability
func (r *Redshift) SafeExec(sqlIn []string) error {
	cmds := append([]string{"BEGIN TRANSACTION"}, sqlIn...)
	cmds = append(cmds, "END TRANSACTION")
	_, err := r.logAndExec(strings.Join(cmds, "; "))
	return err
}

// GetTruncateSQL simply returns SQL that deletes all items from a table, given a schema string and a table name
func (r *Redshift) GetTruncateSQL(schema, table string) string {
	return fmt.Sprintf(`DELETE FROM "%s"."%s"`, schema, table)
}

// RefreshTable refreshes a single table by copying gzipped CSV data into a temporary table
// and later replacing the original table's data with the one from the temporary table in an
// atomic operation.
func (r *Redshift) refreshTable(schema, name, file string, ts Table, delim rune) error {
	truncateSQL := r.GetTruncateSQL(schema, name)
	copySQL := r.GetCSVCopySQL(schema, name, file, ts, delim, true, true)
	err := r.SafeExec([]string{truncateSQL, copySQL})
	if err != nil {
		return err
	}
	return r.VacuumAnalyzeTable(schema, name)
}

// RefreshTables refreshes multiple tables in parallel and returns an error if any of the copies
// fail.
func (r *Redshift) RefreshTables(
	tables map[string]Table, schema, s3prefix string, delim rune) error {
	group := new(errgroup.Group)
	for name, ts := range tables {
		group.Add(1)
		go func(name string, ts Table) {
			if err := r.refreshTable(schema, name, postgres.S3Filename(s3prefix, name), ts, delim); err != nil {
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
