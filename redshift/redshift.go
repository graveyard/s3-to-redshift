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
	log.Print("Executing Redshift command: ", cmd)
	return r.Exec(cmd)
}

// CopyJSONDataFromS3 copies JSON data present in an S3 file into a redshift table.
func (r *Redshift) CopyJSONDataFromS3(schema, table, file, jsonpathsFile string) error {
	creds := fmt.Sprintf(`CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`, r.s3Info.AccessID, r.s3Info.SecretKey)
	copyCmd := fmt.Sprintf(
		`COPY "%s"."%s" FROM '%s' WITH json '%s' region '%s' timeformat 'epochsecs' COMPUPDATE ON %s`,
		schema, table, file, jsonpathsFile, r.s3Info.Region, creds,
	)
	_, err := r.logAndExec(copyCmd)
	return err
}

// CopyGzipCsvDataFromS3 copies gzipped CSV data from an S3 file into a redshift table.
func (r *Redshift) CopyGzipCsvDataFromS3(schema, table, file string, ts postgres.TableSchema, delimiter rune) error {
	cols := []string{}
	sort.Sort(ts)
	for _, ci := range ts {
		cols = append(cols, ci.Name)
	}
	copyCmd := fmt.Sprintf(
		`COPY "%s"."%s" (%s) FROM '%s' WITH REGION '%s' GZIP CSV DELIMITER '%c'`,
		schema, table, strings.Join(cols, ", "), file, r.s3Info.Region, delimiter)
	opts := " IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON"
	creds := fmt.Sprintf(` CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`, r.s3Info.AccessID, r.s3Info.SecretKey)
	fullCopyCmd := fmt.Sprintf("%s%s%s", copyCmd, opts, creds)
	_, err := r.logAndExec(fullCopyCmd)
	return err
}

// Creates a table in tmpschema using the structure of the existing table in schema.
func (r *Redshift) createTempTable(tmpschema, schema, name string) error {
	cmd := fmt.Sprintf(`CREATE TABLE "%s"."%s" (LIKE "%s"."%s")`, tmpschema, name, schema, name)
	_, err := r.logAndExec(cmd)
	return err
}

// refreshData atomically clears out a table and loads in the contents of a table in the temp schema
func (r *Redshift) refreshData(tmpschema, schema, name string) error {
	cmds := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM "%s"."%s"`, schema, name),
		fmt.Sprintf(`INSERT INTO "%s"."%s" (SELECT * FROM "%s"."%s")`, schema, name, tmpschema, name),
		"END TRANSACTION",
	}
	_, err := r.logAndExec(strings.Join(cmds, "; "))
	return err
}

// RefreshTable refreshes a single table by first copying gzipped CSV data into a temporary table
// and later replacing the original table's data with the one from the temporary table in an
// atomic operation.
func (r *Redshift) refreshTable(schema, name, tmpschema, file string, ts postgres.TableSchema, delim rune) error {
	if err := r.createTempTable(tmpschema, schema, name); err != nil {
		return err
	}
	if err := r.CopyGzipCsvDataFromS3(tmpschema, name, file, ts, delim); err != nil {
		return err
	}
	if err := r.refreshData(tmpschema, schema, name); err != nil {
		return err
	}
	return r.VacuumAnalyzeTable(schema, name)
}

// RefreshTables refreshes multiple tables in parallel and returns an error if any of the copies
// fail.
func (r *Redshift) RefreshTables(
	tables map[string]postgres.TableSchema, schema, tmpschema, s3prefix string, delim rune) error {
	if _, err := r.logAndExec(fmt.Sprintf(`CREATE SCHEMA "%s"`, tmpschema)); err != nil {
		return err
	}
	group := new(errgroup.Group)
	for name, ts := range tables {
		group.Add(1)
		go func(name string, ts postgres.TableSchema) {
			if err := r.refreshTable(schema, name, tmpschema, postgres.S3Filename(s3prefix, name), ts, delim); err != nil {
				group.Error(err)
			}
			group.Done()
		}(name, ts)
	}
	errs := new(errgroup.Group)
	if err := group.Wait(); err != nil {
		errs.Error(err)
	}
	if _, err := r.logAndExec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, tmpschema)); err != nil {
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
