package redshift

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/clever/redshifter/postgres"
	"github.com/facebookgo/errgroup"
	"github.com/segmentio/go-env"
)

type DBExecCloser interface {
	Close() error
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Redshift struct {
	DBExecCloser
	AccessId  string
	SecretKey string
}

var (
	// TODO: include flag validation
	awsAccessKeyId     = env.MustGet("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey = env.MustGet("AWS_SECRET_ACCESS_KEY")
	host               = flag.String("redshifthost", "", "Address of the redshift host")
	port               = flag.Int("redshiftport", 0, "Address of the redshift host")
	db                 = flag.String("redshiftdatabase", "", "Redshift database to connect to")
	user               = flag.String("redshiftuser", "", "Redshift user to connect as")
	pwd                = flag.String("redshiftpassword", "", "Password for the redshift user")
	timeout            = flag.Duration("redshiftconnecttimeout", 10*time.Second,
		"Timeout while connecting to Redshift. Defaults to 10 seconds.")
	tmpprefix = flag.String("tmptableprefix", "tmp_refresh_table_",
		"Prefix for temporary tables to ensure they don't collide with existing ones.")
)

func NewRedshift() (*Redshift, error) {
	flag.Parse()
	source := fmt.Sprintf("host=%s port=%d dbname=%s connect_timeout=%d", *host, *port, *db, int(timeout.Seconds()))
	log.Println("Connecting to Reshift Source: ", source)
	source += fmt.Sprintf(" user=%s password=%s", *user, *pwd)
	sqldb, err := sql.Open("postgres", source)
	if err != nil {
		return nil, err
	}
	return &Redshift{sqldb, awsAccessKeyId, awsSecretAccessKey}, nil
}

func (r *Redshift) LogAndExec(cmd string, creds bool) (sql.Result, error) {
	log.Print("Executing Redshift command: ", cmd)
	if creds {
		cmd += fmt.Sprintf(" CREDENTIALS '%s=%s;%s=%s'", "aws_access_key_id", r.AccessId, "aws_secret_access_key", r.SecretKey)
	}
	return r.Exec(cmd)
}

func (r *Redshift) CopyJsonDataFromS3(table, file, jsonpathsFile, awsRegion string) error {
	copyCmd := fmt.Sprint(
		"COPY ", table, " FROM '", file, "' WITH",
		" json '", jsonpathsFile,
		"' region '", awsRegion,
		"' timeformat 'epochsecs'",
		" COMPUPDATE ON")
	_, err := r.LogAndExec(copyCmd, true)
	return err
}

func (r *Redshift) CopyGzipCsvDataFromS3(table, file, awsRegion string, delimiter rune) error {
	copyCmd := fmt.Sprint(
		"COPY ", table, " FROM '", file, "' WITH ",
		"REGION '", awsRegion, "'",
		" GZIP CSV DELIMITER '", string(delimiter), "'",
		" IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON")
	_, err := r.LogAndExec(copyCmd, true)
	return err
}

// TODO: explore adding distkey.
func columnString(c *postgres.ColInfo) string {
	attributes := []string{}
	constraints := []string{}
	if c.DefaultVal != "" {
		attributes = append(attributes, "DEFAULT "+c.DefaultVal)
	}
	if c.PrimaryKey {
		attributes = append(attributes, "SORTKEY")
		constraints = append(constraints, "PRIMARY KEY")
	}
	if c.NotNull {
		constraints = append(constraints, "NOT NULL")
	}
	return fmt.Sprintf("%s %s %s %s", c.Name, c.ColType, strings.Join(attributes, " "), strings.Join(constraints, " "))
}

func (r *Redshift) CreateTable(name string, ts postgres.TableSchema) error {
	colStrings := []string{}
	sort.Sort(ts)
	for _, ci := range ts {
		colStrings = append(colStrings, columnString(ci))
	}
	cmd := fmt.Sprintf("CREATE TABLE %s (%s)", name, strings.Join(colStrings, ", "))
	_, err := r.LogAndExec(cmd, false)
	return err
}

func (r *Redshift) RefreshTable(name, prefix, file, awsRegion string, ts postgres.TableSchema, delim rune) error {
	tmptable := prefix + name
	if _, err := r.LogAndExec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmptable), false); err != nil {
		return err
	}
	if err := r.CreateTable(tmptable, ts); err != nil {
		return err
	}
	if err := r.CopyGzipCsvDataFromS3(tmptable, file, awsRegion, delim); err != nil {
		return err
	}
	_, err := r.LogAndExec(fmt.Sprintf("DROP TABLE %s; ALTER TABLE %s RENAME TO %s;", name, tmptable, name), false)
	return err
}

func (r *Redshift) RefreshTables(
	tables map[string]postgres.TableSchema, s3prefix, awsRegion string, delim rune) error {
	group := new(errgroup.Group)
	for name, ts := range tables {
		group.Add(1)
		go func(name string, ts postgres.TableSchema) {
			if err := r.RefreshTable(name, *tmpprefix, postgres.S3Filename(s3prefix, name), awsRegion, ts, delim); err != nil {
				group.Error(err)
			}
			group.Done()
		}(name, ts)
	}
	return group.Wait()
}

func (r *Redshift) VacuumAnalyze() error {
	_, err := r.LogAndExec("VACUUM FULL; ANALYZE", false)
	return err
}
