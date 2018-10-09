package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/Clever/configure"
	discovery "github.com/Clever/discovery-go"
	"github.com/Clever/s3-to-redshift/logger"
	redshift "github.com/Clever/s3-to-redshift/redshift"
	s3filepath "github.com/Clever/s3-to-redshift/s3filepath"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/kardianos/osext"
	env "github.com/segmentio/go-env"
)

var (
	// things which will would strongly suggest launching as a second worker are env vars
	// also the secrets ... shhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh
	host            = os.Getenv("REDSHIFT_HOST")
	port            = os.Getenv("REDSHIFT_PORT")
	dbName          = env.MustGet("REDSHIFT_DB")
	user            = env.MustGet("REDSHIFT_USER")
	pwd             = env.MustGet("REDSHIFT_PASSWORD")
	redshiftRoleARN = env.MustGet("REDSHIFT_ROLE_ARN")
	vacuumWorker    = env.MustGet("VACUUM_WORKER")

	// payloadForSignalFx holds a subset of the job payload that
	// we want to alert on as a dimension in SignalFx.
	// This is necessary because we would like to selectively group
	// on job parameters - schema but not date, for instance, since
	// logging the date would overwhelm SignalFx
	payloadForSignalFx string

	gearmanAdminURL string
)

func init() {
	gearmanAdminUser := env.MustGet("GEARMAN_ADMIN_USER")
	gearmanAdminPass := env.MustGet("GEARMAN_ADMIN_PASS")
	gearmanAdminPath := env.MustGet("GEARMAN_ADMIN_PATH")
	gearmanAdminURL = generateServiceEndpoint(gearmanAdminUser, gearmanAdminPass, gearmanAdminPath)
}

func generateServiceEndpoint(user, pass, path string) string {
	hostPort, err := discovery.HostPort("gearman-admin", "http")
	if err != nil {
		log.Fatal(err)
	}
	proto, err := discovery.Proto("gearman-admin", "http")
	if err != nil {
		log.Fatal(err)
	}

	return fmt.Sprintf("%s://%s:%s@%s%s", proto, user, pass, hostPort, path)
}

func fatalIfErr(err error, msg string) {
	if err != nil {
		logger.JobFinishedEvent(payloadForSignalFx, false)
		panic(fmt.Sprintf("%s: %s", msg, err)) // TODO: kayvee
	}
}

// helper function used for verifying inputs
func getMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Rounds down a dateTime to a granularity
// For instance, 11:50AM will be truncated to 11:00AM
// if given a granularity of an hour
func truncateDate(date time.Time, granularity string) time.Time {
	switch granularity {
	case "hour":
		return date.Truncate(time.Hour)
	default:
		// Round down to day granularity by default
		return date.Truncate(24 * time.Hour)
	}
}

// Calculates whether or not input data (s3) is more stale than target data (Redshift)
// Expects:
// - inputDataDate corresponds to the s3 data timestamp of the job
// - targetDataDate is the maximum timestamp of the DB table
// - granularity indicating how often data snapshots are recorded in the target
func isInputDataStale(inputDataDate time.Time, targetDataDate *time.Time,
	granularity string, targetDataLoc *time.Location,
) bool {
	// If target table has no data, then input data is fresh by default
	if targetDataDate == nil {
		return false
	}

	// Handle comparison for target data in a different time zone (ex. PT)
	_, offsetSec := targetDataDate.In(targetDataLoc).Zone()
	offsetDuration, err := time.ParseDuration(fmt.Sprintf("%vs", -1*offsetSec))
	fatalIfErr(err, "isInputDataStale was unable to parse offset duration")

	*targetDataDate = targetDataDate.Add(offsetDuration)

	// We truncate the timestamps to make the comparison at the correct granularity
	// i.e. input data lagging by two hours is considered stale when granularity is hourly,
	// but it can still be considered fresh when the granularity is daily.
	return truncateDate(*targetDataDate, granularity).After(truncateDate(inputDataDate, granularity))
}

// getRegionForBucket looks up the region name for the given bucket
func getRegionForBucket(name string) (string, error) {
	// Any region will work for the region lookup, but the request MUST use
	// PathStyle
	config := aws.NewConfig().WithRegion("us-west-1").WithS3ForcePathStyle(true)
	session := session.New()
	client := s3.New(session, config)
	params := s3.GetBucketLocationInput{
		Bucket: aws.String(name),
	}
	resp, err := client.GetBucketLocation(&params)
	if err != nil {
		return "", fmt.Errorf("failed to get location for bucket '%s', %s", name, err)
	}
	if resp.LocationConstraint == nil {
		// "US Standard", returns an empty region. So return any region in the US
		return "us-east-1", nil
	}
	return *resp.LocationConstraint, nil
}

// in a transaction, truncate, create or update, and then copy from the s3 data file or manifest
// yell loudly if there is anything different in the target table compared to config (different distkey, etc)
func runCopy(
	db *redshift.Redshift, inputConf s3filepath.S3File, inputTable redshift.Table, targetTable *redshift.Table,
	truncate, gzip bool, delimiter, timeGranularity, targetTimeZone, streamStart, streamEnd string,
) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// TRUNCATE for dimension tables, but not fact tables
	if truncate && targetTable != nil {
		log.Println("truncating table!")
		if err := db.Truncate(tx, inputConf.Schema, inputTable.Name); err != nil {
			return fmt.Errorf("err running truncate table: %s", err)
		}
	}
	if targetTable == nil {
		if err := db.CreateTable(tx, inputTable); err != nil {
			return fmt.Errorf("err running create table: %s", err)
		}
	} else {
		var start, end time.Time
		var err error
		if timeGranularity == "stream" {
			start, err = time.Parse("2006-01-02T15:04:05", streamStart)
			if err != nil {
				return err
			}
			end, err = time.Parse("2006-01-02T15:04:05", streamEnd)
			if err != nil {
				return err
			}
		} else {
			start, end = startEndFromGranularity(inputConf.DataDate, timeGranularity, targetTimeZone)
		}
		// To prevent duplicates, clear away any existing data within a certain time range as the data date
		// (that is, sharing the same data date up to a certain time granularity)
		if err := db.TruncateInTimeRange(tx, inputConf.Schema, inputTable.Name, inputTable.Meta.DataDateColumn, start, end); err != nil {
			return fmt.Errorf("err truncating data for data refresh: %s", err)
		}

		if err := db.UpdateTable(tx, inputTable, *targetTable); err != nil {
			return fmt.Errorf("err running update table: %s", err)
		}
	}

	// COPY direct into it, ok to do since we're in a transaction
	// can't switch on file ending as manifest files b/c
	// manifest files obscure the underlying file types
	// instead just pass the delimiter along even if it's null
	if err := db.Copy(tx, inputConf, delimiter, true, gzip); err != nil {
		return fmt.Errorf("err running copy: %s", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("err committing transaction: %s", err)
	}

	if truncate {
		// If we've truncated the table we should run vacuum to clear out the old data
		// Only one vacuum can be run at a time, so we're going to throw this over the wall to
		// redshift-vacuum and use gearman-admin as a queueing service.
		if len(gearmanAdminURL) == 0 {
			log.Fatalf("Unable to post vacuum job to %s", vacuumWorker)
		} else {
			log.Println("Submitting job to Gearman admin")

			// N.B. We need to pass backslashes to escape the quotation marks as required
			// by Golang's os.Args for command line arguments
			payload, err := json.Marshal(map[string]string{
				"analyze": inputConf.Schema + `."` + inputTable.Name + `"`,
			})
			if err != nil {
				log.Fatalf("Error creating new payload: %s", err)
			}

			client := &http.Client{}
			endpoint := gearmanAdminURL + fmt.Sprintf("/%s", vacuumWorker)
			req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payload))
			if err != nil {
				log.Fatalf("Error creating new request: %s", err)
			}
			req.Header.Add("Content-Type", "text/plain")
			_, err = client.Do(req)
			if err != nil {
				log.Fatalf("Error submitting job: %s", err)
			}
		}
	}
	return nil
}

func startEndFromGranularity(t time.Time, granularity string, targetTimezone string) (time.Time, time.Time) {
	// Rotate time if in PT
	log.Print(targetTimezone)
	if targetTimezone != "UTC" {
		ptLoc, err := time.LoadLocation(targetTimezone)
		fatalIfErr(err, "startEndFromGranularity was unable to load timezone")

		_, ptOffsetSec := t.In(ptLoc).Zone()
		ptOffsetDuration, err := time.ParseDuration(fmt.Sprintf("%vs", ptOffsetSec))
		fatalIfErr(err, "startEndFromGranularity was unable to parse offset duration")

		t = t.Add(ptOffsetDuration)
	}

	var duration time.Duration
	if granularity == "day" {
		duration = time.Hour * 24
	} else {
		duration = time.Hour
	}

	start := t.UTC().Truncate(duration)
	end := start.Add(duration)
	return start, end
}

// This worker finds the latest file in s3 and uploads it to redshift
// If the destination table does not exist, the worker creates it
// If the destination table lacks columns, the worker creates those as well
// The worker also uses a column in the data to figure out whether the s3 data is
// newer than what already exists.
func main() {
	dir, err := osext.ExecutableFolder()
	if err != nil {
		log.Fatal(err)
	}
	err = logger.SetGlobalRouting(path.Join(dir, "kvconfig.yml"))
	if err != nil {
		log.Fatal(err)
	}

	flags := struct {
		InputSchemaName string `config:"schema"`
		InputTables     string `config:"tables"`
		InputBucket     string `config:"bucket"`
		Truncate        bool   `config:"truncate"`
		Force           bool   `config:"force"`
		DataDate        string `config:"date,required"`
		ConfigFile      string `config:"config"`
		GZip            bool   `config:"gzip"`
		Delimiter       string `config:"delimiter"`
		TimeGranularity string `config:"granularity,required"`
		StreamStart     string `config:"streamStart"`
		StreamEnd       string `config:"streamEnd"`
		TargetTimezone  string `config:"timezone"`
	}{ // Specifying defaults:
		InputSchemaName: "mongo",
		InputTables:     "",
		InputBucket:     "metrics",
		Truncate:        false,
		Force:           false,
		DataDate:        "",
		ConfigFile:      "",
		GZip:            true,
		Delimiter:       "",
		TimeGranularity: "day",
		StreamStart:     "",
		StreamEnd:       "",
		TargetTimezone:  "UTC",
	}
	if err := configure.Configure(&flags); err != nil {
		log.Fatalf("err: %#v", err)
	}

	payloadForSignalFx = fmt.Sprintf("--schema %s", flags.InputSchemaName)
	defer logger.JobFinishedEvent(payloadForSignalFx, true)

	if flags.DataDate == "" {
		logger.JobFinishedEvent(payloadForSignalFx, false)
		panic("No date provided")
	}

	// verify that timeGranularity is a supported value. for convenience,
	// we use the convention that granularities must be valid PostgreSQL dateparts
	// (see: http://www.postgresql.org/docs/8.1/static/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC)
	supportedGranularities := map[string]bool{"hour": true, "day": true, "stream": true}
	if !supportedGranularities[flags.TimeGranularity] {
		logger.JobFinishedEvent(payloadForSignalFx, false)
		panic(fmt.Sprintf("Unsupported granularity, must be one of %v", getMapKeys(supportedGranularities)))
	}

	// verify that targetTimezone is a supported Golang location (i.e. "America/Los_Angeles")
	targetDataLocation, err := time.LoadLocation(flags.TargetTimezone)
	fatalIfErr(err, fmt.Sprintf("unable to load timezone '%s'", flags.TargetTimezone))

	awsRegion, locationErr := getRegionForBucket(flags.InputBucket)
	fatalIfErr(locationErr, "error getting location for bucket "+flags.InputBucket)

	// use an custom bucket type for testablitity
	bucket := s3filepath.S3Bucket{
		Name:            flags.InputBucket,
		Region:          awsRegion,
		RedshiftRoleARN: redshiftRoleARN}

	timeout := 60 // can parameterize later if this is an issue
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5439"
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Signal(syscall.SIGTERM))
	go func() {
		for range c {
			// sfncli will send signals to our container
			// we should gracefully terminate any running SQL queries
			cancel()
		}
	}()

	db, err := redshift.NewRedshift(ctx, host, port, dbName, user, pwd, timeout)
	fatalIfErr(err, "error getting redshift instance")

	var copyErrors error
	// for each table passed in - likely we could goroutine this out
	for _, t := range strings.Split(flags.InputTables, ",") {
		log.Printf("attempting to run on schema: %s table: %s", flags.InputSchemaName, t)
		// override most recent data file
		parsedInputDate, err := time.Parse(time.RFC3339, flags.DataDate)
		fatalIfErr(err, fmt.Sprintf("issue parsing date: %s", flags.DataDate))
		inputConf, err := s3filepath.CreateS3File(s3filepath.S3PathChecker{}, bucket, flags.InputSchemaName, t, flags.ConfigFile, parsedInputDate)
		fatalIfErr(err, "Issue getting data file from s3")
		inputTable, err := db.GetTableFromConf(*inputConf) // allow passing explicit config later
		fatalIfErr(err, "Issue getting table from input")

		// figure out what the current state of the table is to determine if the table is already up to date
		targetTable, targetDataDate, err := db.GetTableMetadata(inputConf.Schema, inputConf.Table, inputTable.Meta.DataDateColumn)
		if err != nil {
			fatalIfErr(err, "Error getting existing latest table metadata") // use fatalIfErr to stay the same
		}

		// unless --force, don't update unless input data is new
		if flags.TimeGranularity != "stream" && isInputDataStale(parsedInputDate, targetDataDate, flags.TimeGranularity, targetDataLocation) {
			if flags.Force == false {
				log.Printf("Recent data already exists in db: %s", *targetDataDate)
				continue
			}
			log.Printf("Forcing update of inputTable: %s", inputConf.Table)
		}

		if err := runCopy(
			db, *inputConf, *inputTable, targetTable, flags.Truncate, flags.GZip, flags.Delimiter,
			flags.TimeGranularity, flags.TargetTimezone, flags.StreamStart, flags.StreamEnd,
		); err != nil {
			log.Printf("error running copy for table %s: %s", t, err)
			copyErrors = multierror.Append(copyErrors, err)
		} else {
			// DON'T NEED TO CREATE VIEWS - will be handled by the refresh script
			log.Printf("done with table: %s.%s", inputConf.Schema, t)
		}
	}
	if copyErrors != nil {
		log.Fatalf("error loading tables: %s", copyErrors)
	}
	if flags.InputTables == "managed_login_facts_vw_stream" {
		job := map[string]string{
			"granularity": "day",
			"input":       "paid_active_schools_vw",
			"schema":      "managed",
		}
		payloadForNextJob, err := json.Marshal(job)
		if err != nil {
			log.Fatalf("error creating new payload: %s", err)
		}
		fmt.Println(string(payloadForNextJob))
	} else if flags.InputTables == "helper_district_active_users_day" {
		job := map[string]string{
			"dest": "district_vw",
			"src":  "managed.district_vw",
		}
		payloadForNextJob, err := json.Marshal(job)
		if err != nil {
			log.Fatalf("error creating new payload: %s", err)
		}
		fmt.Println(string(payloadForNextJob))
	} else {
		log.Println("done with full run")
	}
}
