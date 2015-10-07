package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	//"errors"
	//"github.com/davecgh/go-spew/spew"
	"strings"
	"time"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	env "github.com/segmentio/go-env"

	redshift "github.com/Clever/redshifter/redshift"
	s3filepath "github.com/Clever/redshifter/s3filepath"
)

var (
	// things we are likely to change when running the worker normally are flags
	date            = flag.String("date", "", "what data date we process, must be full RFC3339 TODO")
	truncate        = flag.Bool("truncate", false, "do we truncate the table before inserting")
	force           = flag.Bool("force", false, "do we refresh the data even if it's already handled?")
	inputSchemaName = flag.String("schema", "mongo", "what schema we process")
	inputTables     = flag.String("tables", "", "tables to run on, comma separated")
	s3Prefix        = flag.String("s3prefix", "metrics", "bucket to load into, including s3:// protocol")
	configFile      = flag.String("config", "", "schema & table config to use in YAML format")
	// things which will would strongly suggest launching as a second worker are env vars
	// also the secrets ... shhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh
	host               = os.Getenv("REDSHIFT_HOST")
	port               = os.Getenv("REDSHIFT_PORT")
	dbName             = env.MustGet("REDSHIFT_DB")
	user               = env.MustGet("REDSHIFT_USER")
	pwd                = env.MustGet("REDSHIFT_PASSWORD")
	awsRegion          = env.MustGet("AWS_REGION")
	awsAccessKeyID     = env.MustGet("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey = env.MustGet("AWS_SECRET_ACCESS_KEY")
)

func fatalIfErr(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", msg, err)) // TODO: kayvee
	}
}

// in a transaction, truncate, create or update, and then copy from the s3 JSON file
// yell loudly if there is anything different in the target table compared to config (different distkey, etc)
func runCopy(db *redshift.Redshift, inputConf s3filepath.S3File, inputTable, targetTable redshift.Table, truncate bool) error {
	// determine if target table exists
	// can't compare to empty struct b/c of list
	createTable := false
	if targetTable.Name == "" {
		createTable = true
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// TRUNCATE for dimension tables, but not fact tables
	if truncate && !createTable {
		log.Println("truncating table!")
		if err = db.RunTruncate(tx, inputConf.Schema, inputTable.Name); err != nil {
			return fmt.Errorf("err running truncate table: %s", err)
		}
	}
	if createTable {
		if err = db.RunCreateTable(tx, inputTable); err != nil {
			return fmt.Errorf("err running create table: %s", err)
		}
	} else {
		if err = db.RunUpdateTable(tx, targetTable, inputTable); err != nil {
			return fmt.Errorf("err running update table: %s", err)
		}
	}

	// COPY direct into it, ok to do since we're in a transaction
	// assuming that we always want to copy from s3 and have gzip, so last 2 params are true
	// if we want to change that, we should figure this out from the filename
	if err = db.RunJSONCopy(tx, inputConf, true, true); err != nil {
		return fmt.Errorf("err running JSON copy: %s", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("err committing transaction: %s", err)
	}
	return nil
}

func main() {
	flag.Parse()

	// connect to s3
	region, ok := aws.Regions[awsRegion]
	if !ok {
		fatalIfErr(fmt.Errorf("issue converting region: %s in to aws region"), "")
	}
	awsAuth, err := aws.EnvAuth()
	fatalIfErr(err, "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables must be set.")
	s3Conn := s3.New(awsAuth, region)

	//timeout := 10 // can parameterize later if this is an issue
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5439"
	}
	db, err := redshift.NewRedshift(host, port, dbName, user, pwd, 10)
	fatalIfErr(err, "error getting redshift instance")

	// for each table passed in - likely we could goroutine this out
	for _, t := range strings.Split(*inputTables, ",") {
		log.Printf("attempting to run on schema: %s table: %s", *inputSchemaName, t)
		// find most recent s3 file
		dataDate := time.Now().Round(time.Hour) // default to now
		if *date != "" {
			dataDate, err = time.Parse(time.RFC3339, *date)
			fatalIfErr(err, fmt.Sprintf("issue parsing date: %s", *date))
		}
		// each input will have a configuration associated with it, output by the previous worker
		// TODO: allow a passed-in parameter so that we don't necessarily have to write a config for each piece of data
		inputConf, err := s3filepath.FindLatestInputData(s3Conn, *s3Prefix, *inputSchemaName, t, *configFile, dataDate.UTC())
		fatalIfErr(err, "Issue getting latest schema and input data from s3")

		inputTable, err := db.GetTableFromConf(inputConf) // allow passing explicit config later
		fatalIfErr(err, "Issue getting table from input")

		// VERIFICATION SECTION BEGIN
		targetTable, lastTargetData, err := db.GetTableMetadata(inputConf.Schema, inputConf.Table, inputTable.Meta.DataDateColumn)
		//spew.Dump(lastTargetData)
		fatalIfErr(err, "Error getting existing latest table metadata")

		// unless --force, don't update unless input data is new
		if !inputConf.DataDate.After(lastTargetData) {
			if *force != true {
				log.Printf("Recent data already exists in db: %s", lastTargetData)
				return
			}
			log.Printf("Forcing update of inputTable: %s", inputConf.Table)
		}

		fatalIfErr(runCopy(db, inputConf, inputTable, targetTable, *truncate), "Issue running copy")
		// DON'T NEED TO CREATE VIEWS - will be handled by the refresh script
		log.Printf("done with table: %s.%s", inputConf.Schema, t)
	}
	log.Println("done with full run")
}
