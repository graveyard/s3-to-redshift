package main

import (
	"flag"
	"log"
	"strings"

	"github.com/clever/redshifter/postgres"
	"github.com/clever/redshifter/redshift"
	"github.com/segmentio/go-env"
)

var (
	// TODO: include flag validation
	awsRegion  = env.MustGet("AWS_REGION")
	s3prefix   = flag.String("s3prefix", "", "s3 path to be used as a prefix for temporary storage of postgres data")
	tables_csv = flag.String("tables", "", "Tables to copy as CSV")
	dumppg     = flag.Bool("dumppostgres", true, "Whether to dump postgres")
	updateRS   = flag.Bool("updateredshift", true, "Whether to replace redshift")
)

func main() {
	flag.Parse()
	tables := strings.Split(*tables_csv, ",")

	pgdb := postgres.NewDB()
	defer pgdb.Close()
	tsmap, err := pgdb.GetTableSchemas(tables, "")
	if err != nil {
		log.Fatal(err)
	}
	if *dumppg {
		if err := pgdb.DumpTablesToS3(tables, *s3prefix); err != nil {
			log.Fatal(err)
		}
		log.Println("POSTGRES DUMPED TO S3")
	}
	if *updateRS {
		r, err := redshift.NewRedshift()
		defer r.Close()
		if err != nil {
			log.Fatal(err)
		}
		if err := r.RefreshTables(tsmap, *s3prefix, awsRegion, '|'); err != nil {
			log.Fatal(err)
		}
		if err := r.VacuumAnalyze(); err != nil {
			log.Fatal(err)
		}
	}
}
