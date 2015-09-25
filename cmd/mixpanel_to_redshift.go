package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Clever/redshifter/mixpanel"
	"github.com/Clever/redshifter/redshift"
	"github.com/segmentio/go-env"

	"gopkg.in/Clever/pathio.v1"
)

var (
	// TODO: include flag validation
	jsonpathsFile      = flag.String("jsonpathsfile", "", "s3 file with jsonpaths data.")
	mixpanelEvents     = flag.String("mixpanelevents", "", "Comma separated values of events to be exported.")
	mixpanelExportDate = flag.String("exportdate",
		time.Now().AddDate(0, 0, -1).Format("2006-01-02"),
		"Date in YYYY-MM-DD format. Defaults to yesterday.")
	mixpanelExportDir = flag.String("exportdir", "", "Directory to store the exported mixpanel data.")
	host              = flag.String("host", "", "Address of the redshift host")
	port              = flag.String("port", "5439", "Port of the redshift host")
	db                = flag.String("database", "", "Redshift database to connect to")
	user              = flag.String("user", "", "Redshift user to connect as")
	schema            = flag.String("schema", "public", "Schema with the redshift table.")
	table             = flag.String("table", "", "Name of the redshift table.")
	pwd               = flag.String("password", "", "Password for the redshift user")
	redshiftTimeout   = flag.Int("connecttimeout", 10,
		"Timeout in seconds while connecting to Redshift. Defaults to 10 seconds.")
	exportFromMixpanel = flag.Bool("export", true, "Whether to export from mixpanel.")
	copyToRedshift     = flag.Bool("copy", true, "Whether to copy to redshift.")
)

func main() {
	flag.Parse()
	exportFile := fmt.Sprintf("%s/%s", *mixpanelExportDir, *mixpanelExportDate)

	if *exportFromMixpanel {
		mixpanelExport := mixpanel.NewExport()
		log.Println("Exporting mixpanel data for", *mixpanelExportDate)
		params := map[string]interface{}{
			"event":     strings.Split(*mixpanelEvents, ","),
			"from_date": *mixpanelExportDate,
			"to_date":   *mixpanelExportDate,
		}
		body, err := mixpanelExport.Request("export", params)
		if err != nil {
			log.Fatal(err)
		}
		if err := pathio.Write(exportFile, body); err != nil {
			log.Fatal(err)
		}
	}

	if *copyToRedshift {
		s3Info := redshift.S3Info{
			Region:    env.MustGet("AWS_REGION"),
			AccessID:  env.MustGet("AWS_ACCESS_KEY_ID"),
			SecretKey: env.MustGet("AWS_SECRET_ACCESS_KEY"),
		}
		r, err := redshift.NewRedshift(*host, *port, *db, *user, *pwd, *redshiftTimeout, s3Info)
		defer r.Close()
		if err != nil {
			log.Fatal(err)
		}
		if err := r.CopyJSONDataFromS3(*schema, *table, exportFile, *jsonpathsFile); err != nil {
			log.Fatal(err)
		}
		if err := r.VacuumAnalyze(); err != nil {
			log.Fatal(err)
		}
	}
}
