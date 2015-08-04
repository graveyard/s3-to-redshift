package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Clever/pathio"
	"github.com/Clever/redshifter/mixpanel"
	"github.com/Clever/redshifter/redshift"
	"github.com/segmentio/go-env"
)

var (
	// TODO: include flag validation
	awsRegion          = env.MustGet("AWS_REGION")
	jsonpathsFile      = flag.String("jsonpathsfile", "", "s3 file with jsonpaths data.")
	mixpanelEvents     = flag.String("mixpanelevents", "", "Comma separated values of events to be exported.")
	mixpanelExportDate = flag.String("exportdate",
		time.Now().AddDate(0, 0, -1).Format("2006-01-02"),
		"Date in YYYY-MM-DD format. Defaults to yesterday.")
	mixpanelExportDir  = flag.String("exportdir", "", "Directory to store the exported mixpanel data.")
	redshiftSchema     = flag.String("redshiftschema", "public", "Schema with the redshift table.")
	redshiftTable      = flag.String("redshifttable", "", "Name of the redshift table.")
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
		r, err := redshift.NewRedshift()
		defer r.Close()
		if err != nil {
			log.Fatal(err)
		}
		if err := r.CopyJSONDataFromS3(*redshiftSchema, *redshiftTable, exportFile, *jsonpathsFile, awsRegion); err != nil {
			log.Fatal(err)
		}
		if err := r.VacuumAnalyze(); err != nil {
			log.Fatal(err)
		}
	}
}
