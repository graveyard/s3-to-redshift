package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/Clever/pathio"
	_ "github.com/lib/pq" // Postgres Driver
	env "github.com/segmentio/go-env"
)

var (
	awsAccessKeyId     = env.MustGet("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey = env.MustGet("AWS_SECRET_ACCESS_KEY")
	awsRegion          = env.MustGet("AWS_REGION")
	mixpanelAPIKey     = env.MustGet("MIXPANEL_API_KEY")
	mixpanelAPISecret  = env.MustGet("MIXPANEL_API_SECRET")
	redshiftHost       = env.MustGet("AWS_REDSHIFT_HOST")
	redshiftPort       = env.MustGet("AWS_REDSHIFT_PORT")
	redshiftDatabase   = env.MustGet("AWS_REDSHIFT_DB_NAME")
	redshiftUser       = env.MustGet("AWS_REDSHIFT_USER")
	redshiftPassword   = env.MustGet("AWS_REDSHIFT_PASSWORD")
	jsonpathsFile      = flag.String("jsonpathsfile", "", "s3 file with jsonpaths data.")
	mixpanelEvents     = flag.String("mixpanelevents", "", "Comma separated values of events to be exported.")
	mixpanelExportDate = flag.String("exportdate",
		time.Now().AddDate(0, 0, -1).Format("2006-01-02"),
		"Date in YYYY-MM-DD format. Defaults to yesterday.")
	mixpanelExportRequestTimeout = flag.Duration(
		"exportrequesttimeout", 10*time.Minute,
		"Timeout value to be used while making a mixpanel export request. Defaults to 10 mins.")
	mixpanelExportDir  = flag.String("exportdir", "", "Directory to store the exported mixpanel data.")
	redshiftTable      = flag.String("redshifttable", "", "Name of the redshift table.")
	exportFromMixpanel = flag.Bool("export", true, "Whether to export from mixpanel.")
	copyToRedshift     = flag.Bool("copy", true, "Whether to copy to redshift.")
)

type MixpanelExport struct {
	endpoint  string
	version   string
	apiKey    string
	apiSecret string
}

func NewMixpanelExport() *MixpanelExport {
	m := MixpanelExport{"https://data.mixpanel.com/api", "2.0", mixpanelAPIKey, mixpanelAPISecret}
	return &m
}

// json.Marshal wraps a string with quotes which breaks URL params in API calls. This function fixes
// that problem.
func StringOrJsonMarshal(value interface{}) (string, error) {
	strValue, ok := value.(string)
	if !ok {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		strValue = string(jsonValue)
	}
	return strValue, nil
}

func (m *MixpanelExport) ComputeSig(params map[string]interface{}) (string, error) {
	var keys []string

	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	joinedArgs := ""
	for _, k := range keys {
		joinedArgs += k
		value, err := StringOrJsonMarshal(params[k])
		if err != nil {
			return "", err
		}
		joinedArgs += fmt.Sprintf("=%s", value)
	}

	hash := md5.Sum([]byte(joinedArgs + m.apiSecret))
	return hex.EncodeToString(hash[:]), nil
}

func (m *MixpanelExport) Request(method string, params map[string]interface{}) ([]byte, error) {
	params["api_key"] = m.apiKey
	expireTime := time.Now().Add(*mixpanelExportRequestTimeout)
	params["expire"] = expireTime.Unix()
	params["format"] = "json"
	// Delete original signature from the params if already set and assign the correct signature
	// based on the new param values.
	delete(params, "sig")
	sig, err := m.ComputeSig(params)
	if err != nil {
		return nil, err
	}
	params["sig"] = sig

	values := url.Values{}
	for key, value := range params {
		strValue, err := StringOrJsonMarshal(value)
		if err != nil {
			return nil, err
		}
		values.Set(key, strValue)
	}
	url := fmt.Sprintf("%s/%s/%s/?%s", m.endpoint, m.version, method, values.Encode())
	log.Println(url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func CopyJsonDataFromS3ToRedshift(
	db *sql.DB, redshiftTable string, exportFile string, awsAccessKeyId string,
	awsSecretAccessKey string, jsonpathsFile string, awsRegion string) error {
	copyCmd := fmt.Sprint(
		"COPY ", redshiftTable, " FROM '", exportFile, "' WITH ",
		" json '", jsonpathsFile,
		"' region '", awsRegion,
		"' timeformat 'epochsecs'")
	log.Print("Executing command: ", copyCmd)
	copyCmd += fmt.Sprintf(" credentials '%s=%s;%s=%s'", "aws_access_key_id", awsAccessKeyId, "aws_secret_access_key", awsSecretAccessKey)
	_, err := db.Exec(copyCmd)
	return err
}

func main() {
	flag.Parse()
	exportFile := fmt.Sprintf("%s/%s", *mixpanelExportDir, *mixpanelExportDate)

	if *exportFromMixpanel {
		mixpanelExport := NewMixpanelExport()
		log.Println("Exporting mixpanel data for", *mixpanelExportDate)
		params := map[string]interface{}{
			"event":     strings.Split(*mixpanelEvents, ","),
			"from_date": *mixpanelExportDate,
			"to_date":   *mixpanelExportDate,
		}
		body, err := mixpanelExport.Request("export", params)
		if err != nil {
			log.Panic(err)
		}
		err = pathio.Write(exportFile, body)
		if err != nil {
			log.Panic(err)
		}
	}

	if *copyToRedshift {
		redshiftSource := fmt.Sprintf("host=%s port=%s dbname=%s", redshiftHost, redshiftPort, redshiftDatabase)
		log.Println("Connecting to Reshift Source: ", redshiftSource)
		redshiftSource += fmt.Sprintf(" user=%s password=%s", redshiftUser, redshiftPassword)
		db, err := sql.Open("postgres", redshiftSource)
		if err != nil {
			log.Panic(err)
		}
		defer db.Close()
		err = CopyJsonDataFromS3ToRedshift(
			db, *redshiftTable, exportFile, awsAccessKeyId, awsSecretAccessKey, *jsonpathsFile, awsRegion)
		if err != nil {
			log.Panic(err)
		}
	}
}
