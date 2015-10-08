package s3filepath

import (
	"fmt"
	"log"
	"sort"

	"regexp"
	"time"

	"github.com/mitchellh/goamz/s3"
)

var (
	// currently assumes no unix file created timestamp
	s3Regex = regexp.MustCompile(".*_.*_(.*).json.*")
)

type S3File struct {
	// info needed to access
	Region    string
	AccessID  string
	SecretKey string
	// info on which file to get
	Bucket    string // decide to keep this as string for simplicity
	Schema    string
	Table     string
	JsonPaths string
	Suffix    string
	Delimiter rune
	DataDate  time.Time
	ConfFile  string
}

// GetDataFilename returns the s3 filepath associated with an S3File
// useful for redshift COPY commands, amongst other things
func (f *S3File) GetDataFilename() string {
	return fmt.Sprintf("s3://%s/%s_%s_%s.%s", f.Bucket, f.Schema, f.Table, f.DataDate.Format(time.RFC3339), f.Suffix)
}

// We need to sort by data timestamp to find the most recent s3 file
type byTimeStampDesc []s3.Key

func (k byTimeStampDesc) Len() int           { return len(k) }
func (k byTimeStampDesc) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k byTimeStampDesc) Less(i, j int) bool { return k[i].Key > k[j].Key } // reversing so we get sorted by date desc

// FindLatestS3FileData looks for the most recent file matching the prefix
// created by <schema>_<table>, using the RFC3999 date in the filename
func FindLatestInputData(s3Conn *s3.S3, bucket, schema, table, suppliedConf string, targetDate *time.Time) (S3File, error) {
	var retFile S3File

	// when we list, we want all files that look like <schema>_<table> and we look at the dates
	search := fmt.Sprintf("%s_%s", schema, table)
	maxKeys := 10000 // perhaps configure, right now this seems like a fine default
	listRes, err := s3Conn.Bucket(bucket).List(search, "", "", maxKeys)
	if err != nil {
		return retFile, err
	}
	items := listRes.Contents
	if len(items) == 0 {
		return retFile, fmt.Errorf("no files found with search path: s3://%s/%s", bucket, search)
	}
	if len(items) >= maxKeys {
		return retFile, fmt.Errorf("too many files returned, perhaps increase maxKeys, currently: %s", maxKeys)
	}
	sort.Sort(byTimeStampDesc(items)) // sort by ts desc

	for _, item := range items {
		date, err := getDateFromFileName(item.Key)
		// ignore malformed s3 files
		if err != nil {
			log.Printf("ignoring file: %s, err is: %s", item.Key, err)
		} else {
			if targetDate != nil && !targetDate.Equal(date) {
				log.Printf("date set to %s, ignoring non-matching file: %s with date: %s", *targetDate, item.Key, date)
			} else {
				// set configuration location
				confFile := fmt.Sprintf("s3://%s/config_%s_%s_%s.yml", bucket, schema, table, date.Format(time.RFC3339))
				if suppliedConf != "" {
					confFile = suppliedConf
				}
				// hardcode json.gz
				inputObj := S3File{s3Conn.Region.Name, s3Conn.Auth.AccessKey, s3Conn.Auth.SecretKey, bucket, schema, table, "auto", "json.gz", ' ', date, confFile}
				return inputObj, nil
			}
		}
	}
	notFoundErr := fmt.Errorf("%d files found, but none found with search path: 's3://%s/%s' and date (if set) %s Most recent: %s",
		len(items), bucket, search, targetDate, items[0].Key)

	return retFile, notFoundErr
}

// only used internally, just to parse the data date in the filename
func getDateFromFileName(fileName string) (time.Time, error) {
	var retTime time.Time
	matches := s3Regex.FindStringSubmatch(fileName)
	if len(matches) < 2 {
		return retTime, fmt.Errorf("issue parsing date from file name: %s, no match for date found", fileName)
	}
	return time.Parse(time.RFC3339, matches[1])
}
