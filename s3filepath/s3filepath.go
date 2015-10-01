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
}

func (f *S3File) GetDataFilename() string {
	return fmt.Sprintf("s3://%s/%s_%s_%s.%s", f.Bucket, f.Schema, f.Table, f.DataDate.Format(time.RFC3339), f.Suffix)
}

func (f *S3File) GetConfigFilename() string {
	return fmt.Sprintf("s3://%s/config_%s_%s_%s.yml", f.Bucket, f.Schema, f.Table, f.DataDate.Format(time.RFC3339))
}

// We need to sort by data timestamp to find the most recent s3 file
type byTimeStampDesc []s3.Key

func (k byTimeStampDesc) Len() int           { return len(k) }
func (k byTimeStampDesc) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k byTimeStampDesc) Less(i, j int) bool { return k[i].Key > k[j].Key } // reversing so we get sorted by date desc

// FindLatestS3FileData looks for the most recent file matching the prefix
// created by <schema>_<table> since the date passed in, using the RFC3999 date in the filename
func FindLatestInputData(s3Conn *s3.S3, bucket, schema, table string, beforeDate time.Time) (S3File, error) {
	var retFile S3File
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
		if err == nil {
			// only want dates after or equal to the time requested - for instance the time in the db
			// match rounded date, for instance we might be computing daily or hourly
			if !date.Before(beforeDate) {
				// hardcode json.gz
				inputObj := S3File{s3Conn.Region.Name, s3Conn.Auth.AccessKey, s3Conn.Auth.SecretKey, bucket, schema, table, item.Key, "json.gz", ' ', date}
				if err != nil {
					return retFile, err
				}
				return inputObj, nil
			}
		} else {
			log.Printf("ignoring file: %s, err is: %s", item.Key, err)
		}
	}
	notFoundErr := fmt.Errorf("%d files found, but none after or equal to %s found with search path: 's3://%s/%s' Most recent: %s",
		len(items), beforeDate.Format(time.RFC3339), bucket, search, items[0].Key)
	return retFile, notFoundErr
}

// only used internally, just to parse the date in the filename
func getDateFromFileName(fileName string) (time.Time, error) {
	var retTime time.Time
	matches := s3Regex.FindStringSubmatch(fileName)
	if len(matches) < 2 {
		return retTime, fmt.Errorf("issue parsing date from file name: %s, no match for date found", fileName)
	}
	return time.Parse(time.RFC3339, matches[1])
}
