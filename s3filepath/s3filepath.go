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

type Bucketer interface {
	List(prefix, delim, marker string, max int) (result *s3.ListResp, err error)
	Name() string
	Region() string
	AccessID() string
	SecretKey() string
}

// S3Bucket is our subset of the s3.Bucket class, useful for testing mostly
type S3Bucket struct {
	s3.Bucket
	// info that makes more sense here
	BucketName      string
	BucketRegion    string
	BucketAccessID  string
	BucketSecretKey string
}

// All of these simple accessors are for testing
// Name returns the name
func (b *S3Bucket) Name() string { return b.BucketName }

// Region returns the name
func (b *S3Bucket) Region() string { return b.BucketRegion }

// AccessID returns the name
func (b *S3Bucket) AccessID() string { return b.BucketAccessID }

// SecretKey returns the name
func (b *S3Bucket) SecretKey() string { return b.BucketSecretKey }

// List calls the underlying s3.Bucket List method
func (b *S3Bucket) List(prefix, delim, marker string, max int) (result *s3.ListResp, err error) {
	return b.List(prefix, delim, marker, max)
}

// S3File holds everything needed to run a COPY on the file
type S3File struct {
	// info on which file to get
	Bucket    Bucketer
	Schema    string
	Table     string
	JSONPaths string
	Suffix    string
	DataDate  time.Time
	ConfFile  string
}

// GetDataFilename returns the s3 filepath associated with an S3File
// useful for redshift COPY commands, amongst other things
func (f *S3File) GetDataFilename() string {
	return fmt.Sprintf("s3://%s/%s_%s_%s.%s", f.Bucket.Name(), f.Schema, f.Table, f.DataDate.Format(time.RFC3339), f.Suffix)
}

// We need to sort by data timestamp to find the most recent s3 file
type byTimeStampDesc []s3.Key

func (k byTimeStampDesc) Len() int           { return len(k) }
func (k byTimeStampDesc) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k byTimeStampDesc) Less(i, j int) bool { return k[i].Key > k[j].Key } // reversing so we get sorted by date desc

// FindLatestInputData looks for the most recent file matching the prefix
// created by <schema>_<table>, using the RFC3999 date in the filename
func FindLatestInputData(bucket *S3Bucket, schema, table, suppliedConf string, targetDate *time.Time) (S3File, error) {
	var retFile S3File

	// when we list, we want all files that look like <schema>_<table> and we look at the dates
	search := fmt.Sprintf("%s_%s", schema, table)
	maxKeys := 10000 // perhaps configure, right now this seems like a fine default
	listRes, err := bucket.List(search, "", "", maxKeys)
	if err != nil {
		return retFile, err
	}
	items := listRes.Contents
	if len(items) == 0 {
		return retFile, fmt.Errorf("no files found with search path: s3://%s/%s", bucket.Name(), search)
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
				confFile := fmt.Sprintf("s3://%s/config_%s_%s_%s.yml", bucket.Name(), schema, table, date.Format(time.RFC3339))
				if suppliedConf != "" {
					confFile = suppliedConf
				}
				// hardcode json.gz
				inputObj := S3File{bucket, schema, table, "auto", "json.gz", date, confFile}
				return inputObj, nil
			}
		}
	}
	notFoundErr := fmt.Errorf("%d files found, but none found with search path: 's3://%s/%s' and date (if set) %s Most recent: %s",
		len(items), bucket.Name(), search, targetDate, items[0].Key)

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
