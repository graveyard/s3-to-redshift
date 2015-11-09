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
	s3Regex   = regexp.MustCompile(".*_.*_(.*?)\\.(.*)")
	yamlRegex = regexp.MustCompile(".*\\.yml")
)

// Bucketer interface is useful for testing and showing that
// we only use List from the goamz library
type Bucketer interface {
	List(prefix, delim, marker string, max int) (result *s3.ListResp, err error)
	Name() string
	Region() string
	AccessID() string
	SecretKey() string
}

// S3Bucket is our subset of the s3.Bucket class, useful for testing mostly
type S3Bucket struct {
	C s3.Bucket
	// info that makes more sense here
	BucketName      string
	BucketRegion    string
	BucketAccessID  string
	BucketSecretKey string
}

// All of these simple accessors are to adjust to Bucketer for testing

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
	return b.C.List(prefix, delim, marker, max)
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

// CreateS3File creates an S3File object with either a supplied config
// file or the function generates a config file name
func CreateS3File(bucket Bucketer, schema, table, suffix, suppliedConf string, date time.Time) *S3File {
	// set configuration location
	confFile := fmt.Sprintf("s3://%s/config_%s_%s_%s.yml", bucket.Name(), schema, table, date.Format(time.RFC3339))
	if suppliedConf != "" {
		confFile = suppliedConf
	}
	inputObj := S3File{bucket, schema, table, "auto", suffix, date, confFile}
	return &inputObj
}

// FindLatestInputData looks for the most recent file matching the prefix
// created by <schema>_<table>, using the RFC3999 date in the filename
// and returns the date associated with that data
func FindLatestInputData(bucket Bucketer, schema, table string, targetDate *time.Time) (time.Time, string, error) {
	var returnDate time.Time
	var suffix string
	// when we list, we want all files that look like <schema>_<table> and we look at the dates
	search := fmt.Sprintf("%s_%s", schema, table)
	maxKeys := 10000 // perhaps configure, right now this seems like a fine default
	listRes, err := bucket.List(search, "", "", maxKeys)
	if err != nil {
		return returnDate, suffix, err
	}
	items := listRes.Contents
	if len(items) == 0 {
		return returnDate, suffix, fmt.Errorf("no files found with search path: s3://%s/%s", bucket.Name(), search)
	}
	if len(items) >= maxKeys {
		return returnDate, suffix, fmt.Errorf("too many files returned, perhaps increase maxKeys, currently: %s", maxKeys)
	}
	sort.Sort(byTimeStampDesc(items)) // sort by ts desc

	for _, item := range items {
		returnDate, errDate := getDateFromFileName(item.Key)
		suffix, errZip := getSuffixFromFileName(item.Key)
		// ignore malformed s3 files
		if errDate != nil || errZip != nil {
			log.Printf("ignoring file: %s, errs are: %s, %s", item.Key, errDate, errZip)
			continue
		}
		if targetDate != nil && !targetDate.Equal(returnDate) {
			log.Printf("date set to %s, ignoring non-matching file: %s with date: %s", *targetDate, item.Key, returnDate)
			continue
		}
		if yamlRegex.MatchString(item.Key) {
			log.Printf("ignoring yaml file: %s, we are looking for data files", item.Key)
			continue
		}
		return returnDate, suffix, nil
	}
	notFoundErr := fmt.Errorf("%d files found, but none found with search path: 's3://%s/%s' and date (if set) %s Most recent: %s",
		len(items), bucket.Name(), search, targetDate, items[0].Key)

	return returnDate, suffix, notFoundErr
}

// only used internally, just to parse the data date in the filename
func getDateFromFileName(fileName string) (time.Time, error) {
	var retTime time.Time
	matches := s3Regex.FindStringSubmatch(fileName)
	if len(matches) < 3 {
		return retTime, fmt.Errorf("issue parsing date from file name: %s, no match for date found", fileName)
	}
	return time.Parse(time.RFC3339, matches[1])
}

// only used internally, just to figure out if it's compressed or not
// and get the suffix
func getSuffixFromFileName(fileName string) (string, error) {
	matches := s3Regex.FindStringSubmatch(fileName)
	if len(matches) < 3 {
		return "", fmt.Errorf("issue parsing suffix from file name: %s", fileName)
	}
	return matches[2], nil
}
