package s3filepath

import (
	"fmt"
	"regexp"
	"time"

	"github.com/Clever/pathio"
)

var (
	// currently assumes no unix file created timestamp
	s3Regex   = regexp.MustCompile(".*_.*_(.*?)\\.(.*)")
	yamlRegex = regexp.MustCompile(".*\\.yml")
)

// S3Bucket is our subset of the s3.Bucket class, useful for testing mostly
type S3Bucket struct {
	Name      string
	Region    string
	AccessID  string
	SecretKey string
}

// S3File holds everything needed to run a COPY on the file
type S3File struct {
	// info on which file to get
	Bucket    S3Bucket
	Schema    string
	Table     string
	JSONPaths string
	Suffix    string
	DataDate  time.Time
	ConfFile  string
}

// PathChecker is the interface for determining if a path in S3 exists, which allows
// DI for testing.
type PathChecker interface {
	FileExists(path string) bool
}

// S3PathChecker will use pathio to determine if the path actually exists in S3, and
// will be used in prod.
type S3PathChecker struct{}

// FileExists looks up if the file exists in S3 using the pathio.Reader method.
func (S3PathChecker) FileExists(path string) bool {
	reader, err := pathio.Reader(path)
	if reader != nil {
		defer reader.Close()
	}
	return err == nil
}

// GetDataFilename returns the s3 filepath associated with an S3File
// useful for redshift COPY commands, amongst other things
func (f *S3File) GetDataFilename() string {
	return fmt.Sprintf("s3://%s/%s_%s_%s.%s", f.Bucket.Name, f.Schema, f.Table, f.DataDate.Format(time.RFC3339), f.Suffix)
}

// CreateS3File creates an S3File object with either a supplied config
// file or the function generates a config file name
func CreateS3File(pc PathChecker, bucket S3Bucket, schema, table, suppliedConf string, date time.Time) (*S3File, error) {
	// set configuration location
	formattedDate := date.Format(time.RFC3339)
	confFile := fmt.Sprintf("s3://%s/config_%s_%s_%s.yml", bucket.Name, schema, table, formattedDate)
	if suppliedConf != "" {
		confFile = suppliedConf
	}

	// Try to find manifest or data files out of the following patterns, in order
	// we try to get in order as otherwise
	for _, suffix := range []string{
		"manifest", // 1) manifest file
		"json.gz",  // 2) gzipped json file
		"json"} {   // 3) json file
		inputFile := S3File{bucket, schema, table, "auto", suffix, date, confFile}
		if pc.FileExists(inputFile.GetDataFilename()) {
			return &inputFile, nil
		}
	}
	return nil, fmt.Errorf("S3 file not found at: bucket: %s schema: %s, table: %s date: %s",
		bucket.Name, schema, table, formattedDate)
}
