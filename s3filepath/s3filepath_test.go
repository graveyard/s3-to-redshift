package s3filepath

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	expectedDate = time.Date(2015, time.November, 10, 23, 0, 0, 0, time.UTC)
)

func getTestFileWithResults(b, s, t, r, aID, sk, confFile, suf string, date time.Time) S3File {
	bucket := S3Bucket{b, r, aID, sk}
	s3File := S3File{
		Bucket:    bucket,
		Schema:    s,
		Table:     t,
		JSONPaths: "auto",
		Suffix:    suf,
		DataDate:  date,
		ConfFile:  confFile,
	}
	return s3File
}

type MockPathChecker struct{}

func (MockPathChecker) FileExists(path string) bool {
	return path == "s3://b/s_t_2015-11-10T23:00:00Z.json.gz"
}

type MockJSONPathChecker struct{}

func (MockJSONPathChecker) FileExists(path string) bool {
	return path == "s3://b/s_t_2015-11-10T23:00:00Z.json"
}

func TestCreateS3File(t *testing.T) {
	bucket, schema, table, region, accessID, secretKey := "b", "s", "t", "r", "aID", "sk"
	expConf := fmt.Sprintf("s3://%s/config_%s_%s_%s.yml",
		bucket, schema, table, expectedDate.Format(time.RFC3339))

	// test completely non-existent file
	expFile := getTestFileWithResults(bucket, schema, table, region, accessID, secretKey, expConf, "json.gz", expectedDate)
	returnedFile, err := CreateS3File(MockPathChecker{}, expFile.Bucket, schema, "bad_table", "", expectedDate)
	assert.Equal(t, errors.New("S3 file not found at: bucket: b schema: s, table: bad_table date: 2015-11-10T23:00:00Z"), err)

	// test generated conf file
	expFile = getTestFileWithResults(bucket, schema, table, region, accessID, secretKey, expConf, "json.gz", expectedDate)
	returnedFile, err = CreateS3File(MockPathChecker{}, expFile.Bucket, schema, table, "", expectedDate)
	assert.Equal(t, nil, err)
	assert.Equal(t, expFile, *returnedFile)

	// test generated conf file that isn't zipped
	expFile = getTestFileWithResults(bucket, schema, table, region, accessID, secretKey, expConf, "json", expectedDate)
	returnedFile, err = CreateS3File(MockJSONPathChecker{}, expFile.Bucket, schema, table, "", expectedDate)
	assert.Equal(t, nil, err)
	assert.Equal(t, expFile, *returnedFile)

	// test supplied conf file
	expFile = getTestFileWithResults(bucket, schema, table, region, accessID, secretKey, "foo", "json.gz", expectedDate)
	returnedFile, err = CreateS3File(MockPathChecker{}, expFile.Bucket, schema, table, "foo", expectedDate)
	assert.Equal(t, nil, err)
	assert.Equal(t, expFile, *returnedFile)
}
