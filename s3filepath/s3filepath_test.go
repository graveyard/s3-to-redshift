package s3filepath

import (
	"log"
	"testing"
	"time"

	"github.com/mitchellh/goamz/s3"
	"github.com/stretchr/testify/assert"
)

type MockBucket struct {
	s3.Bucket
	// info that makes more sense here
	BucketName      string
	BucketRegion    string
	BucketAccessID  string
	BucketSecretKey string
	results         *s3.ListResp
}

func (b *MockBucket) Name() string      { return b.BucketName }
func (b *MockBucket) Region() string    { return b.BucketRegion }
func (b *MockBucket) AccessID() string  { return b.BucketAccessID }
func (b *MockBucket) SecretKey() string { return b.BucketSecretKey }
func (b *MockBucket) List(prefix, delim, marker string, max int) (result *s3.ListResp, err error) {
	log.Println("in mock bucket")
	return b.results, nil
}

func getTestFileWithResults(res *s3.ListResp) S3File {
	mockBucket := MockBucket{s3.Bucket{}, "bucket", "testregion", "accesskey", "secretkey", res}
	expectedDate := time.Date(2015, time.November, 10, 23, 0, 0, 0, time.UTC)
	s3File := S3File{
		Bucket:    &mockBucket,
		Schema:    "testschema",
		Table:     "tablename",
		JSONPaths: "",
		Suffix:    "foo",
		DataDate:  expectedDate,
		ConfFile:  "foo.yml",
	}
	return s3File
}

func TestFindLatestInputData(t *testing.T) {
	// test no files found
	res := s3.ListResp{}
	res.Contents = []s3.Key{}
	expFile := getTestFileWithResults(&res)
	returnedFile, err := FindLatestInputData(expFile.Bucket, expFile.Schema, expFile.Table, "", &expFile.DataDate)
	assert.Nil(t, returnedFile)
	assert.Error(t, err)

	// test too many files found?

	// test set date

	// test supplied conf file

	// test no match

	// test match after return of files before and after
	//assert.NoError(t, err)
}

func TestDateFromFileName(t *testing.T) {
	// test valid RFC 3999 date
	expectedDate := time.Date(2015, time.November, 10, 23, 0, 0, 0, time.UTC)
	date, err := getDateFromFileName("foo_bar_2015-11-10T23:00:00Z.json")
	assert.NoError(t, err)
	assert.Equal(t, expectedDate, date)

	// test bad dates
	date, err = getDateFromFileName("nodate.json")
	assert.Error(t, err)
	date, err = getDateFromFileName("bad_date_2015-11-10.json")
	assert.Error(t, err)
}
