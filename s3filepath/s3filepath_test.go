package s3filepath

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mitchellh/goamz/s3"
	"github.com/stretchr/testify/assert"
)

var (
	date1        = time.Date(2015, time.November, 8, 23, 0, 0, 0, time.UTC)
	date2        = time.Date(2015, time.November, 9, 23, 0, 0, 0, time.UTC)
	expectedDate = time.Date(2015, time.November, 10, 23, 0, 0, 0, time.UTC)
	date3        = time.Date(2015, time.November, 11, 23, 0, 0, 0, time.UTC)
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
	return b.results, nil
}

func getTestFileWithResults(b, s, t, suf, r, aID, sk, confFile string, date time.Time, res *s3.ListResp) S3File {
	mockBucket := MockBucket{s3.Bucket{}, b, r, aID, sk, res}
	s3File := S3File{
		Bucket:    &mockBucket,
		Schema:    s,
		Table:     t,
		JSONPaths: "auto",
		Suffix:    suf,
		DataDate:  date,
		ConfFile:  confFile,
	}
	return s3File
}

func getKeyFromDate(schema, table, suffix string, date time.Time) s3.Key {
	return s3.Key{fmt.Sprintf("%s_%s_%s.%s", schema, table, date.Format(time.RFC3339), suffix), "", 0, "", "", s3.Owner{}}
}

func TestFindLatestInputData(t *testing.T) {
	bucket, schema, table, suffix, region, accessID, secretKey := "b", "s", "t", "json.gz", "r", "aID", "sk"
	// test no files found
	res := s3.ListResp{}
	res.Contents = []s3.Key{}
	expFile := getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, "", expectedDate, &res)
	returnedDate, returnedSuffix, err := FindLatestInputData(expFile.Bucket, expFile.Schema, expFile.Table, &expFile.DataDate)
	if assert.Error(t, err) {
		assert.Equal(t, true, strings.Contains(err.Error(), "no files found"))
	}

	// test too many files found?
	res = s3.ListResp{}
	var keys []s3.Key
	for i := 0; i <= 10005; i++ {
		keys = append(keys, s3.Key{})
	}
	res.Contents = keys
	expFile = getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, "", expectedDate, &res)
	returnedDate, returnedSuffix, err = FindLatestInputData(expFile.Bucket, expFile.Schema, expFile.Table, &expFile.DataDate)
	if assert.Error(t, err) {
		assert.Equal(t, true, strings.Contains(err.Error(), "too many files"))
	}

	// test set date
	res = s3.ListResp{}
	keys = []s3.Key{
		getKeyFromDate(schema, table, suffix, date1),
		getKeyFromDate(schema, table, suffix, date2),
		getKeyFromDate(schema, table, suffix, expectedDate),
		getKeyFromDate(schema, table, suffix, date3),
	}
	res.Contents = keys
	expFile = getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, "bar.yml", expectedDate, &res)
	returnedDate, returnedSuffix, err = FindLatestInputData(expFile.Bucket, expFile.Schema, expFile.Table, &expFile.DataDate)
	if assert.NoError(t, err) {
		assert.Equal(t, expectedDate, returnedDate)
	}

	// test no match
	res = s3.ListResp{}
	keys = []s3.Key{
		getKeyFromDate(schema, table, suffix, date1),
		getKeyFromDate(schema, table, suffix, date2),
		getKeyFromDate(schema, table, suffix, date3),
	}
	res.Contents = keys
	expFile = getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, "bar.yml", expectedDate, &res)
	returnedDate, returnedSuffix, err = FindLatestInputData(expFile.Bucket, expFile.Schema, expFile.Table, &expFile.DataDate)
	if assert.Error(t, err) {
		assert.Equal(t, true, strings.Contains(err.Error(), "3 files found, but none found"))
	}

	// test most recent
	res = s3.ListResp{}
	// shuffle the keys to test sorting by date
	keys = []s3.Key{
		getKeyFromDate(schema, table, suffix, date2),
		getKeyFromDate(schema, table, suffix, date3),
		getKeyFromDate(schema, table, suffix, date1),
	}
	res.Contents = keys
	expFile = getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, "bar.yml", date3, &res)
	returnedDate, returnedSuffix, err = FindLatestInputData(expFile.Bucket, expFile.Schema, expFile.Table, nil)
	if assert.NoError(t, err) {
		assert.Equal(t, date3, returnedDate)
		assert.Equal(t, suffix, returnedSuffix)
	}

	// test avoid yml files when looking for most recent data
	res = s3.ListResp{}
	// shuffle the keys to test sorting by date
	keys = []s3.Key{
		getKeyFromDate(schema, table, suffix, date2),
		getKeyFromDate(schema, table, "yml", date3),
		getKeyFromDate(schema, table, "yml", date1),
	}
	res.Contents = keys
	expFile = getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, "bar.yml", date2, &res)
	returnedDate, returnedSuffix, err = FindLatestInputData(expFile.Bucket, expFile.Schema, expFile.Table, nil)
	if assert.NoError(t, err) {
		assert.Equal(t, date2, returnedDate)
		assert.Equal(t, suffix, returnedSuffix)
	}

}

func TestCreateS3File(t *testing.T) {
	bucket, schema, table, suffix, region, accessID, secretKey := "b", "s", "t", "suf", "r", "aID", "sk"
	// test generated conf file
	expConf := fmt.Sprintf("s3://%s/config_%s_%s_%s.yml",
		bucket, schema, table, expectedDate.Format(time.RFC3339))
	res := s3.ListResp{}
	expFile := getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, expConf, expectedDate, &res)
	returnedFile := CreateS3File(expFile.Bucket, schema, table, suffix, "", expectedDate)
	assert.Equal(t, expFile, *returnedFile)

	// test supplied conf file
	expFile = getTestFileWithResults(bucket, schema, table, suffix, region, accessID, secretKey, "foo", expectedDate, &res)
	returnedFile = CreateS3File(expFile.Bucket, schema, table, suffix, "foo", expectedDate)
	assert.Equal(t, expFile, *returnedFile)
}

func TestDateFromFileName(t *testing.T) {
	// test valid RFC 3999 date
	expectedDate := time.Date(2015, time.November, 10, 23, 0, 0, 0, time.UTC)
	date, err := getDateFromFileName("foo_bar_2015-11-10T23:00:00Z.json.gz")
	assert.NoError(t, err)
	assert.Equal(t, expectedDate, date)

	// test bad dates
	date, err = getDateFromFileName("nodate.json")
	assert.Error(t, err)
	date, err = getDateFromFileName("bad_date_2015-11-10.json.gz")
	assert.Error(t, err)
}

func TestSuffixFromFileName(t *testing.T) {
	// test valid RFC 3999 date
	suffix, err := getSuffixFromFileName("foo_bar_2015-11-10T23:00:00Z.json.gz")
	assert.NoError(t, err)
	assert.Equal(t, "json.gz", suffix)

	suffix, err = getSuffixFromFileName("foo_bar_2015-11-10T23:00:00Z.json")
	assert.NoError(t, err)
	assert.Equal(t, "json", suffix)

	// test bad suffix
	_, err = getSuffixFromFileName("foo_bar_2015-11-10T23:00:00Z")
	assert.Error(t, err)
}
