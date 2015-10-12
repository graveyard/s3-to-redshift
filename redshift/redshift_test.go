package redshift

import (
	"fmt"
	"testing"
	"time"

	"github.com/mitchellh/goamz/s3"

	"github.com/Clever/redshifter/s3filepath"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestRunJSONCopy(t *testing.T) {
	schema, table := "testschema", "tablename"
	bucket, region, accessID, secretKey := "bucket", "region", "accessID", "secretKey"
	b := s3filepath.S3Bucket{s3.Bucket{}, bucket, region, accessID, secretKey}
	s3File := s3filepath.S3File{
		Bucket:    &b,
		Schema:    schema,
		Table:     table,
		JSONPaths: "auto",
		Suffix:    "json.gz",
		DataDate:  time.Now(),
		ConfFile:  "",
	}
	// test with creds and GZIP
	sql := `COPY "%s"."%s" FROM '%s' WITH %s JSON '%s' REGION '%s' TIMEFORMAT 'auto' STATUPDATE ON COMPUPDATE ON CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`
	execRegex := fmt.Sprintf(sql, schema, table, s3File.GetDataFilename(),
		"GZIP", "auto", region, accessID, secretKey)

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}

	mock.ExpectBegin()
	mock.ExpectExec(execRegex).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunJSONCopy(tx, s3File, true, true))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	// test with neither creds nor GZIP
	sql = `COPY "%s"."%s" FROM '%s' WITH%s JSON '%s' REGION '%s' TIMEFORMAT 'auto' STATUPDATE ON COMPUPDATE ON`
	execRegex = fmt.Sprintf(sql, schema, table, s3File.GetDataFilename(),
		"", "auto", region)

	db, mock, err = sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs = Redshift{db}

	mock.ExpectBegin()
	mock.ExpectExec(execRegex).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err = mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunJSONCopy(tx, s3File, false, false))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestRunTruncate(t *testing.T) {
	schema, table := "test_schema", "test_table"
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}

	mock.ExpectBegin()
	mock.ExpectPrepare(`DELETE FROM "?"."?"`)
	mock.ExpectExec(`DELETE FROM ".*".".*"`).WithArgs(schema, table).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunTruncate(tx, schema, table))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestVacuumAnalyzeTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}
	mock.ExpectExec(`VACUUM FULL`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`ANALYZE`).WillReturnResult(sqlmock.NewResult(0, 0))
	assert.NoError(t, mockrs.VacuumAnalyze())
}
