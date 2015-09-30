package redshift

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestGetJSONCopySQL(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	schema, table, file, jsonpathsFile := "testschema", "tablename", "s3://path", "s3://jsonpathsfile"
	sql := `COPY "%s"."%s" FROM '%s' WITH %s JSON '%s' REGION '%s' TIMEFORMAT 'auto' STATUPDATE ON COMPUPDATE ON CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`
	prepStatement := fmt.Sprintf(sql, "?", "?", "?", "?", "?", "?", "?", "?")
	execRegex := fmt.Sprintf(sql, ".*", ".*", ".*", ".*", ".*", ".*", ".*", ".*") // slightly awk

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db, s3Info}

	mock.ExpectBegin()
	mock.ExpectPrepare(prepStatement)
	mock.ExpectPrepare(prepStatement) // unsure why prep is called twice
	mock.ExpectExec(execRegex).WithArgs(schema, table, file, "GZIP", jsonpathsFile,
		s3Info.Region, s3Info.AccessID, s3Info.SecretKey).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunJSONCopy(tx, schema, table, file, jsonpathsFile, true, true))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestCopyGzipCsvDataFromS3(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	schema, table, file, delimiter := "testschema", "tablename", "s3://path", '|'
	ts := Table{
		Name: table,
		Columns: []ColInfo{
			{3, "field3", "type3", "defaultval3", false, false, false, 0},
			{1, "field1", "type1", "", true, false, false, 0},
			{2, "field2", "type2", "", false, true, false, 0},
		},
		Meta: Meta{},
	}

	sql := `COPY "%s"."%s" (%s) FROM '%s' WITH REGION '%s' %s CSV DELIMITER '%s'`
	sql += " IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE STATUPDATE ON COMPUPDATE ON"
	sql += ` CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`
	prepStatement := fmt.Sprintf(sql, "?", "?", "?", "?", "?", "?", "?", "?", "?")
	execRegex := fmt.Sprintf(sql, ".*", ".*", ".*", ".*", ".*", ".*", ".*", ".*", ".*") // slightly awk

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db, s3Info}

	mock.ExpectBegin()
	mock.ExpectPrepare(prepStatement)
	mock.ExpectPrepare(prepStatement) // unsure why prep is called twice
	mock.ExpectExec(execRegex).WithArgs(schema, table, "field1, field2, field3",
		file, s3Info.Region, "GZIP", delimiter, s3Info.AccessID, s3Info.SecretKey).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunCSVCopy(tx, schema, table, file, ts, delimiter, true, true))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestRefreshTable(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db, s3Info}
	// not really testing GetCSVCopySQL so don't worry too much about this one
	ts := Table{"Test", []ColInfo{{1, "foo", "int", "4", true, false, false, 0}}, Meta{}}
	schema, name, file, delim := "testschema", "tablename", "s3://path", '|'

	sql := `COPY "%s"."%s" (%s) FROM '%s' WITH REGION '%s' %s CSV DELIMITER '%s'`
	sql += " IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE STATUPDATE ON COMPUPDATE ON"
	sql += ` CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`
	prepStatement := fmt.Sprintf(sql, "?", "?", "?", "?", "?", "?", "?", "?", "?")
	execRegex := fmt.Sprintf(sql, ".*", ".*", ".*", ".*", ".*", ".*", ".*", ".*", ".*") // slightly awk

	mock.ExpectBegin()
	// expect a truncate
	mock.ExpectPrepare(`DELETE FROM "?"."?"`)
	mock.ExpectPrepare(`DELETE FROM "?"."?"`) // again unsure why it's called twice
	mock.ExpectExec(`DELETE FROM ".*".".*"`).WithArgs(schema, name).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectPrepare(prepStatement)
	mock.ExpectPrepare(prepStatement) // unsure why prep is called twice
	mock.ExpectExec(execRegex).WithArgs(schema, name, "foo",
		file, s3Info.Region, "GZIP", delim, s3Info.AccessID, s3Info.SecretKey).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectExec(`VACUUM FULL "testschema"."tablename"; ANALYZE "testschema"."tablename"`).WillReturnResult(sqlmock.NewResult(0, 0))

	// run the refresh table
	assert.NoError(t, mockrs.refreshTable(schema, name, file, ts, delim))

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestRunTruncate(t *testing.T) {
	schema, table := "test_schema", "test_table"
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db, S3Info{}}

	mock.ExpectBegin()
	mock.ExpectPrepare(`DELETE FROM "?"."?"`)
	mock.ExpectPrepare(`DELETE FROM "?"."?"`) // again unsure why it's called twice
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
	mockrs := Redshift{db, S3Info{}}

	schema, table := "testschema", "tablename"
	mock.ExpectExec(`VACUUM FULL "testschema"."tablename"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`ANALYZE "testschema"."tablename"`).WillReturnResult(sqlmock.NewResult(0, 0))
	assert.NoError(t, mockrs.VacuumAnalyzeTable(schema, table))
}
