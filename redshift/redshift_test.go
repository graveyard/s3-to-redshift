package redshift

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Clever/s3-to-redshift/s3filepath"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

// helper for TestTableFromConf - marshals the table into a file
func getTempConfFromTable(name string, table Table) (string, error) {
	toMarshal := map[string]Table{name: table}
	file, err := ioutil.TempFile(os.TempDir(), "testconf")
	if err != nil {
		return "", err
	}
	defer file.Close()
	d, err := yaml.Marshal(&toMarshal)
	if err != nil {
		return "", err
	}
	_, err = file.Write(d)
	if err != nil {
		return "", err
	}
	return file.Name(), nil
}

func TestTableFromConf(t *testing.T) {
	db := Redshift{nil}

	schema, table := "testschema", "testtable"
	bucket, region, accessID, secretKey := "bucket", "region", "accessID", "secretKey"
	b := s3filepath.S3Bucket{bucket, region, accessID, secretKey}

	matchingTable := Table{
		Name:    table,
		Columns: []ColInfo{},
		Meta: Meta{
			Schema:         schema,
			DataDateColumn: "foo",
		},
	}

	f := s3filepath.S3File{
		Bucket:    b,
		Schema:    schema,
		Table:     table,
		JSONPaths: "auto",
		Suffix:    "json.gz",
		DataDate:  time.Now(),
	}

	// valid
	fileName, err := getTempConfFromTable(table, matchingTable)
	assert.NoError(t, err)
	f.ConfFile = fileName
	returnedTable, err := db.GetTableFromConf(f)
	assert.NoError(t, err)
	assert.Equal(t, matchingTable, *returnedTable)

	// one which doesn't have the target table
	fileName, err = getTempConfFromTable("notthetable", matchingTable)
	assert.NoError(t, err)
	f.ConfFile = fileName
	returnedTable, err = db.GetTableFromConf(f)
	if assert.Error(t, err) {
		assert.Equal(t, true, strings.Contains(err.Error(), "can't find table in conf"))
	}

	// one which has a mismatched schema
	badSchema := matchingTable
	badSchema.Meta.Schema = "notthesameschema"
	fileName, err = getTempConfFromTable(table, badSchema)
	assert.NoError(t, err)
	f.ConfFile = fileName
	returnedTable, err = db.GetTableFromConf(f)
	if assert.Error(t, err) {
		assert.Equal(t, true, strings.Contains(err.Error(), "mismatched schema"))
	}

	// one without a data date column
	noDataDateCol := matchingTable
	noDataDateCol.Meta.DataDateColumn = ""
	fileName, err = getTempConfFromTable(table, noDataDateCol)
	assert.NoError(t, err)
	f.ConfFile = fileName
	returnedTable, err = db.GetTableFromConf(f)
	if assert.Error(t, err) {
		assert.Equal(t, true, strings.Contains(err.Error(), "Data Date Column must be set"))
	}
}

// I'm not going to worry about if the db throws an error
// however, this is a good candidate for an integration test to make sure that
// the SQL to find the columns works
func TestGetTableMetadata(t *testing.T) {
	schema, table, dataDateCol := "testschema", "testtable", "testdatadatecol"

	expectedDate := time.Now()
	expectedTable := Table{
		Name: table,
		Columns: []ColInfo{{
			Name:        "foo",
			Type:        "integer",
			DefaultVal:  "5",
			NotNull:     false,
			PrimaryKey:  false,
			DistKey:     false,
			SortOrdinal: 0,
		}},
		Meta: Meta{
			Schema:         schema,
			DataDateColumn: dataDateCol,
		},
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}

	// test normal operation
	//   - test existence of table
	//   - gets a bunch of rows
	//   - requests time info from the table
	//   - returns a table
	mock.ExpectBegin()

	// test table existence
	existRegex := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'`, schema, table)
	existRows := sqlmock.NewRows([]string{"table_name"})
	existRows.AddRow(table)
	mock.ExpectQuery(existRegex).WithArgs().WillReturnRows(existRows)
	// column info
	// don't look for the whole query, just the important bits
	colInfoRegex := fmt.Sprintf(`SELECT .*nspname = '%s' .*relname = '%s'.*`, schema, table)
	colInfoRows := sqlmock.NewRows([]string{"name", "col_type", "default_val",
		"not_null", "primary_key", "dist_key", "sort_ord"})
	// matches expectedTable above, used for returning from sql mock
	colInfoRows.AddRow("foo", "integer", 5, false, false, false, 0)
	mock.ExpectQuery(colInfoRegex).WithArgs().WillReturnRows(colInfoRows)
	// last data
	dateRegex := fmt.Sprintf(`SELECT "%s" FROM "%s"."%s" ORDER BY "%s" DESC LIMIT 1`, dataDateCol, schema, table, dataDateCol)
	dateRows := sqlmock.NewRows([]string{"date"})
	dateRows.AddRow(expectedDate)
	mock.ExpectQuery(dateRegex).WithArgs().WillReturnRows(dateRows)
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	returnedTable, returnedDate, err := mockrs.GetTableMetadata(schema, table, dataDateCol)
	assert.NoError(t, err)
	assert.Equal(t, expectedTable, *returnedTable)
	assert.Equal(t, expectedDate, *returnedDate)
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	// test table does not exist
	// should only run the first check
	// we don't return it as an errorin this case
	mock.ExpectBegin()
	existRegex = fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'`, schema, table)
	existRows = sqlmock.NewRows([]string{"table_name"})
	existRows.AddRow(table)
	mock.ExpectQuery(existRegex).WithArgs().WillReturnError(sql.ErrNoRows)
	mock.ExpectCommit()

	tx, err = mockrs.Begin()
	assert.NoError(t, err)
	returnedTable, returnedDate, err = mockrs.GetTableMetadata(schema, table, dataDateCol)
	assert.Error(t, err)
	assert.Equal(t, sql.ErrNoRows, err)
	assert.Nil(t, returnedTable)
	assert.Nil(t, returnedDate)
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

// test create and getColumnSQL at the same time
// getColumnSQL is too simple to be worth testing independently
func TestCreateTable(t *testing.T) {
	schema, table := "testschema", "tablename"
	dbTable := Table{
		Name: table,
		Columns: []ColInfo{
			{"test1", "int", "100", true, false, true, 1},
			{"id", "text", "", false, true, false, 0},
			{"somelongtext", "longtext", "", false, false, false, 0},
		},
		Meta: Meta{Schema: schema},
	}

	//createSQL := `aasdadsa character varying(256) PRIMARY KEY , test5 integer DEFAULT 100 NOT NULL SORTKEY DISTKEY , someww221longtext character varying(10000)`
	//sql := fmt.Sprintf(`CREATE TABLE "%s"."%s" (%s)`, schema, table, createSQL)
	regex := `CREATE TABLE ".*".".*".*` +
		`test1 integer DEFAULT 100 NOT NULL SORTKEY.*` +
		`DISTKEY.*id character varying\(256\).*PRIMARY KEY.*` +
		`somelongtext character varying\(10000\).*` // a little awk, but the prepare makes sure this is good

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}

	mock.ExpectBegin()
	mock.ExpectPrepare("this does not exist for sure")
	mock.ExpectExec(regex).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.CreateTable(tx, dbTable))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestUpdateTable(t *testing.T) {
	schema, table := "testschema", "tablename"

	targetTable := Table{
		Name: table,
		// order incorrectly on purpose to ensure ordering works
		Columns: []ColInfo{
			{"test3", "boolean", "true", false, false, false, 0},
			{"test2", "integer", "100", true, false, true, 1},
			{"id", "character varying(256)", "", false, true, false, 0},
			{"test4", "double precision", "false", false, false, false, 0},
		},
		Meta: Meta{Schema: schema},
	}

	inputTable := Table{
		Name: table,
		// order incorrectly on purpose to ensure ordering works
		Columns: []ColInfo{
			{"test3", "boolean", "true", false, false, false, 0},
			{"test2", "int", "100", true, false, true, 1},
			{"id", "text", "", false, true, false, 0},
			{"test4", "float", "false", false, false, false, 0},
		},
		Meta: Meta{Schema: schema},
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}

	// test no update
	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.UpdateTable(tx, targetTable, inputTable))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	// test regular update

	mock.ExpectBegin()
	for _, updateSQL := range []string{
		`ADD COLUMN id character varying(256) PRIMARY KEY `,
		`ADD COLUMN test2 integer DEFAULT 100 NOT NULL SORTKEY DISTKEY `,
		`ADD COLUMN test4 double precision`,
	} {
		sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" (%s)`, schema, table, updateSQL)
		regex := `ALTER TABLE ".*".".*" (.*)` // a little awk, but the prepare makes sure this is good

		mock.ExpectPrepare(sql)
		mock.ExpectExec(regex).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	}
	mock.ExpectCommit()

	fewerColumnsTargetTable := Table{
		Name: table,
		Columns: []ColInfo{
			{"test3", "boolean", "true", false, false, false, 0},
		},
		Meta: Meta{Schema: schema},
	}
	tx, err = mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.UpdateTable(tx, fewerColumnsTargetTable, inputTable))
	assert.NoError(t, tx.Commit())

	// test extra columns (no error currently)
	fewerColumnsInputTable := Table{
		Name: table,
		// order incorrectly on purpose to ensure ordering works
		Columns: []ColInfo{
			{"id", "text", "", false, true, false, 0},
		},
		Meta: Meta{Schema: schema},
	}
	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err = mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.UpdateTable(tx, targetTable, fewerColumnsInputTable))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	// test mismatching columns (does error)
	// each one is one item off from: {1, "id", "text", "", false, true, false, 0},
	for _, c := range []ColInfo{
		{"id", "boolean", "", false, true, false, 0},
		{"id", "text", "foo", false, true, false, 0},
		{"id", "text", "", true, true, false, 0},
		{"id", "text", "", false, false, false, 0},
		{"id", "text", "", false, true, true, 0},
		{"id", "text", "", false, true, false, 1},
	} {
		mismatchingColInputTable := Table{
			Name:    table,
			Columns: []ColInfo{c},
			Meta:    Meta{Schema: schema},
		}
		mock.ExpectBegin()
		mock.ExpectCommit()

		tx, err = mockrs.Begin()
		assert.NoError(t, err)
		err = mockrs.UpdateTable(tx, targetTable, mismatchingColInputTable)
		log.Println("mismatch err: ", err)
		assert.Error(t, err)
		assert.NoError(t, tx.Commit())

		if err = mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expections: %s", err)
		}
	}
}

func TestJSONCopy(t *testing.T) {
	schema, table := "testschema", "tablename"
	bucket, region, accessID, secretKey := "bucket", "region", "accessID", "secretKey"
	b := s3filepath.S3Bucket{bucket, region, accessID, secretKey}
	s3File := s3filepath.S3File{
		Bucket:    b,
		Schema:    schema,
		Table:     table,
		JSONPaths: "auto",
		Suffix:    "json.gz",
		DataDate:  time.Now(),
		ConfFile:  "",
	}
	// test with creds and GZIP
	sql := `COPY "%s"."%s" FROM '%s' WITH %s JSON '%s' REGION '%s' TIMEFORMAT 'auto' TRUNCATECOLUMNS STATUPDATE ON COMPUPDATE ON CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`
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
	assert.NoError(t, mockrs.JSONCopy(tx, s3File, true, true))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	// test with neither creds nor GZIP
	sql = `COPY "%s"."%s" FROM '%s' WITH%s JSON '%s' REGION '%s' TIMEFORMAT 'auto' TRUNCATECOLUMNS STATUPDATE ON COMPUPDATE ON`
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
	assert.NoError(t, mockrs.JSONCopy(tx, s3File, false, false))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestManifestCopy(t *testing.T) {
	schema, table := "testschema", "tablename"
	bucket, region, accessID, secretKey := "bucket", "region", "accessID", "secretKey"
	b := s3filepath.S3Bucket{bucket, region, accessID, secretKey}
	s3File := s3filepath.S3File{
		Bucket:    b,
		Schema:    schema,
		Table:     table,
		JSONPaths: "auto",
		Suffix:    "manifest",
		DataDate:  time.Now(),
		ConfFile:  "",
	}
	// test with creds and GZIP
	sql := `COPY "%s"."%s" FROM '%s' WITH %s JSON '%s' REGION '%s' TIMEFORMAT 'auto' TRUNCATECOLUMNS STATUPDATE ON COMPUPDATE ON manifest CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'`
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
	assert.NoError(t, mockrs.JSONCopy(tx, s3File, true, true))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestTruncate(t *testing.T) {
	schema, table := "test_schema", "test_table"
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}

	mock.ExpectBegin()
	mock.ExpectPrepare(fmt.Sprintf(`DELETE FROM "%s"."%s"`, schema, table))
	mock.ExpectExec(`DELETE FROM ".*".".*"`).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.Truncate(tx, schema, table))
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
