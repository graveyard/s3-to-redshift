package redshift

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/clever/redshifter/postgres"
)

type mockSQLDB []string

func (m *mockSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	*m = mockSQLDB(append([]string(*m), fmt.Sprintf(query)))
	return nil, nil
}

func (m *mockSQLDB) Close() error {
	return nil
}

func TestCopyJsonDataFromS3(t *testing.T) {
	table, file, jsonpathsFile, awsRegion := "tablename", "s3://path", "s3://jsonpathsfile", "testregion"
	exp := fmt.Sprintf("COPY %s FROM '%s' WITH json '%s' region '%s' timeformat 'epochsecs' COMPUPDATE ON",
		table, file, jsonpathsFile, awsRegion)
	exp += " CREDENTIALS 'aws_access_key_id=accesskey;aws_secret_access_key=secretkey'"
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, "accesskey", "secretkey"}
	err := mockrs.CopyJsonDataFromS3(table, file, jsonpathsFile, awsRegion)
	if err != nil {
		t.Error("Unexpected error %s while during CopyJsonDataFromS3(). Expected query: %s", err.Error(), exp)
	}
	if len(cmds) == 0 {
		t.Fatalf("Expected query \"%s\" not executed during CopyJsonDataFromS3().", exp)
	}
	if cmds[0] != exp {
		log.Println(cmds[0])
		t.Fatalf("Unexpected query \"%s\" executed during CopyJsonDataFromS3. Expected \"%s\"", cmds[0], exp)
	}
}

func TestCopyGzipCsvDataFromS3(t *testing.T) {
	table, file, awsRegion, delimiter := "tablename", "s3://path", "testregion", '|'
	exp := fmt.Sprintf("COPY %s FROM '%s' WITH REGION '%s' GZIP CSV DELIMITER '%c' IGNOREHEADER 0",
		table, file, awsRegion, delimiter)
	exp += " ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON"
	exp += " CREDENTIALS 'aws_access_key_id=accesskey;aws_secret_access_key=secretkey'"
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, "accesskey", "secretkey"}
	err := mockrs.CopyGzipCsvDataFromS3(table, file, awsRegion, delimiter)
	if err != nil {
		t.Error("Unexpected error %s while during CopyGzipCsvDataFromS3(). Expected query: %s", err.Error(), exp)
	}
	if len(cmds) == 0 {
		t.Fatalf("Expected query \"%s\" not executed during CopyGzipCsvDataFromS3().", exp)
	}
	if cmds[0] != exp {
		log.Println(cmds[0])
		t.Fatalf("Unexpected query \"%s\" executed during CopyGzipCsvDataFromS3(). Expected \"%s\"", cmds[0], exp)
	}
}

func TestCreateTable(t *testing.T) {
	ts := postgres.TableSchema{
		{3, "field3", "type3", "defaultval3", false, false},
		{1, "field1", "type1", "", true, false},
		{2, "field2", "type2", "", false, true},
	}
	exp := "CREATE TABLE tablename (field1 type1  NOT NULL, field2 type2 SORTKEY PRIMARY KEY, field3 type3 DEFAULT defaultval3 )"
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, "accesskey", "secretkey"}
	err := mockrs.CreateTable("tablename", ts)
	if err != nil {
		t.Error("Unexpected error %s while during CreateTable(). Expected query: %s", err.Error(), exp)
	}
	if len(cmds) == 0 {
		t.Fatalf("Expected query \"%s\" not executed during CreateTable().", exp)
	}
	if cmds[0] != exp {
		log.Println(cmds[0])
		t.Fatalf("Unexpected query \"%s\" executed during CreateTable(). Expected \"%s\"", cmds[0], exp)
	}
}

func TestRefreshTable(t *testing.T) {
	name, prefix, file, awsRegion, delim := "tablename", "test_prefix_", "s3://path", "testRegion", '|'
	ts := postgres.TableSchema{
		{3, "field3", "type3", "defaultval3", false, false},
		{1, "field1", "type1", "", true, false},
		{2, "field2", "type2", "", false, true},
	}
	copycmd := fmt.Sprintf("COPY %s FROM '%s' WITH REGION '%s' GZIP CSV DELIMITER '%c' IGNOREHEADER 0",
		prefix+name, file, awsRegion, delim)
	copycmd += " ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON"
	copycmd += " CREDENTIALS 'aws_access_key_id=accesskey;aws_secret_access_key=secretkey'"
	expcmds := []string{
		"DROP TABLE IF EXISTS test_prefix_tablename",
		"CREATE TABLE test_prefix_tablename (field1 type1  NOT NULL, field2 type2 SORTKEY PRIMARY KEY, field3 type3 DEFAULT defaultval3 )",
		copycmd,
		"DROP TABLE tablename; ALTER TABLE test_prefix_tablename RENAME TO tablename;",
	}
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, "accesskey", "secretkey"}
	err := mockrs.RefreshTable(name, prefix, file, awsRegion, ts, delim)
	if err != nil {
		t.Fatalf("Unexpected error %s during RefreshTables()", err.Error())
	}
	if !reflect.DeepEqual([]string(cmds), expcmds) {
		t.Fatalf("Unexpected queries during RefreshTables().\nExpected: %v\n  Actual: %v", expcmds, cmds)
	}
}
