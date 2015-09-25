package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/Clever/redshifter/postgres"
	"github.com/stretchr/testify/assert"
)

type mockSQLDB []string

func (m *mockSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	*m = mockSQLDB(append([]string(*m), fmt.Sprintf(query)))
	return nil, nil
}

func (m *mockSQLDB) Close() error {
	return nil
}

func TestCopyJSONDataFromS3(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	schema, table, file, jsonpathsFile := "testschema", "tablename", "s3://path", "s3://jsonpathsfile"
	exp := fmt.Sprintf("COPY \"%s\".\"%s\" FROM '%s' WITH json '%s' region '%s' timeformat 'epochsecs' COMPUPDATE ON",
		schema, table, file, jsonpathsFile, s3Info.Region)
	exp += fmt.Sprintf(" CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'", s3Info.AccessID, s3Info.SecretKey)
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, s3Info}
	err := mockrs.CopyJSONDataFromS3(schema, table, file, jsonpathsFile)
	assert.NoError(t, err)
	assert.Equal(t, mockSQLDB{exp}, cmds)
}

func TestCopyGzipCsvDataFromS3(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	schema, table, file, delimiter := "testschema", "tablename", "s3://path", '|'
	ts := postgres.TableSchema{
		{3, "field3", "type3", "defaultval3", false, false},
		{1, "field1", "type1", "", true, false},
		{2, "field2", "type2", "", false, true},
	}
	exp := fmt.Sprintf(`COPY "%s"."%s" (%s) FROM '%s' WITH REGION '%s' GZIP CSV DELIMITER '%c'`,
		schema, table, "field1, field2, field3", file, s3Info.Region, delimiter)
	exp += " IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON"
	exp += fmt.Sprintf(" CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'", s3Info.AccessID, s3Info.SecretKey)
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, s3Info}
	err := mockrs.CopyGzipCsvDataFromS3(schema, table, file, ts, delimiter)
	assert.NoError(t, err)
	assert.Equal(t, mockSQLDB{exp}, cmds)
}

func TestCreateTable(t *testing.T) {
	tmpschema, schema, name := "testtmpschema", "testschema", "testtable"
	exp := fmt.Sprintf(`CREATE TABLE "%s"."%s" (LIKE "%s"."%s")`, tmpschema, name, schema, name)
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, S3Info{}}
	err := mockrs.createTempTable(tmpschema, schema, name)
	assert.NoError(t, err)
	assert.Equal(t, mockSQLDB{exp}, cmds)
}

func TestRefreshTable(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	schema, name, tmpschema, file, delim := "testschema", "tablename", "testtmpschema", "s3://path", '|'
	ts := postgres.TableSchema{
		{3, "field3", "type3", "defaultval3", false, false},
		{1, "field1", "type1", "", true, false},
		{2, "field2", "type2", "", false, true},
	}
	copycmd := fmt.Sprintf(`COPY "%s"."%s" (%s) FROM '%s' WITH REGION '%s' GZIP CSV DELIMITER '%c'`,
		tmpschema, name, "field1, field2, field3", file, s3Info.Region, delim)
	copycmd += " IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON"
	copycmd += fmt.Sprintf(" CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'", s3Info.AccessID, s3Info.SecretKey)
	datarefreshcmds := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM "%s"."%s"`, schema, name),
		fmt.Sprintf(`INSERT INTO "%s"."%s" (SELECT * FROM "%s"."%s")`, schema, name, tmpschema, name),
		"END TRANSACTION",
	}
	expcmds := mockSQLDB{
		fmt.Sprintf(`CREATE TABLE "%s"."%s" (LIKE "%s"."%s")`, tmpschema, name, schema, name),
		copycmd,
		strings.Join(datarefreshcmds, "; "),
		`VACUUM FULL "testschema"."tablename"; ANALYZE "testschema"."tablename"`,
	}
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, s3Info}
	err := mockrs.refreshTable(schema, name, tmpschema, file, ts, delim)
	assert.NoError(t, err)
	assert.Equal(t, expcmds, cmds)
}

func TestVacuumAnalyzeTable(t *testing.T) {
	schema, table := "testschema", "tablename"
	expcmds := mockSQLDB{`VACUUM FULL "testschema"."tablename"; ANALYZE "testschema"."tablename"`}
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, S3Info{}}
	err := mockrs.VacuumAnalyzeTable(schema, table)
	assert.NoError(t, err)
	assert.Equal(t, expcmds, cmds)
}
