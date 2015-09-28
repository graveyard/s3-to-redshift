package redshift

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

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

func TestGetJSONCopySQL(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	schema, table, file, jsonpathsFile := "testschema", "tablename", "s3://path", "s3://jsonpathsfile"
	exp := "BEGIN TRANSACTION; "
	exp += fmt.Sprintf("COPY \"%s\".\"%s\" FROM '%s' WITH  JSON '%s' REGION '%s' TIMEFORMAT 'auto' COMPUPDATE ON",
		schema, table, file, jsonpathsFile, s3Info.Region)
	exp += fmt.Sprintf(" CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'", s3Info.AccessID, s3Info.SecretKey)
	exp += "; END TRANSACTION"
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, s3Info}
	sql := mockrs.GetJSONCopySQL(schema, table, file, jsonpathsFile, true, false)
	err := mockrs.SafeExec([]string{sql})
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
	ts := Table{
		Name: table,
		Columns: []ColInfo{
			{3, "field3", "type3", "defaultval3", false, false, false, 0},
			{1, "field1", "type1", "", true, false, false, 0},
			{2, "field2", "type2", "", false, true, false, 0},
		},
		Meta: Meta{},
	}
	exp := "BEGIN TRANSACTION; "
	exp += fmt.Sprintf(`COPY "%s"."%s" (%s) FROM '%s' WITH REGION '%s' GZIP CSV DELIMITER '%c'`,
		schema, table, "field1, field2, field3", file, s3Info.Region, delimiter)
	exp += " IGNOREHEADER 0 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL DATEFORMAT 'auto' ACCEPTANYDATE COMPUPDATE ON"
	exp += fmt.Sprintf(" CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'", s3Info.AccessID, s3Info.SecretKey)
	exp += "; END TRANSACTION"
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, s3Info}
	sql := mockrs.GetCSVCopySQL(schema, table, file, ts, delimiter, true, true)
	err := mockrs.SafeExec([]string{sql})
	assert.NoError(t, err)
	assert.Equal(t, mockSQLDB{exp}, cmds)
}

func TestRefreshTable(t *testing.T) {
	s3Info := S3Info{
		Region:    "testregion",
		AccessID:  "accesskey",
		SecretKey: "secretkey",
	}
	cmds := mockSQLDB([]string{})
	mockrs := Redshift{&cmds, s3Info}
	// not really testing GetCSVCopySQL so don't worry too much about this one
	ts := Table{"Test", []ColInfo{{1, "Foo", "int", "4", true, false, false, 0}}, Meta{}}
	schema, name, file, delim := "testschema", "tablename", "s3://path", '|'
	copySQL := mockrs.GetCSVCopySQL(schema, name, file, ts, delim, true, true)
	datarefreshcmds := []string{
		"BEGIN TRANSACTION",
		fmt.Sprintf(`DELETE FROM "%s"."%s"`, schema, name),
		copySQL,
		"END TRANSACTION",
	}
	expcmds := mockSQLDB{
		strings.Join(datarefreshcmds, "; "),
		`VACUUM FULL "testschema"."tablename"; ANALYZE "testschema"."tablename"`,
	}
	err := mockrs.refreshTable(schema, name, file, ts, delim)
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
