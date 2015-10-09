package redshift

import (
	"fmt"
	"log"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

// test create and getColumnSQL at the same time
// getColumnSQL is too simple to be worth testing independently
func TestCreateTable(t *testing.T) {
	schema, table := "testschema", "tablename"
	dbTable := Table{
		Name: table,
		// order incorrectly on purpose to ensure ordering works
		Columns: []ColInfo{
			{2, "test1", "int", "100", true, false, true, 1},
			{1, "id", "text", "", false, true, false, 0},
		},
		Meta: Meta{Schema: schema},
	}
	createSQL := `id character varying(256) PRIMARY KEY , test1 int DEFAULT 100 NOT NULL SORTKEY DISTKEY`
	sql := fmt.Sprintf(`CREATE TABLE "%s"."%s" (%s)`, schema, table, createSQL)
	regex := `CREATE TABLE ".*".".*" (.*)` // a little awk, but the prepare makes sure this is good

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mockrs := Redshift{db}

	mock.ExpectBegin()
	mock.ExpectPrepare(sql)
	mock.ExpectExec(regex).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunCreateTable(tx, dbTable))
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
			{3, "test3", "boolean", "true", false, false, false, 0},
			{2, "test2", "int", "100", true, false, true, 1},
			{1, "id", "character varying(256)", "", false, true, false, 0},
		},
		Meta: Meta{Schema: schema},
	}

	inputTable := Table{
		Name: table,
		// order incorrectly on purpose to ensure ordering works
		Columns: []ColInfo{
			{3, "test3", "boolean", "true", false, false, false, 0},
			{2, "test2", "int", "100", true, false, true, 1},
			{1, "id", "text", "", false, true, false, 0},
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
	assert.NoError(t, mockrs.RunUpdateTable(tx, targetTable, inputTable))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	// test regular update
	updateSQL := `ADD COLUMN id character varying(256) PRIMARY KEY , ADD COLUMN test2 int DEFAULT 100 NOT NULL SORTKEY DISTKEY`
	sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" (%s)`, schema, table, updateSQL)
	regex := `ALTER TABLE ".*".".*" (.*)` // a little awk, but the prepare makes sure this is good

	mock.ExpectBegin()
	mock.ExpectPrepare(sql)
	mock.ExpectExec(regex).WithArgs().WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	fewerColumnsTargetTable := Table{
		Name: table,
		Columns: []ColInfo{
			{3, "test3", "boolean", "true", false, false, false, 0},
		},
		Meta: Meta{Schema: schema},
	}
	tx, err = mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunUpdateTable(tx, fewerColumnsTargetTable, inputTable))
	assert.NoError(t, tx.Commit())

	// test extra columns (no error currently)
	fewerColumnsInputTable := Table{
		Name: table,
		// order incorrectly on purpose to ensure ordering works
		Columns: []ColInfo{
			{1, "id", "text", "", false, true, false, 0},
		},
		Meta: Meta{Schema: schema},
	}
	mock.ExpectBegin()
	mock.ExpectCommit()

	tx, err = mockrs.Begin()
	assert.NoError(t, err)
	assert.NoError(t, mockrs.RunUpdateTable(tx, targetTable, fewerColumnsInputTable))
	assert.NoError(t, tx.Commit())

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}

	// test mismatching columns (does error)
	// each one is one item off from: {1, "id", "text", "", false, true, false, 0},
	for _, c := range []ColInfo{
		{2, "id", "text", "", false, true, false, 0},
		{1, "id", "boolean", "", false, true, false, 0},
		{1, "id", "text", "foo", false, true, false, 0},
		{1, "id", "text", "", true, true, false, 0},
		{1, "id", "text", "", false, false, false, 0},
		{1, "id", "text", "", false, true, true, 0},
		{1, "id", "text", "", false, true, false, 1},
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
		err = mockrs.RunUpdateTable(tx, targetTable, mismatchingColInputTable)
		log.Println("mismatch err: ", err)
		assert.Error(t, err)
		assert.NoError(t, tx.Commit())

		if err = mock.ExpectationsWereMet(); err != nil {
			t.Errorf("there were unfulfilled expections: %s", err)
		}
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
