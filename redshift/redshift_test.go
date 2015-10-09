package redshift

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

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
