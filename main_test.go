package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeGranularity(t *testing.T) {
	baseTime := time.Date(2017, 7, 11, 12, 9, 0, 0, time.UTC)

	start, end := startEndFromGranularity(baseTime, "day", "UTC")
	assert.Equal(t, start, time.Date(2017, 7, 11, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, end, time.Date(2017, 7, 12, 0, 0, 0, 0, time.UTC))

	start, end = startEndFromGranularity(baseTime, "hour", "UTC")
	assert.Equal(t, start, time.Date(2017, 7, 11, 12, 0, 0, 0, time.UTC))
	assert.Equal(t, end, time.Date(2017, 7, 11, 13, 0, 0, 0, time.UTC))

	// Simulate timestamps that cross timezones in PT vs UTC
	baseTime = time.Date(2017, 7, 11, 4, 0, 0, 0, time.UTC)

	start, end = startEndFromGranularity(baseTime, "day", "UTC")
	assert.Equal(t, start, time.Date(2017, 7, 11, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, end, time.Date(2017, 7, 12, 0, 0, 0, 0, time.UTC))

	start, end = startEndFromGranularity(baseTime, "day", "America/Los_Angeles")
	assert.Equal(t, start, time.Date(2017, 7, 10, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, end, time.Date(2017, 7, 11, 0, 0, 0, 0, time.UTC))
}

func TestIsInputDataStale(t *testing.T) {
	locationUTC, _ := time.LoadLocation("UTC")
	inputDataDate, _ := time.Parse(time.RFC3339, "2017-08-15T14:00:00Z")
	targetDataDate, _ := time.Parse(time.RFC3339, "2017-08-15T21:00:00Z")

	assert.Equal(t, false, isInputDataStale(inputDataDate, &targetDataDate, "day", locationUTC))
	assert.Equal(t, true, isInputDataStale(inputDataDate, &targetDataDate, "hour", locationUTC))
	assert.Equal(t, false, isInputDataStale(inputDataDate, nil, "hour", locationUTC))

	// Simulate Redshift timestamp without time zones that is parsed as UTC but is actually PT
	locationPT, _ := time.LoadLocation("America/Los_Angeles")
	targetDataDatePT, _ := time.Parse(time.RFC3339, "2017-08-15T14:00:00Z")
	inputDataDateUTC, _ := time.Parse(time.RFC3339, "2017-08-15T14:00:00Z")

	assert.Equal(t, false, isInputDataStale(inputDataDateUTC, &targetDataDatePT, "hour", locationUTC))
	assert.Equal(t, true, isInputDataStale(inputDataDateUTC, &targetDataDatePT, "hour", locationPT))

	// Test Redshift timestamp without time zones where UTC and PT are on different days
	targetDataDatePT, _ = time.Parse(time.RFC3339, "2017-08-15T23:00:00Z")
	inputDataDateUTC, _ = time.Parse(time.RFC3339, "2017-08-15T14:00:00Z")

	assert.Equal(t, false, isInputDataStale(inputDataDateUTC, &targetDataDatePT, "day", locationUTC))
	assert.Equal(t, true, isInputDataStale(inputDataDateUTC, &targetDataDatePT, "day", locationPT))
}
