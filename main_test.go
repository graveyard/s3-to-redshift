package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeGranularity(t *testing.T) {
	baseTime := time.Date(2017, 7, 11, 12, 9, 0, 0, time.UTC)

	start, end := startEndFromGranularity(baseTime, "day")
	assert.Equal(t, start, time.Date(2017, 7, 11, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, end, time.Date(2017, 7, 12, 0, 0, 0, 0, time.UTC))

	start, end = startEndFromGranularity(baseTime, "hour")
	assert.Equal(t, start, time.Date(2017, 7, 11, 12, 0, 0, 0, time.UTC))
	assert.Equal(t, end, time.Date(2017, 7, 11, 13, 0, 0, 0, time.UTC))
}
