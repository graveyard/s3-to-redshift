package logger

import (
	l "log"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

func init() {
	err := logger.SetGlobalRouting("../kvconfig.yml")
	if err != nil {
		l.Fatal(err)
	}
}

// TestJobFinished verifies that JobFinishedEvent
// log routes to the 'job-finished' rule
func TestJobFinished(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		rule       string
		payload    string
		didSucceed bool
	}{
		{
			rule:       "job-finished",
			payload:    "--schema api --tables business_metrics_auth_counts",
			didSucceed: true,
		},
		{
			rule:       "job-finished",
			payload:    "--schema api --tables business_metrics_auth_counts",
			didSucceed: false,
		},
	}

	for _, test := range tests {
		t.Logf("Routing rule %s", test.rule)

		mocklog := logger.NewMockCountLogger("s3-to-redshift")
		log = mocklog // Overrides package level logger

		JobFinishedEvent(test.payload, test.didSucceed)
		counts := mocklog.RuleCounts()

		assert.Equal(counts[test.rule], 1)
	}
}
