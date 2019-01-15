package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	expectedDistrict   = "abc123"
	expectedCollection = "schools"
)

var (
	errMissingDistrictField = fmt.Errorf(missingValuesErrTemplate, []string{"district_id"})
	emptyCurrent            = []byte("{}")
	emptyRemaining          = []byte("[]")
)

func TestAnalyticsWorker(t *testing.T) {
	for _, spec := range []struct {
		context      string
		args         []string
		err          error
		district     string
		collection   string
		newCurrent   []byte
		newRemaining []byte
	}{
		{
			context:      "normal case w/ flags",
			args:         []string{"-district_id=abc123"},
			district:     expectedDistrict,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
		},
		{
			context: "missing required field",
			err:     errMissingDistrictField,
		},
		{
			context: "given other field but not required field",
			args:    []string{"-collection=schools"},
			err:     errMissingDistrictField,
		},
		{
			context:      "normal case w/ json",
			args:         []string{`{ "current": {"district_id":"abc123"}, "remaining": [] }`},
			district:     expectedDistrict,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
		},
		{
			context:      "unwrapped json",
			args:         []string{`{"district_id":"abc123"}`},
			district:     expectedDistrict,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
		},
		{
			context:      "json w/ all fields",
			args:         []string{`{"current": {"district_id":"abc123","collection":"schools"},"remaining":[{}]}`},
			district:     expectedDistrict,
			collection:   expectedCollection,
			newCurrent:   emptyCurrent,
			newRemaining: emptyRemaining,
		},
		{
			context:      "json w/ remaining",
			args:         []string{`{"current": {"district_id":"abc123","collection":"schools"},"remaining":[{"district_id":"abc456"}]}`},
			district:     expectedDistrict,
			collection:   expectedCollection,
			newCurrent:   []byte("{\"district_id\":\"abc456\"}"),
			newRemaining: emptyRemaining,
		},
		{
			context:      "json w/ remaining array",
			args:         []string{`{"current": {"district_id":"abc123","collection":"schools"},"remaining":[{"district_id":"abc456"},{"district_id":"abc789"}]}`},
			district:     expectedDistrict,
			collection:   expectedCollection,
			newCurrent:   []byte("{\"district_id\":\"abc456\"}"),
			newRemaining: []byte("[{\"district_id\":\"abc789\"}]"),
		},
		{
			context: "empty JSON blob",
			err:     errMissingDistrictField,
		},
		{
			context: "fails with broken JSON",
			args:    []string{`{"collection":"not closed, oops"`},
			err:     ErrInvalidJSON,
		},
		{
			context: "only evaluates flags if provided first",
			args:    []string{"-collection=schools", `{"district_id":"abc123"}`},
			err:     errMissingDistrictField,
		},
		{
			context: "fails with non-declared flags",
			args:    []string{"-district_id=abc123", "-random-test-flag=X"},
			err:     errors.New("flag provided but not defined: -random-test-flag"),
		},
	} {
		// NOTE: we override both the os.Args and flag.Commandline variables to allow
		// repeated calls to the flag library.
		os.Args = append([]string{"test"}, spec.args...)
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

		var config struct {
			DistrictID string `config:"district_id,required"`
			Collection string `config:"collection"`
		}
		newPayload, err := AnalyticsWorker(&config)
		if spec.err == nil {
			assert.NoError(t, err, "Case '%s'", spec.context)
			assert.Equal(t, spec.district, config.DistrictID, "Case '%s'", spec.context)
			assert.Equal(t, spec.collection, config.Collection, "Case '%s'", spec.context)
			retrievedCurrent, err := json.Marshal(newPayload.Current)
			assert.NoError(t, err, "Case '%s'", spec.context)
			assert.Equal(t, spec.newCurrent, retrievedCurrent)
			retrievedRemaining, err := json.Marshal(newPayload.Remanining)
			assert.NoError(t, err, "Case '%s'", spec.context)
			assert.Equal(t, spec.newRemaining, retrievedRemaining)
		} else {
			assert.Equal(t, spec.err, err, "Case '%s'", spec.context)
		}
	}
}
