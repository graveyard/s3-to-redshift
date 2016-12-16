package logger

import (
	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var log logger.KayveeLogger

const (
	// jobFinished refers to job completions
	jobFinished = "job-finished"
)

func init() {
	log = logger.New("s3-to-redshift")
}

// GetLogger returns a reference to the logger
func GetLogger() logger.KayveeLogger {
	return log
}

// M is an alias for map[string]interface{} to make log lines less painful to write
type M map[string]interface{}

// SetGlobalRouting installs a new log router with the input config
func SetGlobalRouting(kvconfigPath string) error {
	return logger.SetGlobalRouting(kvconfigPath)
}

// JobFinishedEvent logs when s3-to-redshift has completed
// along with payload and success/failure
func JobFinishedEvent(payload string, didSucceed bool) {
	value := 0
	if didSucceed {
		value = 1
	}
	log.GaugeIntD(jobFinished, value, M{
		"payload": payload,
		"success": didSucceed,
	})
}
