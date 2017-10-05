package config

import (
	"fmt"
	"log"
	"os"

	"github.com/Clever/configure"
	discovery "github.com/Clever/discovery-go"
	env "github.com/segmentio/go-env"
)

var (
	// things we are likely to change when running the worker normally are flags
	InputSchemaName string
	InputTables     string
	InputBucket     string
	Truncate        bool
	Force           bool
	DataDate        string
	ConfigFile      string
	Gzip            bool
	Delimiter       string
	TimeGranularity string
	StreamStart     string
	StreamEnd       string
	TargetTimezone  string

	// things which will would strongly suggest launching as a second worker are env vars
	// also the secrets ... shhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh
	Host            string //= os.Getenv("REDSHIFT_HOST")
	Port            string //= os.Getenv("REDSHIFT_PORT")
	DbName          string //= env.MustGet("REDSHIFT_DB")
	User            string //= env.MustGet("REDSHIFT_USER")
	Pwd             string //= env.MustGet("REDSHIFT_PASSWORD")
	RedshiftRoleARN string //= env.MustGet("REDSHIFT_ROLE_ARN")
	VacuumWorker    string //= env.MustGet("VACUUM_WORKER")

	// payloadForSignalFx holds a subset of the job payload that
	// we want to alert on as a dimension in SignalFx.
	// This is necessary because we would like to selectively group
	// on job parameters - schema but not date, for instance, since
	// logging the date would overwhelm SignalFx
	PayloadForSignalFx string

	GearmanAdminURL string
)

type cliConfig struct {
	InputSchemaName string `config:"schema"`
	InputTables     string `config:"tables"`
	InputBucket     string `config:"bucket"`
	Truncate        bool   `config:"truncate"`
	Force           bool   `config:"force"`
	DataDate        string `config:"date"`
	ConfigFile      string `config:"config"`
	Gzip            bool   `config:"gzip"`
	Delimiter       string `config:"delimiter"`
	TimeGranularity string `config:"granularity"`
	StreamStart     string `config:"streamStart"`
	StreamEnd       string `config:"streamEnd"`
	TargetTimezone  string `config:"timezone"`
}

func Parse() {
	argConfig := cliConfig{
		InputSchemaName: "mongo",
		InputBucket:     "metrics",
		Truncate:        false,
		Gzip:            true,
		TargetTimezone:  "UTC",
	}
	if err := configure.Configure(&argConfig); err != nil {
		log.Fatalf("err: %#v", err)
	}

	fmt.Printf("%+v\n", argConfig)
	os.Exit(1)

	// copy flags to config package vars
	InputSchemaName = argConfig.InputSchemaName
	InputTables = argConfig.InputTables
	InputBucket = argConfig.InputBucket
	Truncate = argConfig.Truncate
	Force = argConfig.Force
	DataDate = argConfig.DataDate
	ConfigFile = argConfig.ConfigFile
	Gzip = argConfig.Gzip
	Delimiter = argConfig.Delimiter
	TimeGranularity = argConfig.TimeGranularity
	StreamStart = argConfig.StreamStart
	StreamEnd = argConfig.StreamEnd
	TargetTimezone = argConfig.TargetTimezone

	if Host == "" {
		Host = "localHost"
	}
	if Port == "" {
		Port = "5439"
	}

	gearmanAdminUser := env.MustGet("GEARMAN_ADMIN_USER")
	gearmanAdminPass := env.MustGet("GEARMAN_ADMIN_PASS")
	gearmanAdminPath := env.MustGet("GEARMAN_ADMIN_PATH")
	GearmanAdminURL = generateServiceEndpoint(gearmanAdminUser, gearmanAdminPass, gearmanAdminPath)
}

func generateServiceEndpoint(user, pass, path string) string {
	hostPort, err := discovery.HostPort("gearman-admin", "http")
	if err != nil {
		log.Fatal(err)
	}
	proto, err := discovery.Proto("gearman-admin", "http")
	if err != nil {
		log.Fatal(err)
	}

	return fmt.Sprintf("%s://%s:%s@%s%s", proto, user, pass, hostPort, path)
}
