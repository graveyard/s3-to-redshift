# redshift
--
    import "github.com/Clever/redshifter/redshift"


## Usage

#### type ColInfo

```go
type ColInfo struct {
	Ordinal     int    `yaml:"ordinal"`
	Name        string `yaml:"dest"`
	Type        string `yaml:"type"`
	DefaultVal  string `yaml:"defaultval"`
	NotNull     bool   `yaml:"notnull"`
	PrimaryKey  bool   `yaml:"primarykey"`
	DistKey     bool   `yaml:"distkey"`
	SortOrdinal int    `yaml:"sortord"`
}
```

ColInfo is a struct that contains information about a column in a Redshift
database. SortOrdinal and DistKey only make sense for Redshift

#### type Meta

```go
type Meta struct {
	DataDateColumn string `yaml:"datadatecolumn"`
	Schema         string `yaml:"schema"`
}
```

Meta holds information that might be not in Redshift or annoying to access in
this case, we want to know the schema a table is part of and the column which
corresponds to the timestamp at which the data was gathered

#### type Redshift

```go
type Redshift struct {
}
```

Redshift wraps a dbExecCloser and can be used to perform operations on a
redshift database.

#### func  NewRedshift

```go
func NewRedshift(host, port, db, user, password string, timeout int) (*Redshift, error)
```
NewRedshift returns a pointer to a new redshift object using configuration
values passed in on instantiation and the AWS env vars we assume exist Don't
need to pass s3 info unless doing a COPY operation

#### func (*Redshift) GetTableFromConf

```go
func (r *Redshift) GetTableFromConf(f s3filepath.S3File) (Table, error)
```
it's a little awkward to turn the moSQL format into what I want, this belongs
here - redshift should not have to know about s3 files really

#### func (*Redshift) GetTableMetadata

```go
func (r *Redshift) GetTableMetadata(schema, tableName, dataDateCol string) (Table, time.Time, error)
```
GetTableMetadata looks for a table and returns both the Table representation of
the db table and the last data in the table, if that exists if the table does
not exist it returns an empty table but does not error

#### func (*Redshift) RefreshTables

```go
func (r *Redshift) RefreshTables(
	tables map[string]Table, awsRegion, awsAccessID, awsSecretKey, bucket, schema string, delim rune) error
```
RefreshTables refreshes multiple tables in parallel and returns an error if any
of the copies fail.

#### func (*Redshift) RunCSVCopy

```go
func (r *Redshift) RunCSVCopy(tx *sql.Tx, f s3filepath.S3File, ts Table, creds, gzip bool) error
```
RunCSVCopy copies gzipped CSV data from an S3 file into a redshift table this is
meant to be run in a transaction, so the first arg must be a pg.Tx

#### func (*Redshift) RunCreateTable

```go
func (r *Redshift) RunCreateTable(tx *sql.Tx, table Table) error
```

#### func (*Redshift) RunJSONCopy

```go
func (r *Redshift) RunJSONCopy(tx *sql.Tx, f s3filepath.S3File, creds, gzip bool) error
```
RunJSONCopy copies JSON data present in an S3 file into a redshift table. this
is meant to be run in a transaction, so the first arg must be a sql.Tx if not
using jsonPaths, set to "auto"

#### func (*Redshift) RunTruncate

```go
func (r *Redshift) RunTruncate(tx *sql.Tx, schema, table string) error
```
RunTruncate deletes all items from a table, given a transaction, a schema string
and a table name you shuold run vacuum and analyze soon after doing this for
performance reasons

#### func (*Redshift) RunUpdateTable

```go
func (r *Redshift) RunUpdateTable(tx *sql.Tx, targetTable, inputTable Table) error
```
only supports adding columns currently

#### func (*Redshift) VacuumAnalyze

```go
func (r *Redshift) VacuumAnalyze() error
```
VacuumAnalyze performs VACUUM FULL; ANALYZE on the redshift database. This is
useful for recreating the indices after a database has been modified and
updating the query planner.

#### func (*Redshift) VacuumAnalyzeTable

```go
func (r *Redshift) VacuumAnalyzeTable(schema, table string) error
```
VacuumAnalyzeTable performs VACUUM FULL; ANALYZE on a specific table. This is
useful for recreating the indices after a database has been modified and
updating the query planner.

#### type Table

```go
type Table struct {
	Name    string    `yaml:"dest"`
	Columns []ColInfo `yaml:"columns"`
	Meta    Meta      `yaml:"meta"`
}
```

Table is our representation of a Redshift table
