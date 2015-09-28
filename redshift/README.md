# redshift
--
    import "github.com/Clever/redshifter/redshift"


## Usage

#### type ColInfo

```go
type ColInfo struct {
	Ordinal    int    `yaml:"ordinal"`
	Name       string `yaml:"dest"`
	Type       string `yaml:"type"`
	DefaultVal string `yaml:"defaultval"`
	NotNull    bool   `yaml:"notnull"`
	PrimaryKey bool   `yaml:"primarykey"`
	DistKey    bool   `yaml:"distkey"`
	SortOrd    int    `yaml:"sortord"`
}
```

ColInfo is a struct that contains information about a column in a Redshift
database. SortKey and DistKey only make sense for Redshift

#### type Meta

```go
type Meta struct {
	DataDateColumn string `yaml:"data_date_column"`
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
func NewRedshift(host, port, db, user, password string, timeout int, s3Info S3Info) (*Redshift, error)
```
NewRedshift returns a pointer to a new redshift object using configuration
values passed in on instantiation and the AWS env vars we assume exist Don't
need to pass s3 info unless doing a COPY operation

#### func (*Redshift) GetCSVCopySQL

```go
func (r *Redshift) GetCSVCopySQL(schema, table, file string, ts Table, delimiter rune, creds, gzip bool) string
```
GetCSVCopySQL copies gzipped CSV data from an S3 file into a redshift table.

#### func (*Redshift) GetJSONCopySQL

```go
func (r *Redshift) GetJSONCopySQL(schema, table, filename, jsonPaths string, creds, gzip bool) string
```
GetJSONCopySQL copies JSON data present in an S3 file into a redshift table. if
not using jsonPaths, set to "auto"

#### func (*Redshift) GetTruncateSQL

```go
func (r *Redshift) GetTruncateSQL(schema, table string) string
```
GetTruncateSQL simply returns SQL that deletes all items from a table, given a
schema string and a table name

#### func (*Redshift) RefreshTables

```go
func (r *Redshift) RefreshTables(
	tables map[string]Table, schema, s3prefix string, delim rune) error
```
RefreshTables refreshes multiple tables in parallel and returns an error if any
of the copies fail.

#### func (*Redshift) SafeExec

```go
func (r *Redshift) SafeExec(sqlIn []string) error
```
SafeExec allows execution of SQL in a transaction block While it seems a little
dangerous to export such a powerful function, it is very difficult to control
execution control and actually effectively use this library without this ability

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

#### type S3Info

```go
type S3Info struct {
	Region    string
	AccessID  string
	SecretKey string
}
```

S3Info holds the information necessary to copy data from s3 buckets

#### type SortableColumns

```go
type SortableColumns []ColInfo
```

Just a helper to make sure the CSV copy works properly

#### func (SortableColumns) Len

```go
func (c SortableColumns) Len() int
```

#### func (SortableColumns) Less

```go
func (c SortableColumns) Less(i, j int) bool
```

#### func (SortableColumns) Swap

```go
func (c SortableColumns) Swap(i, j int)
```

#### type Table

```go
type Table struct {
	Name    string    `yaml:"dest"`
	Columns []ColInfo `yaml:"columns"`
	Meta    Meta      `yaml:"meta"`
}
```

Table is our representation of a Redshift table the main difference is an added
metadata section and YAML unmarshalling guidance
