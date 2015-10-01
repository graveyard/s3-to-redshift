# s3filepath
--
    import "github.com/Clever/redshifter/s3filepath"


## Usage

#### type S3File

```go
type S3File struct {
	// info needed to access
	Region    string
	AccessID  string
	SecretKey string
	// info on which file to get
	Bucket    string // decide to keep this as string for simplicity
	Schema    string
	Table     string
	Filename  string
	JsonPaths string
	Delimiter rune
	DataDate  time.Time
}
```


#### func  FindLatestInputData

```go
func FindLatestInputData(s3Conn *s3.S3, bucket, schema, table string, beforeDate time.Time) (S3File, error)
```
FindLatestS3FileData looks for the most recent file matching the prefix created
by <schema>_<table> since the date passed in, using the RFC3999 date in the
filename

#### func (*S3File) GetConfigFilename

```go
func (f *S3File) GetConfigFilename() string
```

#### func (*S3File) GetDataFilename

```go
func (f *S3File) GetDataFilename() string
```
