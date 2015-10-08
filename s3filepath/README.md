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
	JsonPaths string
	Suffix    string
	Delimiter rune
	DataDate  time.Time
	ConfFile  string
}
```


#### func  FindLatestInputData

```go
func FindLatestInputData(s3Conn *s3.S3, bucket, schema, table, suppliedConf string, targetDate *time.Time) (S3File, error)
```
FindLatestS3FileData looks for the most recent file matching the prefix created
by <schema>_<table>, using the RFC3999 date in the filename

#### func (*S3File) GetDataFilename

```go
func (f *S3File) GetDataFilename() string
```
GetDataFilename returns the s3 filepath associated with an S3File useful for
redshift COPY commands, amongst other things
