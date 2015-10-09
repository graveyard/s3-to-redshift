# s3filepath
--
    import "github.com/Clever/redshifter/s3filepath"


## Usage

#### type S3Bucket

```go
type S3Bucket struct {
	C *s3.Bucket
	// info that makes more sense here
	Name      string
	Region    string
	AccessID  string
	SecretKey string
}
```

S3Bucket is our subset of the s3.Bucket class, useful for testing mostly

#### func (*S3Bucket) List

```go
func (b *S3Bucket) List(prefix, delim, marker string, max int) (result *s3.ListResp, err error)
```
List calls the underlying s3.Bucket List method

#### type S3File

```go
type S3File struct {
	// info on which file to get
	Bucket    *S3Bucket
	Schema    string
	Table     string
	JSONPaths string
	Suffix    string
	DataDate  time.Time
	ConfFile  string
}
```

S3File holds everything needed to run a COPY on the file

#### func  FindLatestInputData

```go
func FindLatestInputData(bucket *S3Bucket, schema, table, suppliedConf string, targetDate *time.Time) (S3File, error)
```
FindLatestInputData looks for the most recent file matching the prefix created
by <schema>_<table>, using the RFC3999 date in the filename

#### func (*S3File) GetDataFilename

```go
func (f *S3File) GetDataFilename() string
```
GetDataFilename returns the s3 filepath associated with an S3File useful for
redshift COPY commands, amongst other things
