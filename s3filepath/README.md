# s3filepath
--
    import "github.com/Clever/s3-to-redshift/s3filepath"


## Usage

#### type PathChecker

```go
type PathChecker interface {
	FileExists(path string) bool
}
```

PathChecker is the interface for determining if a path in S3 exists, which
allows DI for testing.

#### type S3Bucket

```go
type S3Bucket struct {
	Name      string
	Region    string
	AccessID  string
	SecretKey string
}
```

S3Bucket is our subset of the s3.Bucket class, useful for testing mostly

#### type S3File

```go
type S3File struct {
	// info on which file to get
	Bucket    S3Bucket
	Schema    string
	Table     string
	JSONPaths string
	Suffix    string
	DataDate  time.Time
	ConfFile  string
}
```

S3File holds everything needed to run a COPY on the file

#### func  CreateS3File

```go
func CreateS3File(pc PathChecker, bucket S3Bucket, schema, table, suppliedConf string, date time.Time) (*S3File, error)
```
CreateS3File creates an S3File object with either a supplied config file or the
function generates a config file name

#### func (*S3File) GetDataFilename

```go
func (f *S3File) GetDataFilename() string
```
GetDataFilename returns the s3 filepath associated with an S3File useful for
redshift COPY commands, amongst other things

#### type S3PathChecker

```go
type S3PathChecker struct{}
```

S3PathChecker will use pathio to determine if the path actually exists in S3,
and will be used in prod.

#### func (S3PathChecker) FileExists

```go
func (S3PathChecker) FileExists(path string) bool
```
FileExists looks up if the file exists in S3 using the pathio.Reader method
