# s3filepath
--
    import "github.com/Clever/redshifter/s3filepath"


## Usage

#### type Bucketer

```go
type Bucketer interface {
	List(prefix, delim, marker string, max int) (result *s3.ListResp, err error)
	Name() string
	Region() string
	AccessID() string
	SecretKey() string
}
```


#### type S3Bucket

```go
type S3Bucket struct {
	s3.Bucket
	// info that makes more sense here
	BucketName      string
	BucketRegion    string
	BucketAccessID  string
	BucketSecretKey string
}
```

S3Bucket is our subset of the s3.Bucket class, useful for testing mostly

#### func (*S3Bucket) AccessID

```go
func (b *S3Bucket) AccessID() string
```
AccessID returns the name

#### func (*S3Bucket) List

```go
func (b *S3Bucket) List(prefix, delim, marker string, max int) (result *s3.ListResp, err error)
```
List calls the underlying s3.Bucket List method

#### func (*S3Bucket) Name

```go
func (b *S3Bucket) Name() string
```
All of these simple accessors are for testing Name returns the name

#### func (*S3Bucket) Region

```go
func (b *S3Bucket) Region() string
```
Region returns the name

#### func (*S3Bucket) SecretKey

```go
func (b *S3Bucket) SecretKey() string
```
SecretKey returns the name

#### type S3File

```go
type S3File struct {
	// info on which file to get
	Bucket    Bucketer
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
func FindLatestInputData(bucket Bucketer, schema, table, suppliedConf string, targetDate *time.Time) (*S3File, error)
```
FindLatestInputData looks for the most recent file matching the prefix created
by <schema>_<table>, using the RFC3999 date in the filename

#### func (*S3File) GetDataFilename

```go
func (f *S3File) GetDataFilename() string
```
GetDataFilename returns the s3 filepath associated with an S3File useful for
redshift COPY commands, amongst other things
