# pathio
--
    import "github.com/Clever/pathio"

Package pathio is a package that allows writing to and reading from different
types of paths transparently. It supports two types of paths:

    1. Local file paths
    2. S3 File Paths (s3://bucket/key)

Note that using s3 paths requires setting two environment variables

    1. AWS_SECRET_ACCESS_KEY
    2. AWS_ACCESS_KEY_ID

## Usage

```go
var DefaultClient = &Client{}
```
DefaultClient is the default pathio client called by the Reader, Writer, and
WriteReader methods. It has S3 encryption enabled.

#### func  Reader

```go
func Reader(path string) (rc io.ReadCloser, err error)
```
Reader Calls DefaultClient's Reader method.

#### func  Write

```go
func Write(path string, input []byte) error
```
Write Calls DefaultClient's Write method.

#### func  WriteReader

```go
func WriteReader(path string, input io.ReadSeeker) error
```
WriteReader Calls DefaultClient's WriteReader method.

#### type Client

```go
type Client struct {
}
```

Client is the pathio client used to access the local file system and S3. It
includes an option to disable S3 encryption. To disable S3 encryption, create a
new Client and call it directly: `&Client{disableS3Encryption: true}.Write(...)`

#### func (*Client) Reader

```go
func (c *Client) Reader(path string) (rc io.ReadCloser, err error)
```
Reader returns an io.Reader for the specified path. The path can either be a
local file path or an S3 path. It is the caller's responsibility to close rc.

#### func (*Client) Write

```go
func (c *Client) Write(path string, input []byte) error
```
Write writes a byte array to the specified path. The path can be either a local
file path or an S3 path.

#### func (*Client) WriteReader

```go
func (c *Client) WriteReader(path string, input io.ReadSeeker) error
```
WriteReader writes all the data read from the specified io.Reader to the output
path. The path can either a local file path or an S3 path.
