# mixpanel
--
    import "github.com/Clever/redshifter/mixpanel"


## Usage

#### func  StringOrJSONMarshal

```go
func StringOrJSONMarshal(value interface{}) (string, error)
```
StringOrJSONMarshal returns a string value for an object to be used in URL
strings. Compare this to json.Marshal which wraps a string with quotes which
breaks URL params.

#### type Export

```go
type Export struct {
}
```

Export is a struct to perform mixpanel export operations.

#### func  NewExport

```go
func NewExport() *Export
```
NewExport returns a pointer to an Export object initialized using API key and
API secret provided in flags.

#### func (*Export) Request

```go
func (m *Export) Request(method string, params map[string]interface{}) ([]byte, error)
```
Request performs a request on the export endpoint of the mixpanel API. Returns
the raw bytes received in the response.
