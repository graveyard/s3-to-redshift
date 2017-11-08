# validator
--
    import "gopkg.in/Clever/kayvee-go.v3/validator"


## Usage

#### func  ValidateJSONFormat

```go
func ValidateJSONFormat(logLine string) error
```
ValidateJSONFormat returns a errors if the given string is not a valid
JSON-formatted kayvee log line.

#### type InvalidJSONError

```go
type InvalidJSONError struct {
}
```

InvalidJSONError is returned by `ValidateJSONFormat` for log lines that aren't
valid JSON objects.

#### func (*InvalidJSONError) Error

```go
func (e *InvalidJSONError) Error() string
```

#### type InvalidValueError

```go
type InvalidValueError struct {
	Field string
	Value interface{}
}
```

InvalidValueError is returned for log lines containing a field with an invalid
value.

#### func (*InvalidValueError) Error

```go
func (e *InvalidValueError) Error() string
```

#### type InvalidValueTypeError

```go
type InvalidValueTypeError struct {
	Field        string
	ExpectedType ValueType
}
```

InvalidValueTypeError is returned for log lines containing a field with an
invalid value type.

#### func (*InvalidValueTypeError) Error

```go
func (e *InvalidValueTypeError) Error() string
```

#### type MissingRequiredFieldError

```go
type MissingRequiredFieldError struct {
	Field string
}
```

MissingRequiredFieldError is returned for log lines that don't contain a
required field. NOTE: Certain fields are only required for specific log types.

#### func (*MissingRequiredFieldError) Error

```go
func (e *MissingRequiredFieldError) Error() string
```

#### type ValueType

```go
type ValueType string
```

ValueType represents the valuve type of a Kayvee field, where applicable.

```go
const (
	StringType ValueType = "string"
	NumberType ValueType = "number"
)
```
Kayvee value types that are explicitly checked.
