package validator

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/Clever/kayvee-go.v3/logger"
)

// ValueType represents the valuve type of a Kayvee field, where applicable.
type ValueType string

// Kayvee value types that are explicitly checked.
const (
	StringType ValueType = "string"
	NumberType ValueType = "number"
)

var (
	requiredKayveeFields = []string{
		"source",
		"title",
		"level",
	}

	expectedValueTypes = map[string]ValueType{
		"source": StringType,
		"title":  StringType,
		"level":  StringType,
		"type":   StringType,
		"value":  NumberType,
	}

	validLevels = map[string]bool{
		logger.Debug.String():    true,
		logger.Info.String():     true,
		logger.Warning.String():  true,
		logger.Error.String():    true,
		logger.Critical.String(): true,
	}
)

// InvalidJSONError is returned by `ValidateJSONFormat` for log lines that
// aren't valid JSON objects.
type InvalidJSONError struct {
	jsonError error
}

func (e *InvalidJSONError) Error() string {
	return fmt.Sprintf("Invalid JSON: %s", e.jsonError)
}

// MissingRequiredFieldError is returned for log lines that don't contain a
// required field.
// NOTE: Certain fields are only required for specific log types.
type MissingRequiredFieldError struct {
	Field string
}

func (e *MissingRequiredFieldError) Error() string {
	return fmt.Sprintf("Missing required field `%s`", e.Field)
}

// InvalidValueError is returned for log lines containing a field with an
// invalid value.
type InvalidValueError struct {
	Field string
	Value interface{}
}

func (e *InvalidValueError) Error() string {
	return fmt.Sprintf("Field `%s` contains invalid value `%s`", e.Field, e.Value)
}

// InvalidValueTypeError is returned for log lines containing a field with an
// invalid value type.
type InvalidValueTypeError struct {
	Field        string
	ExpectedType ValueType
}

func (e *InvalidValueTypeError) Error() string {
	return fmt.Sprintf(
		"Field `%s` contains value of invalid type; expected `%s`.",
		e.Field,
		e.ExpectedType,
	)
}

// ValidateJSONFormat returns a errors if the given string is not a valid
// JSON-formatted kayvee log line.
func ValidateJSONFormat(logLine string) error {
	var kayveeData map[string]interface{}

	err := json.Unmarshal([]byte(strings.TrimSpace(logLine)), &kayveeData)
	if err != nil {
		return &InvalidJSONError{jsonError: err}
	}

	return validateKayveeData(kayveeData)
}

func validateKayveeData(kayveeData map[string]interface{}) error {
	for field, value := range kayveeData {
		switch expectedValueTypes[field] {
		case StringType:
			if _, isString := value.(string); !isString {
				return &InvalidValueTypeError{Field: field, ExpectedType: StringType}
			}
		case NumberType:
			if _, isNumber := value.(float64); !isNumber {
				return &InvalidValueTypeError{Field: field, ExpectedType: NumberType}
			}
		}
	}

	requiredFields := requiredKayveeFields

	// Add type-specific required fields, if applicable.
	if kayveeData["type"] != nil {
		logType, _ := kayveeData["type"].(string)
		switch logType {
		case "gauge":
			requiredFields = append(requiredFields, "value")
		}
	}

	for _, field := range requiredFields {
		if kayveeData[field] == nil {
			return &MissingRequiredFieldError{Field: field}
		}
	}

	// Check for a valid `level` field.
	if level, _ := kayveeData["level"].(string); !validLevels[level] {
		return &InvalidValueError{Field: "level", Value: kayveeData["level"]}
	}

	return nil
}
