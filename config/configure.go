package configure

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
)

const (
	structTagKey             = "config"
	requiredTagKey           = "required"
	missingValuesErrTemplate = "Missing required fields: %s"
)

var (
	ErrStringAndBoolOnly      = errors.New("Only string/bool values are allowed in a config struct.")
	ErrBoolCannotBeRequired   = errors.New("Boolean attributes cannot be required")
	ErrNotReference           = errors.New("The config struct must be a pointer to a struct.")
	ErrStructOnly             = errors.New("Config object must be a struct.")
	ErrNoTagValue             = errors.New("Config object attributes must have a 'config' tag value.")
	ErrTooManyTagValues       = errors.New("Config object attributes can only have a key and optional required attribute.")
	ErrFlagParsed             = errors.New("The flag library cannot be used in conjunction with configure")
	ErrInvalidJSON            = errors.New("Invalid JSON found in arguments.")
	ErrStructTagInvalidOption = errors.New("Only 'required' is a config option.")
)

// AnalyticsPipelinePayload is the standard shape of a worker payload in the analytics pipeline
type AnalyticsPipelinePayload struct {
	Current    map[string]interface{}   `json:"current"`
	Remanining []map[string]interface{} `json:"remaining"`
}

// PrintPayload prints a passed in AnalyticsPipelinePayload
func PrintPayload(payload AnalyticsPipelinePayload) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	_, err = fmt.Println(string(bytes))
	if err != nil {
		panic(err)
	}
}

// parseTagKey parses the values in a tag.
func parseTagKey(tag string) (key string, required bool, err error) {
	if tag == "" {
		return "", false, ErrNoTagValue
	}

	s := strings.Split(tag, ",")
	switch len(s) {
	case 2:
		if s[1] != requiredTagKey {
			return "", false, ErrStructTagInvalidOption
		}
		return s[0], true, nil
	case 1:
		return s[0], false, nil
	default:
		return "", false, ErrTooManyTagValues
	}
}

func createFlags(config reflect.Value, configFlags *flag.FlagSet, flagStringValueMap map[string]*string, flagBoolValueMap map[string]*bool) error {
	// this block creates flags for every attribute
	for i := 0; i < config.NumField(); i++ {
		valueField := config.Field(i)
		if !valueField.CanSet() {
			return ErrNotReference
		}

		// currently we only support strings and bools
		typedAttr := config.Type().Field(i)
		if typedAttr.Type.Kind() != reflect.String && typedAttr.Type.Kind() != reflect.Bool {
			return ErrStringAndBoolOnly
		}

		// get the name of the value and create a flag
		tagVal, _, err := parseTagKey(typedAttr.Tag.Get(structTagKey))
		if err != nil {
			return err
		}
		switch typedAttr.Type.Kind() {
		case reflect.String:
			flagStringValueMap[tagVal] = configFlags.String(tagVal, "", "generated field")
		case reflect.Bool:
			// set the default to the value passed in
			flagBoolValueMap[tagVal] = configFlags.Bool(tagVal, config.Field(i).Bool(), "generated field")
		}
	}
	return nil
}

func retrieveFlagValues(config reflect.Value, configFlags *flag.FlagSet, flagStringValueMap map[string]*string, flagBoolValueMap map[string]*bool) error {
	if err := createFlags(config, configFlags, flagStringValueMap, flagBoolValueMap); err != nil {
		return err
	}
	if err := configFlags.Parse(os.Args[1:]); err != nil {
		return err
	}
	return nil
}

func populateFromFlagMaps(config reflect.Value, flagStringValueMap map[string]*string, flagBoolValueMap map[string]*bool) (bool, error) {
	flagFound := false
	// grab values from flag map
	for i := 0; i < config.NumField(); i++ {
		valueField := config.Field(i)
		tagVal, _, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
		if err != nil {
			return flagFound, err
		}

		typedAttr := config.Type().Field(i)
		switch typedAttr.Type.Kind() {
		case reflect.String:
			if *flagStringValueMap[tagVal] != "" {
				flagFound = true
				valueField.SetString(*flagStringValueMap[tagVal])
			}
		case reflect.Bool:
			// we can only know if a bool flag was set if the default was changed
			if *flagBoolValueMap[tagVal] != config.Field(i).Bool() {
				flagFound = true
			}
			valueField.SetBool(*flagBoolValueMap[tagVal]) // always set from flags
		}
	}
	return flagFound, nil
}

func parseFromJSON(config reflect.Value, configFlags *flag.FlagSet) error {
	jsonValues := map[string]interface{}{}
	if err := json.NewDecoder(bytes.NewBufferString(configFlags.Arg(0))).Decode(&jsonValues); err != nil {
		return ErrInvalidJSON
	}

	for i := 0; i < config.NumField(); i++ {
		valueField := config.Field(i)
		tagVal, _, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
		if err != nil {
			return err
		} else if _, ok := jsonValues[tagVal]; ok {
			typedAttr := config.Type().Field(i)
			switch typedAttr.Type.Kind() {
			case reflect.String:
				valueField.SetString(jsonValues[tagVal].(string))
			case reflect.Bool:
				valueField.SetBool(jsonValues[tagVal].(bool))
			}
		}
	}

	return nil
}

func validateRequiredFields(config reflect.Value) error {
	missingRequiredFields := []string{}
	for i := 0; i < config.NumField(); i++ {
		tagKey, required, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
		if err != nil {
			return err
		} else if required {
			switch config.Field(i).Type().Kind() {
			case reflect.String:
				if config.Field(i).String() == "" {
					missingRequiredFields = append(missingRequiredFields, tagKey)
				}
			case reflect.Bool:
				return ErrBoolCannotBeRequired
			}
		}
	}
	if len(missingRequiredFields) > 0 {
		return fmt.Errorf(missingValuesErrTemplate, missingRequiredFields)
	}

	return nil
}

// AnalyticsWorker does the same as Configure, except JSON input is parsed differently.
// Instead of containing just the structure of configStruct, JSON is expected to have a "current"
// object that matches configStruct and an array of "remaining" payloads for future workers in the
// workflow. Remaining payloads are returned as a printable []byte.
func AnalyticsWorker(configStruct interface{}) (*AnalyticsPipelinePayload, error) {
	if flag.Parsed() {
		return nil, ErrFlagParsed
	}

	reflectConfig := reflect.ValueOf(configStruct)
	if reflectConfig.Kind() != reflect.Ptr {
		return nil, ErrStructOnly
	}

	var (
		configFlags        = flag.NewFlagSet("configure", flag.ContinueOnError)
		flagStringValueMap = map[string]*string{} // holds references to attribute string flags
		flagBoolValueMap   = map[string]*bool{}   // holds references to attribute bool flags
		config             = reflectConfig.Elem()
	)

	if err := retrieveFlagValues(config, configFlags, flagStringValueMap, flagBoolValueMap); err != nil {
		return nil, err
	}

	flagFound, err := populateFromFlagMaps(config, flagStringValueMap, flagBoolValueMap)
	if err != nil {
		return nil, err
	}

	// if no flags were found and we have a value in the first arg, we try to parse JSON from it.
	analyticsPayload := AnalyticsPipelinePayload{}
	if !flagFound && configFlags.Arg(0) != "" {
		if err := json.NewDecoder(bytes.NewBufferString(configFlags.Arg(0))).Decode(&analyticsPayload); err != nil {
			return nil, ErrInvalidJSON
		}

		for i := 0; i < config.NumField(); i++ {
			valueField := config.Field(i)
			tagVal, _, err := parseTagKey(config.Type().Field(i).Tag.Get(structTagKey))
			if err != nil {
				return nil, err
			} else if _, ok := analyticsPayload.Current[tagVal]; ok {
				typedAttr := config.Type().Field(i)
				switch typedAttr.Type.Kind() {
				case reflect.String:
					valueField.SetString(analyticsPayload.Current[tagVal].(string))
				case reflect.Bool:
					valueField.SetBool(analyticsPayload.Current[tagVal].(bool))
				}
			}
		}
	}

	// validate that all required fields were set
	if err := validateRequiredFields(config); err != nil {
		return nil, err
	}

	result := AnalyticsPipelinePayload{}
	if len(analyticsPayload.Remanining) > 0 {
		result.Current = analyticsPayload.Current
		result.Remanining = analyticsPayload.Remanining[1:]
	}

	return &result, nil
}
