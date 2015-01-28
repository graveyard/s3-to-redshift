package mixpanel

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"time"
)

var (
	//TODO: Use flag validators
	apikey    = flag.String("mixpanelapikey", "", "API Key for Mixpanel.")
	apisecret = flag.String("mixpanelapisecret", "", "API Secret for Mixpanel")
	timeout   = flag.Duration("Exporttimeout", 10*time.Minute,
		"Timeout value to be used while making a mixpanel export request. Defaults to 10 mins.")
)

// Export is a struct to perform mixpanel export operations.
type Export struct {
	endpoint   string
	version    string
	apiKey     string
	apiSecret  string
	httpGetter func(string) (*http.Response, error)
	nowGetter  func() time.Time
}

// NewExport returns a pointer to an Export object initialized using API key and API secret provided
// in flags.
func NewExport() *Export {
	m := Export{"https://data.mixpanel.com/api", "2.0", *apikey, *apisecret, http.Get, time.Now}
	return &m
}

// StringOrJSONMarshal returns a string value for an object to be used in URL strings. Compare this
// to json.Marshal which wraps a string with quotes which breaks URL params.
func StringOrJSONMarshal(value interface{}) (string, error) {
	strValue, ok := value.(string)
	if !ok {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		strValue = string(jsonValue)
	}
	return strValue, nil
}

func (m *Export) computeSig(params map[string]interface{}) (string, error) {
	var keys []string

	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	joinedArgs := ""
	for _, k := range keys {
		joinedArgs += k
		value, err := StringOrJSONMarshal(params[k])
		if err != nil {
			return "", err
		}
		joinedArgs += fmt.Sprintf("=%s", value)
	}

	hash := md5.Sum([]byte(joinedArgs + m.apiSecret))
	return hex.EncodeToString(hash[:]), nil
}

// Request performs a request on the export endpoint of the mixpanel API. Returns the raw bytes
// received in the response.
func (m *Export) Request(method string, params map[string]interface{}) ([]byte, error) {
	params["api_key"] = m.apiKey
	expireTime := m.nowGetter().Add(*timeout)
	params["expire"] = expireTime.Unix()
	params["format"] = "json"

	delete(params, "sig")
	sig, err := m.computeSig(params)
	if err != nil {
		return nil, err
	}
	params["sig"] = sig

	values := url.Values{}
	for key, value := range params {
		strValue, err := StringOrJSONMarshal(value)
		if err != nil {
			return nil, err
		}
		values.Set(key, strValue)
	}
	url := fmt.Sprintf("%s/%s/%s/?%s", m.endpoint, m.version, method, values.Encode())
	log.Println(url)
	resp, err := m.httpGetter(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}
