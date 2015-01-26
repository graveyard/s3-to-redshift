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
	timeout   = flag.Duration("mixpanelexporttimeout", 10*time.Minute,
		"Timeout value to be used while making a mixpanel export request. Defaults to 10 mins.")
)

type MixpanelExport struct {
	endpoint  string
	version   string
	apiKey    string
	apiSecret string
}

func NewMixpanelExport() *MixpanelExport {
	m := MixpanelExport{"https://data.mixpanel.com/api", "2.0", *apikey, *apisecret}
	return &m
}

// json.Marshal wraps a string with quotes which breaks URL params in API calls. This function fixes
// that problem.
func StringOrJsonMarshal(value interface{}) (string, error) {
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

func (m *MixpanelExport) ComputeSig(params map[string]interface{}) (string, error) {
	var keys []string

	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	joinedArgs := ""
	for _, k := range keys {
		joinedArgs += k
		value, err := StringOrJsonMarshal(params[k])
		if err != nil {
			return "", err
		}
		joinedArgs += fmt.Sprintf("=%s", value)
	}

	hash := md5.Sum([]byte(joinedArgs + m.apiSecret))
	return hex.EncodeToString(hash[:]), nil
}

func (m *MixpanelExport) Request(method string, params map[string]interface{}) ([]byte, error) {
	params["api_key"] = m.apiKey
	expireTime := time.Now().Add(*timeout)
	params["expire"] = expireTime.Unix()
	params["format"] = "json"

	delete(params, "sig")
	sig, err := m.ComputeSig(params)
	if err != nil {
		return nil, err
	}
	params["sig"] = sig

	values := url.Values{}
	for key, value := range params {
		strValue, err := StringOrJsonMarshal(value)
		if err != nil {
			return nil, err
		}
		values.Set(key, strValue)
	}
	url := fmt.Sprintf("%s/%s/%s/?%s", m.endpoint, m.version, method, values.Encode())
	log.Println(url)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}
