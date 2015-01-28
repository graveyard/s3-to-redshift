package mixpanel

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

type marshalTestPair struct {
	value    interface{}
	strvalue string
}

func TestStringOrJSONMarshal(t *testing.T) {
	tests := []marshalTestPair{
		{"teststring", "teststring"},
		{map[string]string{"key1": "val1", "key2:": "val2"}, "{\"key1\":\"val1\",\"key2:\":\"val2\"}"},
		{[]string{"arrary", "of", "strings"}, "[\"arrary\",\"of\",\"strings\"]"},
	}
	for _, pair := range tests {
		v, err := StringOrJSONMarshal(pair.value)
		if err != nil {
			t.Errorf("Unexpected error %s for StringOrJSONMarshal(%+v). Expected %s", err.Error(), pair.value, pair.strvalue)
		}
		if v != pair.strvalue {
			t.Errorf("Unexpected value %s for StringOrJSONMarshal(%+v). Expected %s", v, pair.value, pair.strvalue)
		}
	}
}

func TestComputeSig(t *testing.T) {
	params := map[string]interface{}{
		"api_key": "TESTMIXPANELAPIKEY",
		"expire":  12312324324,
		"events":  []string{"event1", "event2"},
		"param1":  "value1",
	}
	m, exp := Export{apiSecret: "TESTMIXPANELAPISECRET"}, "1beb8d4f61da24302844e252a8ff6e75"
	sig, err := m.computeSig(params)
	if err != nil {
		t.Errorf("Unexpected error %s for computeSign(%+v). Expected %s", err.Error(), params, exp)
	}
	if sig != exp {
		t.Errorf("Unexpected value %s for computeSign(%+v). Expected %s", sig, params, exp)
	}
}

func mockHTTPGet(url string) (*http.Response, error) {
	reader := bytes.NewReader([]byte(url))
	mockbody := ioutil.NopCloser(reader)
	return &http.Response{Body: mockbody}, nil
}

func mockTimeGetter() time.Time {
	return time.Unix(10000000, 0)
}

func TestRequest(t *testing.T) {
	m := Export{"https://data.mixpanel.com/api", "2.0", "apikey", "apisecret", mockHTTPGet, mockTimeGetter}
	params := map[string]interface{}{
		"events": []string{"event1", "event2"},
		"param1": "value1",
		"param2": "value2",
	}
	expurl := "https://data.mixpanel.com/api/2.0/method/?api_key=apikey&events=%5B%22event1%22%2C%22event2%22%5D&expire=10000600&format=json&param1=value1&param2=value2&sig=825125f9640ce6bee7b6c25fd33eacb3"
	url, err := m.Request("method", params)
	if err != nil {
		t.Errorf("Unexpected error %s for Request(%+v, %+v). Expected %s", err.Error(), "method", params, expurl)
	}
	if string(url) != expurl {
		t.Errorf("Unexpected URL %s for Request(%+v, %+v). Expected %s", url, "method", params, expurl)
	}
}
