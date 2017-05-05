package discovery

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"gopkg.in/Clever/kayvee-go.v3"
	"gopkg.in/Clever/kayvee-go.v3/logger"
)

const (
	templateVar = "SERVICE_%s_%s_%%s"
)

func getVar(envVar string) (string, error) {
	envVar = strings.ToUpper(envVar)
	envVar = strings.Replace(envVar, "-", "_", -1)
	val := os.Getenv(envVar)
	if val == "" {
		return "", errors.New(kayvee.FormatLog("discovery-go", kayvee.Error, "missing.env.var", logger.M{
			"var": envVar,
		}))
	}
	return val, nil
}

// URL finds the specified URL for a service based off of the service's name and which
// interface you are accessing. Values are found in environment variables fitting the scheme:
// SERVICE_{SERVICE NAME}_{INTERFACE NAME}_{PROTO,HOST,PORT}.
func URL(service, name string) (string, error) {
	proto, err := Proto(service, name)
	if err != nil {
		return "", err
	}
	host, err := Host(service, name)
	if err != nil {
		return "", err
	}
	port, err := Port(service, name)
	if err != nil {
		return "", err
	}

	rawURL := fmt.Sprintf("%s://%s:%s", proto, host, port)
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", errors.New(kayvee.FormatLog("discovery-go", kayvee.Error, "missing env var", logger.M{
			"url":   rawURL,
			"error": fmt.Errorf("Failed to parse URL: %s", err.Error()),
		}))
	}
	return u.String(), nil
}

// HostPort finds the specified host:port combo for a service based off of the service's name and
// which interface you are accessing. Values are found in environment variables fitting the scheme:
// SERVICE_{SERVICE NAME}_{INTERFACE NAME}_{PROTO,HOST,PORT}.
func HostPort(service, name string) (string, error) {
	host, err := Host(service, name)
	if err != nil {
		return "", err
	}
	port, err := Port(service, name)
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(host, port), nil
}

// Proto finds the specified protocol for a service based off of the service's name and which
// interface you are accessing. Values are found in environment variables fitting the scheme:
// SERVICE_{SERVICE NAME}_{INTERFACE NAME}_PROTO.
func Proto(service, name string) (string, error) {
	template := fmt.Sprintf(templateVar, service, name)
	return getVar(fmt.Sprintf(template, "PROTO"))
}

// Host finds the specified host for a service based off of the service's name and which
// interface you are accessing. Values are found in environment variables fitting the scheme:
// SERVICE_{SERVICE NAME}_{INTERFACE NAME}_HOST.
func Host(service, name string) (string, error) {
	template := fmt.Sprintf(templateVar, service, name)
	return getVar(fmt.Sprintf(template, "HOST"))
}

// Port finds the specified port for a service based off of the service's name and which
// interface you are accessing. Values are found in environment variables fitting the scheme:
// SERVICE_{SERVICE NAME}_{INTERFACE NAME}_PORT.
func Port(service, name string) (string, error) {
	template := fmt.Sprintf(templateVar, service, name)
	return getVar(fmt.Sprintf(template, "PORT"))
}
