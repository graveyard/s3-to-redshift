include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: build test golint docs $(PKG) $(PKGS) vendor
SHELL := /bin/bash
PKG := github.com/Clever/s3-to-redshift
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE := $(shell basename $(PKG))
$(eval $(call golang-version-check,1.8))

# variables for testing
export GEARMAN_ADMIN_PATH ?= x
export GEARMAN_ADMIN_USER ?= x
export GEARMAN_ADMIN_PASS ?= x
export VACUUM_WORKER ?= x
export REDSHIFT_PASSWORD ?= x
export REDSHIFT_USER ?= x
export REDSHIFT_DB ?= x
export SERVICE_GEARMAN_ADMIN_HTTP_HOST ?= x
export SERVICE_GEARMAN_ADMIN_HTTP_PORT ?= x
export SERVICE_GEARMAN_ADMIN_HTTP_PROTO ?= x
export AWS_REGION ?= x
export REDSHIFT_ROLE_ARN ?= x


all: test build

clean:
	rm -f $(GOPATH)/src/$(PKG)/bin/$(EXECUTABLE)
	rm -f $(GOPATH)/src/$(PKG)/bin/kvconfig.yml

build: clean
	go build -o bin/$(EXECUTABLE) $(PKG)
	cp kvconfig.yml bin/kvconfig.yml

test: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

# Variables for testing
export S3_BUCKET ?= clever-analytics-dev
export SCHEMA ?= api
export TABLES ?= business_metrics_auth_counts
export DATE ?= 2016-12-03T00:00:00Z
export GRANULARITY ?= day
export FORCE ?= false
export TRUNCATE ?= false
export GZIP ?= true

run: build
	./bin/$(EXECUTABLE) --bucket $(S3_BUCKET) --schema $(SCHEMA) --tables $(TABLES) --date $(DATE) --granularity $(GRANULARITY) --force=$(FORCE) --truncate=$(TRUNCATE) --gzip=$(GZIP)

vendor: golang-godep-vendor-deps
	$(call golang-godep-vendor,$(PKGS))
