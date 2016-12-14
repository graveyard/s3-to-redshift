include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: build test golint docs $(PKG) $(PKGS) vendor
SHELL := /bin/bash
PKG := github.com/Clever/s3-to-redshift
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE := $(shell basename $(PKG))
$(eval $(call golang-version-check,1.7))

all: test build

clean:
	rm -f $(GOPATH)/src/$(PKG)/bin/$(EXECUTABLE)

build: clean
	go build -o bin/$(EXECUTABLE) $(PKG)

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
