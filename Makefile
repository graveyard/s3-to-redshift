include golang.mk
include sfncli.mk
.DEFAULT_GOAL := test

SHELL := /bin/bash
PKG := github.com/Clever/s3-to-redshift
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE := $(shell basename $(PKG))
SFNCLI_VERSION := latest

.PHONY: test $(PKGS) run install_deps build

$(eval $(call golang-version-check,1.9))

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

test: $(PKGS)

build: bin/sfncli
	$(call golang-build,$(PKG),$(EXECUTABLE))

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
	bin/sfncli --activityname $(_DEPLOY_ENV)--$(_APP_NAME) \
		--region us-west-2 \
		--cloudwatchregion us-west-1 \
		--workername `hostname` \
		--cmd bin/$(EXECUTABLE)

install_deps: golang-dep-vendor-deps
	$(call golang-dep-vendor)
