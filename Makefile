include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: build test golint docs $(PKG) $(PKGS) vendor
SHELL := /bin/bash
PKG := github.com/Clever/s3-to-redshift
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE := $(shell basename $(PKG))
$(eval $(call golang-version-check,1.7))

all: test build

build:
	go build -o bin/$(EXECUTABLE) $(PKG)

test: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

vendor: golang-godep-vendor-deps
	$(call golang-godep-vendor,$(PKGS))
