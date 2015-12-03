SHELL := /bin/bash
PKG := github.com/Clever/s3-to-redshift/cmd
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE := s3-to-redshift
.PHONY: build test golint docs $(PKG) $(PKGS) vendor

GOVERSION := $(shell go version | grep 1.5)
ifeq "$(GOVERSION)" ""
		$(error must be running Go version 1.5)
endif
export GO15VENDOREXPERIMENT=1

GOLINT := $(GOPATH)/bin/golint
$(GOLINT):
	go get github.com/golang/lint/golint

GODEP := $(GOPATH)/bin/godep
$(GODEP):
	go get -u github.com/tools/godep

build: test
	go build -o bin/$(EXECUTABLE) $(PKG)

test: $(PKGS)

$(PKGS): $(GOPATH)/bin/golint
	@gofmt -w=true $(GOPATH)/src/$@*/**.go
ifneq ($(NOLINT),1)
	@echo "LINTING..."
	@$(GOLINT) $(GOPATH)/src/$@*/**.go
	@echo ""
endif
	@echo "TESTING..."
	@go test $@ -test.v
	@echo ""

vendor: $(GODEP)
	$(GODEP) save $(PKGS)
	find vendor/ -path '*/vendor' -type d | xargs -IX rm -r X # remove any nested vendor directories
