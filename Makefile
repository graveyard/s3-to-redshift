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

all: test build

FGT := $(GOPATH)/bin/fgt
$(FGT):
		go get github.com/GeertJohan/fgt

GOLINT := $(GOPATH)/bin/golint
$(GOLINT):
	go get github.com/golang/lint/golint

GODEP := $(GOPATH)/bin/godep
$(GODEP):
	go get -u github.com/tools/godep

build:
	go build -o bin/$(EXECUTABLE) $(PKG)

test: $(PKGS)

$(PKGS): $(GOLINT) $(FGT)
	@echo "FORMATTING"
	@$(FGT) gofmt -l=true $(GOPATH)/src/$@/*.go
	@echo "TESTING"
	@go test -v $@
	@echo "LINTING"
	@$(FGT) $(GOLINT) $(GOPATH)/src/$@/*.go
	@echo "VETTING"
	@go vet -v $@

vendor: $(GODEP)
	$(GODEP) save $(PKGS)
	find vendor/ -path '*/vendor' -type d | xargs -IX rm -r X # remove any nested vendor directories
