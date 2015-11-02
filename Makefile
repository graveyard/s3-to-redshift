SHELL := /bin/bash
PKG := github.com/Clever/s3-to-redshift
SUBPKG_NAMES := redshift s3filepath
SUBPKGS = $(addprefix $(PKG)/, $(SUBPKG_NAMES))
PKGS = $(PKG)/cmd/ $(SUBPKGS)

.PHONY: build test golint docs $(PKG) $(PKGS)

GOVERSION := $(shell go version | grep 1.5)
ifeq "$(GOVERSION)" ""
		$(error must be running Go version 1.5)
endif
export GO15VENDOREXPERIMENT=1


build: test
	go build -o build/s3-to-redshift github.com/Clever/s3-to-redshift/cmd

test: docs $(PKGS)

$(GOPATH)/bin/golint:
	@go get github.com/golang/lint/golint

$(GOPATH)/bin/godocdown:
	@go get github.com/robertkrimen/godocdown/godocdown

$(PKGS): $(GOPATH)/bin/golint docs
	@go get -d -t $@
	@gofmt -w=true $(GOPATH)/src/$@*/**.go
ifneq ($(NOLINT),1)
	@echo "LINTING..."
	@$(GOPATH)/bin/golint $(GOPATH)/src/$@*/**.go
	@echo ""
endif
	@echo "TESTING..."
	@go test $@ -test.v
	@echo ""

docs: $(addsuffix /README.md, $(SUBPKG_NAMES)) README.md
%/README.md: %/*.go $(GOPATH)/bin/godocdown
	@$(GOPATH)/bin/godocdown $(PKG)/$(shell dirname $@) > $@
