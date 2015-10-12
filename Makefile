SHELL := /bin/bash
PKG := github.com/Clever/redshifter
SUBPKG_NAMES := redshift s3filepath
SUBPKGS = $(addprefix $(PKG)/, $(SUBPKG_NAMES))
PKGS = $(PKG)/cmd/ $(SUBPKGS)

.PHONY: test golint docs

test: docs $(PKGS)

$(GOPATH)/bin/godep:
	@go get github.com/tools/godep

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
ifeq ($(COVERAGE),1)
	@$(GOPATH)/bin/godep go test -cover -coverprofile=$(GOPATH)/src/$@/c.out $@ -test.v
	@$(GOPATH)/bin/godep go tool cover -html=$(GOPATH)/src/$@/c.out
else
	@echo "TESTING..."
	@$(GOPATH)/bin/godep go test $@ -test.v
	@echo ""
endif

docs: $(addsuffix /README.md, $(SUBPKG_NAMES)) README.md
%/README.md: %/*.go $(GOPATH)/bin/godocdown
	@$(GOPATH)/bin/godocdown $(PKG)/$(shell dirname $@) > $@
