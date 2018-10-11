# This is the default sfncli Makefile.
# Please do not alter this file directly.
SFNCLI_MK_VERSION := 0.1.1
SHELL := /bin/bash
SYSTEM := $(shell uname -a | cut -d" " -f1 | tr '[:upper:]' '[:lower:]')
SFNCLI_INSTALLED := $(shell [[ -e "bin/sfncli" ]] && bin/sfncli --version)
SFNCLI_LATEST = $(shell curl -s https://api.github.com/repos/Clever/sfncli/releases/latest | grep tag_name | cut -d\" -f4)

.PHONY: bin/sfncli sfncli-update-makefile ensure-sfncli-version-set ensure-curl-installed

ensure-sfncli-version-set:
	@ if [[ "$(SFNCLI_VERSION)" = "" ]]; then \
		echo "SFNCLI_VERSION not set in Makefile - Suggest setting 'SFNCLI_VERSION := latest'"; \
		exit 1; \
	fi

ensure-curl-installed:
	@command -v curl >/dev/null 2>&1 || { echo >&2 "curl not installed. Please install curl."; exit 1; }

bin/sfncli: ensure-sfncli-version-set ensure-curl-installed
	@mkdir -p bin
	$(eval SFNCLI_VERSION := $(if $(filter latest,$(SFNCLI_VERSION)),$(SFNCLI_LATEST),$(SFNCLI_VERSION)))
	@echo "Checking for sfncli updates..."
	@if [[ "$(SFNCLI_VERSION)" == "$(SFNCLI_INSTALLED)" ]]; then \
		echo "Using latest sfncli version $(SFNCLI_VERSION)"; \
	else \
		echo "Updating sfncli..."; \
		curl --retry 5 --fail --max-time 30 -o bin/sfncli -sL https://github.com/Clever/sfncli/releases/download/$(SFNCLI_VERSION)/sfncli-$(SFNCLI_VERSION)-$(SYSTEM)-amd64 && \
		chmod +x bin/sfncli && \
		echo "Successfully updated sfncli to $(SFNCLI_LATEST)" || \
		{ echo "Failed to update sfncli"; exit 1; } \
	;fi

sfncli-update-makefile: ensure-curl-installed
	@curl -o /tmp/sfncli.mk -sL https://raw.githubusercontent.com/Clever/sfncli/master/make/sfncli.mk
	@if ! grep -q $(SFNCLI_MK_VERSION) /tmp/sfncli.mk; then cp /tmp/sfncli.mk sfncli.mk && echo "sfncli.mk updated"; else echo "sfncli.mk is up-to-date"; fi
