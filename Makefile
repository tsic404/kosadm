# Local entry points for kosadm. CI runs `lint`, `test` and `acceptance`, one
# step per target; `build` and `vet` are the pieces a pull request can run
# without the envtest control plane.
#
# Nothing here pushes or publishes: the repository is the delivery.

SHELL := /bin/sh
GO ?= go
ENVTEST_K8S_VERSION ?= 1.37.0
ENVTEST_ASSETS ?= $(CURDIR)/bin/envtest
# The release publishes one sha512 sidecar per archive; hosts without coreutils
# can pass SHA512SUM="shasum -a 512".
SHA512SUM ?= sha512sum
PLATFORM := $(shell $(GO) env GOOS)-$(shell $(GO) env GOARCH)
ENVTEST_ARCHIVE := envtest-v$(ENVTEST_K8S_VERSION)-$(PLATFORM).tar.gz
ENVTEST_URL := https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v$(ENVTEST_K8S_VERSION)/$(ENVTEST_ARCHIVE)
ENVTEST_STAMP := $(ENVTEST_ASSETS)/.version
# Binaries a usable asset directory must provide; the re-download decision and
# the post-unpack check read this same list, so a partial unpack stays unusable.
ENVTEST_BINARIES := kube-apiserver etcd

.PHONY: all build fmt-check vet lint test acceptance envtest-version envtest-assets clean

all: build lint test

build:
	$(GO) build ./...

fmt-check:
	@unformatted=$$(gofmt -l $$(git ls-files '*.go')); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt required for:"; echo "$$unformatted"; exit 1; \
	fi

vet:
	$(GO) vet ./...

lint: fmt-check vet

# test covers the packages that need no control plane; the acceptance cases run
# only under `acceptance`, so a CI run does not execute them twice.
test:
	$(GO) test -p 1 ./pkg/... ./cmd/...

# acceptance needs a kube-apiserver and etcd; the assets are the pinned envtest
# release. Packages run one at a time: each case boots its own API server, and a
# parallel run piles up control planes until the host refuses to start another
# etcd.
acceptance: envtest-assets
	KUBEBUILDER_ASSETS="$(ENVTEST_ASSETS)" $(GO) test -p 1 ./tests/... -count=1 -v

# envtest-version is the only place the pinned control-plane version is written
# down: CI reads it for the cache key, so the two cannot drift apart.
envtest-version:
	@echo $(ENVTEST_K8S_VERSION)

# envtest-assets unpacks the pinned control plane only after the published
# sha512 matches. The stamp is written last, and only once both binaries are
# executable: a failed unpack leaves no ready marker, so the next run re-downloads.
envtest-assets:
	@set -e; \
	has_binaries() { \
		for b in $(ENVTEST_BINARIES); do [ -x "$(ENVTEST_ASSETS)/$$b" ] || return 1; done; \
	}; \
	if [ "$$(cat "$(ENVTEST_STAMP)" 2>/dev/null)" = "$(ENVTEST_K8S_VERSION)" ] && has_binaries; then \
		exit 0; \
	fi; \
	echo "downloading $(ENVTEST_URL)"; \
	tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSL "$(ENVTEST_URL)" -o "$$tmp/$(ENVTEST_ARCHIVE)"; \
	curl -fsSL "$(ENVTEST_URL).sha512" -o "$$tmp/$(ENVTEST_ARCHIVE).sha512"; \
	want=$$(cut -d' ' -f1 "$$tmp/$(ENVTEST_ARCHIVE).sha512"); \
	got=$$($(SHA512SUM) "$$tmp/$(ENVTEST_ARCHIVE)" | cut -d' ' -f1); \
	if [ -z "$$want" ] || [ "$$want" != "$$got" ]; then \
		echo "sha512 mismatch for $(ENVTEST_ARCHIVE)" >&2; \
		exit 1; \
	fi; \
	rm -rf "$(ENVTEST_ASSETS)"; \
	mkdir -p "$(ENVTEST_ASSETS)"; \
	tar -xzf "$$tmp/$(ENVTEST_ARCHIVE)" -C "$(ENVTEST_ASSETS)" --strip-components=2 "controller-tools/envtest"; \
	if ! has_binaries; then \
		echo "envtest assets incomplete: $(ENVTEST_BINARIES) must be executable under $(ENVTEST_ASSETS)" >&2; \
		exit 1; \
	fi; \
	printf '%s\n' "$(ENVTEST_K8S_VERSION)" > "$(ENVTEST_STAMP)"

clean:
	rm -rf bin
