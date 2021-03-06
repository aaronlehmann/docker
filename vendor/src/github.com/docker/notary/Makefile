# Set an output prefix, which is the local directory if not specified
PREFIX?=$(shell pwd)

# Populate version variables
# Add to compile time flags
NOTARY_PKG := github.com/docker/notary
NOTARY_VERSION := $(shell cat NOTARY_VERSION)
GITCOMMIT := $(shell git rev-parse --short HEAD)
GITUNTRACKEDCHANGES := $(shell git status --porcelain --untracked-files=no)
ifneq ($(GITUNTRACKEDCHANGES),)
GITCOMMIT := $(GITCOMMIT)-dirty
endif
CTIMEVAR=-X $(NOTARY_PKG)/version.GitCommit=$(GITCOMMIT) -X $(NOTARY_PKG)/version.NotaryVersion=$(NOTARY_VERSION)
GO_LDFLAGS=-ldflags "-w $(CTIMEVAR)"
GO_LDFLAGS_STATIC=-ldflags "-w $(CTIMEVAR) -extldflags -static"
GOOSES = darwin freebsd linux
GOARCHS = amd64
NOTARY_BUILDTAGS ?= pkcs11
GO_EXC = go
NOTARYDIR := /go/src/github.com/docker/notary

# check to be sure pkcs11 lib is always imported with a build tag
GO_LIST_PKCS11 := $(shell go list -e -f '{{join .Deps "\n"}}' ./... | xargs go list -e -f '{{if not .Standard}}{{.ImportPath}}{{end}}' | grep -q pkcs11)
ifeq ($(GO_LIST_PKCS11),)
$(info pkcs11 import was not found anywhere without a build tag, yay)
else
$(error You are importing pkcs11 somewhere and not using a build tag)
endif

_empty :=
_space := $(empty) $(empty)

# go cover test variables
COVERDIR=.cover
COVERPROFILE?=$(COVERDIR)/cover.out
COVERMODE=count
PKGS = $(shell go list ./... | tr '\n' ' ')

GO_VERSION = $(shell go version | awk '{print $$3}')

.PHONY: clean all fmt vet lint build test binaries cross cover docker-images notary-dockerfile
.DELETE_ON_ERROR: cover
.DEFAULT: default

go_version:
ifeq (,$(findstring go1.5.,$(GO_VERSION)))
	$(error Requires go version 1.5.x - found $(GO_VERSION))
else
	@echo
endif


all: AUTHORS clean fmt vet fmt lint build test binaries

AUTHORS: .git/HEAD
	git log --format='%aN <%aE>' | sort -fu > $@

# This only needs to be generated by hand when cutting full releases.
version/version.go:
	./version/version.sh > $@

${PREFIX}/bin/notary-server: NOTARY_VERSION $(shell find . -type f -name '*.go')
	@echo "+ $@"
	@godep go build -tags ${NOTARY_BUILDTAGS} -o $@ ${GO_LDFLAGS} ./cmd/notary-server

${PREFIX}/bin/notary: NOTARY_VERSION $(shell find . -type f -name '*.go')
	@echo "+ $@"
	@godep go build -tags ${NOTARY_BUILDTAGS} -o $@ ${GO_LDFLAGS} ./cmd/notary

${PREFIX}/bin/notary-signer: NOTARY_VERSION $(shell find . -type f -name '*.go')
	@echo "+ $@"
	@godep go build -tags ${NOTARY_BUILDTAGS} -o $@ ${GO_LDFLAGS} ./cmd/notary-signer

vet: go_version
	@echo "+ $@"
ifeq ($(shell uname -s), Darwin)
	@test -z "$(shell find . -iname *test*.go | grep -v _test.go | grep -v Godeps | xargs echo "This file should end with '_test':"  | tee /dev/stderr)"
else
	@test -z "$(shell find . -iname *test*.go | grep -v _test.go | grep -v Godeps | xargs -r echo "This file should end with '_test':"  | tee /dev/stderr)"
endif
	@test -z "$$(go tool vet -printf=false . 2>&1 | grep -v Godeps/_workspace/src/ | tee /dev/stderr)"

fmt:
	@echo "+ $@"
	@test -z "$$(gofmt -s -l .| grep -v .pb. | grep -v Godeps/_workspace/src/ | tee /dev/stderr)"

lint:
	@echo "+ $@"
	@test -z "$$(golint ./... | grep -v .pb. | grep -v Godeps/_workspace/src/ | tee /dev/stderr)"

build: go_version
	@echo "+ $@"
	@go build -tags "${NOTARY_BUILDTAGS}" -v ${GO_LDFLAGS} ./...

test: TESTOPTS =
test: go_version
	@echo "+ $@ $(TESTOPTS)"
	go test -tags "${NOTARY_BUILDTAGS}" $(TESTOPTS) ./...

test-full: vet lint
	@echo "+ $@"
	go test -tags "${NOTARY_BUILDTAGS}" -v ./...

protos:
	@protoc --go_out=plugins=grpc:. proto/*.proto

# This allows coverage for a package to come from tests in different package.
# Requires that the following:
# go get github.com/wadey/gocovmerge; go install github.com/wadey/gocovmerge
#
# be run first

define gocover
$(GO_EXC) test $(OPTS) $(TESTOPTS) -covermode="$(COVERMODE)" -coverprofile="$(COVERDIR)/$(subst /,-,$(1)).$(subst $(_space),.,$(NOTARY_BUILDTAGS)).cover" "$(1)" || exit 1;
endef

gen-cover: go_version
	@mkdir -p "$(COVERDIR)"
	$(foreach PKG,$(PKGS),$(call gocover,$(PKG)))

cover: GO_EXC := go
       OPTS = -tags "${NOTARY_BUILDTAGS}" -coverpkg "$(shell ./coverpkg.sh $(1) $(NOTARY_PKG))"
cover: gen-cover covmerge
	@go tool cover -html="$(COVERPROFILE)"

# Codecov knows how to merge multiple coverage files
ci: OPTS = -tags "${NOTARY_BUILDTAGS}" -race -coverpkg "$(shell ./coverpkg.sh $(1) $(NOTARY_PKG))"
    GO_EXC := godep go
ci: gen-cover

covmerge:
	@gocovmerge $(shell ls -1 $(COVERDIR)/* | tr "\n" " ") > $(COVERPROFILE)
	@go tool cover -func="$(COVERPROFILE)"

clean-protos:
	@rm proto/*.pb.go

binaries: go_version ${PREFIX}/bin/notary-server ${PREFIX}/bin/notary ${PREFIX}/bin/notary-signer
	@echo "+ $@"

define template
mkdir -p ${PREFIX}/cross/$(1)/$(2);
GOOS=$(1) GOARCH=$(2) CGO_ENABLED=0 go build -o ${PREFIX}/cross/$(1)/$(2)/notary -a -tags "static_build netgo" -installsuffix netgo ${GO_LDFLAGS_STATIC} ./cmd/notary;
endef

cross: go_version
	$(foreach GOARCH,$(GOARCHS),$(foreach GOOS,$(GOOSES),$(call template,$(GOOS),$(GOARCH))))


notary-dockerfile:
	@docker build --rm --force-rm -t notary .

server-dockerfile:
	@docker build --rm --force-rm -f Dockerfile.server -t notary-server .

signer-dockerfile:
	@docker build --rm --force-rm -f Dockerfile.signer -t notary-signer .

docker-images: notary-dockerfile server-dockerfile signer-dockerfile

shell: notary-dockerfile
	docker run --rm -it -v $(CURDIR)/cross:$(NOTARYDIR)/cross -v $(CURDIR)/bin:$(NOTARYDIR)/bin notary bash


clean:
	@echo "+ $@"
	@rm -rf "$(COVERDIR)"
	@rm -rf "${PREFIX}/bin/notary-server" "${PREFIX}/bin/notary" "${PREFIX}/bin/notary-signer"
