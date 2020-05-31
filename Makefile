ENV = $(shell go env GOPATH)
GO_VERSION = $(shell go version)

# Look for versions prior to 1.10 which have a different fmt output
# and don't lint with gofmt against them.
ifneq (,$(findstring go version go1.8, $(GO_VERSION)))
	FMT=
else ifneq (,$(findstring go version go1.9, $(GO_VERSION)))
	FMT=
else
    FMT=--enable gofmt
endif

test:
	GOTRACEBACK=all go test $(TESTARGS) -timeout=60s -race test/*
	GOTRACEBACK=all go test $(TESTARGS) -timeout=60s -tags batchtest -race test/*

lint:
	gofmt -s -w .
	golangci-lint run -c .golangci-lint.yml $(FMT) pkg/**/*

dep-linter:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(ENV)/bin $(GOLANG_CI_VERSION)

deps:
	go get -t -d -v ./...

build:
	sh $(PWD)/scripts/build.sh

ci: $(MAKE) deps test dep-linter lint

.PHONY: deps test integ lint