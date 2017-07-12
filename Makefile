# for makefile
SHELL := /bin/bash
export PATH := ${GOPATH}/bin:$(PATH)


all: build

install-glide:
	go get github.com/Masterminds/glide

.PHONY: deps
deps:
	@echo "deps"
	glide cache-clear
	glide update

.PHONY: build
build:
	@mkdir -p bin
	go build -o ./bin/sqlitify sqlitify.go

.PHONY: example 
example:
	@mkdir -p bin
	go build -o ./bin/glossary example/glossary/main/*.go

.PHONY: clean
clean:
	rm -f .bin/sqlitify

.PHONY: test
test:
	go test .
