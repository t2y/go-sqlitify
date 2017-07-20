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

.PHONY: build-generator
build-generator:
	@mkdir -p bin
	go build -o ./bin/type_generator cli/type_generator/main.go
	go build -o ./bin/code_generator cli/code_generator/main.go

.PHONY: build
build:
	@mkdir -p bin
	./bin/type_generator --inputPath .
	go generate
	go build -o ./bin/sqlitify *.go

.PHONY: example 
example:
	@mkdir -p bin
	go build -o ./bin/glossary example/glossary/main/*.go

.PHONY: clean
clean:
	rm -f .bin/*

.PHONY: test
test:
	go test .
