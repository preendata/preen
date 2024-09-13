.PHONY: integration-test
integration-test: build
	build/ci/integration-test.sh

.PHONY: build
build:
	go build -o bin/hypha main.go

.PHONY: lint
lint:
	golangci-lint run

.PHONY: install-depenencies
install-depenencies:
	brew install golangci-lint