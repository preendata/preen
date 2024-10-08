.PHONY: integration-test
integration-test: build
	build/ci/integration-test.sh

.PHONY: build
build:
	go build -o bin/preen main.go

.PHONY: lint
lint:
	golangci-lint run

.PHONY: install-depenencies
install-depenencies:
	brew install golangci-lint

.PHONY: test
test:
	go test -v ./...