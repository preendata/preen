.PHONY: integration-test
integration-test: build
	docker compose -f build/ci/docker-compose.yaml up -d
	sleep 5
	build/ci/integration-tests.sh
	docker compose -f build/ci/docker-compose.yaml down

.PHONY: build
build:
	go build -o bin/hypha main.go

.PHONY: lint
lint:
	golangci-lint run

.PHONY: install-depenencies
install-depenencies:
	brew install golangci-lint