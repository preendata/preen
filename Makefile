.PHONY: integration-test
integration-test: build
	docker compose -f build/ci/docker-compose.yaml up -d
	sleep 5
	bin/hypha model build
	bin/hypha query -f json "select * from mysql_data_types_test;"
	docker compose -f build/ci/docker-compose.yaml down

.PHONY: build
build:
	go build -o bin/hypha main.go
