.PHONY: install-deps
install-deps:
	brew install go
	brew install protobuf
	brew install libpq
	brew link --force libpq
	brew install bufbuild/buf/buf

.PHONY: fmt-proto
fmt-proto:
	buf format --diff -w --exit-code proto

.PHONY: gen-code
gen-code:
	buf generate --timeout 1m --template buf/buf.gen.go.yaml
