# HyphaDB

This is the backend for the HyphaDB project. There are several components to this project:

- [REST API](./cmd/api/)
- [CLI](./cmd/cli/)

## Quick Start

`cp .env.example .env`

Temporary: name the config file config.yaml and place it in ./.config/

Running the API:

```bash
go run cmd/api/main.go
```

CLI entrypoint: 

`go run cmd/cli/main.go -h`


## Testing
To run a simple test suite, read the top of ./test/test.sh and install the requisite tools. Then run ./test/test.sh
