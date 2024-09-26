![tests](https://github.com/hyphasql/hypha/actions/workflows/ci.yaml/badge.svg)

# Hypha

Hypha is a powerful command-line application for querying from multiple sources locally from your laptop. Under the hood, Hypha uses [DuckDB](https://duckdb.org/) to build an ephemeral, in-memory data warehouse and then uses DuckDB's SQL engine to query the data. Think of Hypha as a mix of Fivetran and DBT for your DuckDB use cases. You describe the data you want to query using a declarative language and Hypha takes care of the rest.

Hypha is currently in the alpha stage and not all features are available. We are working on adding more features and improving the user experience. If you have any questions or feedback, please feel free to open an issue on GitHub.

## Features

- Query data from multiple sources using a single interface
- Support for MongoDB, PostgreSQL, MySQL, and AWS S3
- Model-based data retrieval and collation
- Identify and resolve data type discrepancies between sources
- Interactive REPL for querying data
- Configurable output formats (table, CSV, markdown, JSON)
- Extensible architecture for adding new data sources

## Installation

### Download pre-built binary

You can download a pre-built binary for your operating system and architecture from the [GitHub Releases](https://github.com/hyphasql/hypha/releases) page.

```bash
# Using curl
sh -c "$(curl -fsSL https://raw.githubusercontent.com/hyphasql/hypha/main/build/install.sh)"

# Using wget
sh -c "$(wget https://raw.githubusercontent.com/hyphasql/hypha/main/build/install.sh -O -)"
```

### Build from source

To build Hypha from source, you need to have Go 1.23.0 or later installed on your system. Then, you can build the application using the following commands:

```bash
git clone https://github.com/hyphasql/hypha.git
cd hypha
make build
```

This will create a `hypha` binary in the `bin` directory. You can add this to your `PATH` if you want to use the `hypha` command from anywhere.

## Configuration

Hypha uses two configuration files: `sources.yaml` and `models.yaml`. The `sources.yaml` file is used to configure the data sources that Hypha will query. The `models.yaml` file is used to define the models that Hypha will build. The directory Hypha will look for source and model configurations is configurable via the `HYPHA_CONFIG_PATH` environment variable. You can see an example of the environment configuation in the [.env.example](.env.example) file.The `models.yaml` file is optional. If it is not present, Hypha will look for `.yaml` files in the `models` directory.

Here is an example `sources.yaml` file:

```yaml
sources:
  - name: mongo-db-us-west-1 # This has to be unique
    engine: mongodb
    connection:
      host: localhost
      port: 27117
      database: hypha
      username: root
      password: ${MONGO_PASSWORD} # You can also use environment variables.
      auth_source: admin
    models: 
      - users
      - orders
      - products
```

Here is an example `models.yaml` file:

```yaml
models:
  - name: hypha-users-model
    type: database
    query: |
      SELECT users.user_id, users.name, users.email FROM users
```

You can validate your configuration by running:

```bash
hypha source validate
```

## Usage

### Building Models

Building a model will fetch the data from the source and save it to the DuckDB database. To build your models, run:

```bash
hypha model build
```

### Querying Data

You can query data using the interactive REPL. You can also specify the output format of the data (table, csv, markdown, json).

```bash
hypha repl

# Specify output format
hypha repl --output-format csv
```

For one-off queries, use the `query` command:

```bash
hypha query "select * from your_model limit 10" --output-format csv
```

## Development

To set up the development environment:

1. Clone the repository
2. Copy `.env.example` to `.env` and adjust the values as needed
3. Install dependencies: `go mod tidy`
4. Run Unit tests: `make test`
5. Run Integration tests: `make integration-test`
6. Run linter: `make lint`

## License

This project is dual-licensed under the LGPL-3.0 License and Hypha Enterprise License - see the [LICENSE](LICENSE) file for details. Our core features are free to use under the LGPL-3.0 License. If you need additional features, you can purchase a Hypha Enterprise License.
