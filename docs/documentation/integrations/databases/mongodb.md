# MongoDB

Hypha can connect to MongoDB databases. Our current implementation uses the Go [mongo](https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo) library to connect to databases.

## Example Hypha Source Configuration

```yaml
# FILENAME: ~/.hypha/sources.yaml
sources:
  - name: mongo-example
    engine: mongodb
    connection:
      host: localhost
      port: 27117
      database: hyphadb
      username: ${MONGODB_USERNAME}
      password: ${MONGODB_PASSWORD}
      auth_source: admin
```

## Mongo Database Models

MongoDB models are defined as a YAML file that contains a MongoDB document filter. This filter is used to match documents in the database and return the data that matches the filter. The documents are written to DuckDB as a JSON column for local querying using the native [JSON querying capabilities of DuckDB](https://duckdb.org/docs/extensions/json.html).

## Code References

- [mongo.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/mongo.go)
