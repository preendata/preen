---
description: how to configure preen to connect to MongoDB databases.
---

# MongoDB

Preen can connect to MongoDB databases. Our current implementation uses the Go [mongo](https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo) library to connect to databases.

## Example Preen Source Configuration

```yaml
# FILENAME: ~/.preen/sources.yaml
sources:
  - name: mongo-example
    engine: mongodb
    connection:
      host: localhost
      port: 27117
      database: preendb
      username: ${MONGODB_USERNAME}
      password: ${MONGODB_PASSWORD}
      auth_source: admin
```

## Mongo Database Models

MongoDB models are defined as a YAML file that contains a MongoDB document filter. This filter is used to match documents in the database and return the data that matches the filter. The documents are written to DuckDB as a JSON column for local querying using the native [JSON querying capabilities of DuckDB](https://duckdb.org/docs/extensions/json.html).

```yaml
# FILENAME: ~/.preen/models/users.yaml
name: users-mongodb
type: mongodb
collection: users # The name of the collection to query.
query: |
    {
      "login_attempts": {
        "$gt": 1
      },
      "account_status": {
        "$in": ["inactive", "suspended"]
      }
    }
```

## Code References

- [mongo.go](https://github.com/preendata/preen/blob/main/internal/engine/mongo.go)
