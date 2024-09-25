---
description: how to configure hypha to connect to Postgres databases.
---

# Postgres

Hypha uses the [pgx](https://github.com/jackc/pgx) library to connect to Postgres databases.

## Example Hypha Source Configuration

```yaml
# FILENAME: ~/.hypha/sources.yaml
sources:
  - name: postgres-example
    engine: postgres
    connection:
      host: localhost
      port: 5432
      database: postgres
      username: ${PG_USER} # You can specify environment variables in the sources.yaml file.
      password: ${PG_PASSWORD}    
```

## Postgres Type Mappings

A comprehensive list of Postgres type mappings can be found [here](https://github.com/hyphasql/hypha/blob/main/internal/engine/types.go#L190-L240). We use the [pgtype](https://pkg.go.dev/github.com/jackc/pgtype) library to map Postgres types to Go types, with a few custom mappings for things like `float64`, `duration`, and `time` types.

## Code References

- [types.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/types.go)
- [postgres.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/postgres.go)
