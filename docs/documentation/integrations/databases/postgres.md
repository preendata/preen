---
description: how to configure preen to connect to Postgres databases.
---

# Postgres

Preen uses the [pgx](https://github.com/jackc/pgx) library to connect to Postgres databases.

## Example Preen Source Configuration

```yaml
# FILENAME: ~/.preen/sources.yaml
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

## Postgres Models

Postgres models are defined as a YAML file that contains a SQL query.

```yaml
# FILENAME: ~/.preen/models/users.yaml
name: users # This name needs to be unique
type: sql
query: |
  select
    users.id,
    users.first_name,
    users.last_name,
    users.birthday
  from
    users;
```

## Postgres Type Mappings

A comprehensive list of Postgres type mappings can be found [here](https://github.com/preendata/preen/blob/main/internal/engine/types.go#L190-L240). We use the [pgtype](https://pkg.go.dev/github.com/jackc/pgtype) library to map Postgres types to Go types, with a few custom mappings for things like `float64`, `duration`, and `time` types.

## Code References

- [types.go](https://github.com/preendata/preen/blob/main/internal/engine/types.go)
- [postgres.go](https://github.com/preendata/preen/blob/main/internal/engine/postgres.go)
