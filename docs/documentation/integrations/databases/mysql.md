---
description: how to configure preen to connect to MySQL databases.
---

# MySQL

Preen uses the [sql](https://pkg.go.dev/database/sql) library to connect to MySQL databases.

## Example Preen Source Configuration

```yaml
# FILENAME: ~/.preen/sources.yaml
sources:
  - name: mysql-example
    engine: mysql
    connection:
      host: localhost
      port: 3306
      database: mysql
      username: ${MYSQL_USER} # You can specify environment variables in the sources.yaml file.
      password: ${MYSQL_PASSWORD}    
```

## MySQL Models

MySQL models are defined as a YAML file that contains a SQL query.

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

## MySQL Type Mappings

A comprehensive list of MySQL type mappings can be found [here](https://github.com/preendata/preen/blob/main/internal/engine/types.go#L190-L240).

## Code References

- [types.go](https://github.com/preendata/preen/blob/main/internal/engine/types.go)
- [postgres.go](https://github.com/preendata/preen/blob/main/internal/engine/mysql.go)