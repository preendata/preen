---
description: how to configure hypha to connect to SQL databases.
---

# SQL

Hypha can connect to SQL databases. Our current implementation uses the Go [sql](https://pkg.go.dev/database/sql) and [pgx](https://github.com/jackc/pgx) libraries to connect to databases.

## Supported Integrations

Hypha currently supports the following SQL databases:

- [Postgres](postgres.md)
- [MySQL](mysql.md)
- [MongoDB](mongodb.md)

## Code References

- [mysql.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/mysql.go)
- [postgres.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/postgres.go)
- [mongo.go](https://github.com/hyphasql/hypha/blob/main/internal/engine/mongo.go)