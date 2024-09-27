---
description: how to configure preen to connect to databases.
---

# Databases

Preen can connect to SQL and NoSQL databases. Our current implementation uses the Go [sql](https://pkg.go.dev/database/sql) and [pgx](https://github.com/jackc/pgx) libraries to connect to databases.

## Supported Integrations

Preen currently supports the following SQL databases:

- [Postgres](postgres.md)
- [MySQL](mysql.md)
- [MongoDB](mongodb.md)

## Code References

- [mysql.go](https://github.com/preendata/preen/blob/main/internal/engine/mysql.go)
- [postgres.go](https://github.com/preendata/preen/blob/main/internal/engine/postgres.go)
- [mongo.go](https://github.com/preendata/preen/blob/main/internal/engine/mongo.go)