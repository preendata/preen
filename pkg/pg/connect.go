package pg

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgconn"
)

// The connect function takes a database url as a string and returns the
// pgx.Conn object for use in querying the database
func connect(url string) *pgconn.PgConn {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	connection, err := pgconn.Connect(context.Background(), url)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		os.Exit(1)
	}
	return connection
}
