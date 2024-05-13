package pg

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The connect function takes a database url as a string and returns the
// pgx.Conn object for use in querying the database
func connect(url string) (*pgconn.PgConn, error) {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	connection, err := pgconn.Connect(context.Background(), url)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}
	return connection, nil
}

func dbpool(url string) (*pgxpool.Pool, error) {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	dbpool, err := pgxpool.New(context.Background(), url)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}
	return dbpool, nil
}
