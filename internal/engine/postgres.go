package engine

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type QueryResult struct {
	Rows    []map[string]any
	Columns []string
}

func getPostgresPool(url string) (*pgxpool.Pool, error) {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	dbpool, err := pgxpool.New(context.Background(), url)

	if err != nil {
		Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}
	return dbpool, nil
}

func GetPostgresPoolFromSource(source configSource) (*pgxpool.Pool, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		url.QueryEscape(source.Connection.Password),
		url.QueryEscape(source.Connection.Host),
		source.Connection.Port,
		source.Connection.Database,
	)
	dbpool, err := getPostgresPool(url)

	if err != nil {
		return nil, err
	}

	return dbpool, nil
}

// Execute a raw statement on all sources in the config.
func ExecuteRaw(statement string, cfg *Config, source configSource) (pgx.Rows, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		source.Connection.Password,
		source.Connection.Host,
		source.Connection.Port,
		source.Connection.Database,
	)

	dbpool, err := getPostgresPool(url)

	if err != nil {
		return nil, err
	}

	defer dbpool.Close()
	Debug("Executing query against Postgres: ", statement)

	rows, err := dbpool.Query(context.Background(), statement)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
