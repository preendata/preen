package main

import (
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
)

type RDBMS struct {
	connectionString string
	pool             *sqlx.DB
}

type QueryResult struct {
	Rows    []map[string]any
	Columns []string
}

func (r *RDBMS) CreateConnectionPool(url string) (*sqlx.DB, error) {
	pool, err := sqlx.Open("mysql", url)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}

	return pool, err
}

func (r *RDBMS) ExecuteRaw(statement string) (*sqlx.Rows, error) {
	r.CreateConnectionPool(r.connectionString)
	defer r.pool.Close()

	rows, err := r.pool.Queryx(statement)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
