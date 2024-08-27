package duckdb

import (
	"context"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
)

// Returns a DuckDB appender instance for bulk loading of data
func NewAppender(connector driver.Connector, schema string, table string) (*duckdb.Appender, error) {
	conn, err := connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}

	appender, err := duckdb.NewAppenderFromConn(conn, schema, table)
	if err != nil {
		return nil, err
	}

	return appender, nil
}
