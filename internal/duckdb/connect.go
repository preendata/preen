package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
)

func CreateConnector() (driver.Connector, error) {
	connector, err := duckdb.NewConnector("./hyphaContext.db?threads=4", func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
		}

		for _, query := range bootQueries {
			_, err := execer.ExecContext(context.Background(), query, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return connector, nil
}

func OpenDatabase(connector driver.Connector) (*sql.DB, error) {
	db := sql.OpenDB(connector)
	return db, nil
}
