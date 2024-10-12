package engine

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	_ "github.com/snowflakedb/gosnowflake"
)

func getSnowflakePoolFromSource(source Source) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s",
		source.Connection.Username,
		source.Connection.Password,
		source.Connection.Host,
		source.Connection.Database,
		source.Connection.Schema,
		source.Connection.Warehouse,
		source.Connection.Role)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to Snowflake: %w", err)
	}

	return db, nil
}

func ingestSnowflakeModel(r *Retriever, ic chan []driver.Value) error {
	Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	clientPool, err := getSnowflakePoolFromSource(r.Source)
	if err != nil {
		return err
	}
	defer clientPool.Close()
	rows, err := clientPool.Query(r.Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err = processSnowflakeRows(r, ic, rows); err != nil {
		return err
	}

	return nil
}

func processSnowflakeRows(r *Retriever, ic chan []driver.Value, rows *sql.Rows) error {
	// TODO: Implement
	// valuePtrs, err := processSnowflakeColumns(rows)
	// if err != nil {
	// 	return err
	// }

	return nil
}
