package duckdb

import (
	"database/sql"

	"github.com/hyphadb/hyphadb/internal/hlog"
)

func Query(queryString string, c chan map[string]any) error {
	connector, err := CreateConnector()
	if err != nil {
		return err
	}

	db, err := OpenDatabase(connector)
	if err != nil {
		return err
	}

	defer db.Close()
	hlog.Debug("querying duckdb database with query: ", queryString)
	rows, err := db.Query(queryString)
	if err != nil {
		return err
	}

	err = ReadRows(rows, c)

	if err != nil {
		return err
	}
	return nil
}

func ReadRows(rows *sql.Rows, c chan map[string]any) error {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	numColumns := len(columns)

	values := make([]any, numColumns)
	for i := range values {
		values[i] = new(interface{})
	}

	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return err
		}

		dest := make(map[string]interface{}, numColumns)
		for i, column := range columns {
			dest[column] = *(values[i].(*interface{}))
		}
		c <- dest
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
