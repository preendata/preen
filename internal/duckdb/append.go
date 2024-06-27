package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/marcboeker/go-duckdb"
)

func GetDuckDbColumns(connector driver.Connector, schema string, table string) ([]string, error) {
	db, err := OpenDatabase(connector)
	response := make([]string, 0)

	if err != nil {
		return nil, err
	}

	defer db.Close()
	rows, err := db.Query(fmt.Sprintf(`
		select 
			column_name 
		from 
			information_schema.columns 
		where 
			table_schema = '%v' and table_name = '%v'
		order by 
			ordinal_position`, schema, table,
	))

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var column string
		rows.Scan(&column)
		response = append(response, column)
	}

	return response, nil
}

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
