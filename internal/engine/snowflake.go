package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/snowflakedb/gosnowflake"
)

func getSnowflakePoolFromSource(source Source) (*sql.DB, error) {

	config := gosnowflake.Config{
		Account:   source.Connection.Account,
		User:      source.Connection.Username,
		Password:  source.Connection.Password,
		Database:  source.Connection.Database,
		Schema:    source.Connection.Schema,
		Warehouse: source.Connection.Warehouse,
	}
	connStr, err := gosnowflake.DSN(&config)

	if err != nil {
		panic(err)
	}

	db, err := sql.Open("snowflake", connStr)
	if err != nil {
		panic(err)
	}
	err = db.PingContext(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error pinging Snowflake: %w", err)
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
	valuePtrs, err := processSnowflakeColumns(rows)
	if err != nil {
		return err
	}
	for rows.Next() {
		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}
		driverRow := make([]driver.Value, len(valuePtrs)+1)
		driverRow[0] = r.Source.Name
		for i, ptr := range valuePtrs {
			driverRow[i+1] = ptr
		}
		ic <- driverRow
	}

	return nil
}

func processSnowflakeColumns(rows *sql.Rows) ([]any, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	valuePtrs := make([]any, len(columnTypes))

	//TODO: make snowflake specific type changes
	for i, columnType := range columnTypes {
		switch columnType.DatabaseTypeName() {
		case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
			valuePtrs[i] = new(duckdbDecimal)
		case "BIGINT":
			valuePtrs[i] = new(int64)
		case "INT", "MEDIUMINT":
			valuePtrs[i] = new(int32)
		case "SMALLINT", "YEAR":
			valuePtrs[i] = new(int16)
		case "TINYINT":
			valuePtrs[i] = new(int8)
		case "BIT", "BINARY", "VARBINARY", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BLOB":
			valuePtrs[i] = new([]byte)
		case "DATE", "DATETIME", "TIMESTAMP":
			valuePtrs[i] = new(time.Time)
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET", "JSON", "TIME":
			valuePtrs[i] = new(string)
		default:
			return nil, fmt.Errorf("unsupported column type: %s", columnType.DatabaseTypeName())
		}
	}

	return nil, nil
}
