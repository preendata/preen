package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
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
		return fmt.Errorf("error querying Snowflake: %w", err)
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
		return fmt.Errorf("error processing Snowflake columns: %w", err)
	}
	for rows.Next() {
		if err = rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("error scanning Snowflake rows: %w", err)
		}
		driverRow := make([]driver.Value, len(valuePtrs)+1)
		driverRow[0] = r.Source.Name
		for i, ptr := range valuePtrs {
			switch v := ptr.(type) {
			case *duckdbDecimal:
				driverRow[i+1], err = v.Value()
				if err != nil {
					return fmt.Errorf("error converting duckdbDecimal: %w", err)
				}
			default:
				driverRow[i+1] = dereferenceIfPtr(ptr)
			}
		}
		ic <- driverRow
	}

	return nil
}

func dereferenceIfPtr[T any](v T) T {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		return rv.Elem().Interface().(T)
	}
	return v
}

func processSnowflakeColumns(rows *sql.Rows) ([]any, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	valuePtrs := make([]any, len(columnTypes))

	for i, columnType := range columnTypes {

		switch columnType.DatabaseTypeName() {
		case "DECIMAL", "NUMBER", "FLOAT", "DOUBLE", "REAL", "FIXED":
			valuePtrs[i] = new(duckdbDecimal)
		case "BIGINT":
			valuePtrs[i] = new(int64)
		case "BOOLEAN":
			valuePtrs[i] = new(bool)
		case "INT", "MEDIUMINT":
			valuePtrs[i] = new(int32)
		case "SMALLINT", "YEAR":
			valuePtrs[i] = new(int16)
		case "TINYINT":
			valuePtrs[i] = new(int8)
		case "BIT", "BINARY", "VARBINARY", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BLOB", "VARIANT", "OBJECT", "ARRAY":
			valuePtrs[i] = new([]byte)
		case "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_TZ", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ":
			valuePtrs[i] = new(time.Time)
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET", "JSON", "TIME":
			Debug(fmt.Sprintf("Column type is a string: %s", columnType.DatabaseTypeName()))
			valuePtrs[i] = new(string)
		default:
			return nil, fmt.Errorf("unsupported column type: %s", columnType.DatabaseTypeName())
		}
	}

	return valuePtrs, nil
}
