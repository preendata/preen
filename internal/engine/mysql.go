package engine

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hyphasql/hypha/internal/config"
	"github.com/hyphasql/hypha/internal/utils"
)

func GetMysqlPoolFromSource(source config.Source) (*sql.DB, error) {
	// Example url := "root:thisisnotarealpassword@tcp(127.0.0.1:33061)/mysql_db_1"
	url := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true",
		source.Connection.Username,
		url.QueryEscape(source.Connection.Password),
		url.QueryEscape(source.Connection.Host),
		source.Connection.Port,
		source.Connection.Database,
	)
	dbpool, err := getMysqlPool(url)

	if err != nil {
		return nil, err
	}

	return dbpool, nil
}

func getMysqlPool(url string) (*sql.DB, error) {
	dbPool, err := sql.Open("mysql", url)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}

	return dbPool, nil
}

// Retrieve retrieves data from a MySQL source and sends it to the insert channel.
func ingestMysqlSource(r *Retriever, ic chan []driver.Value) error {
	utils.Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	clientPool, err := GetMysqlPoolFromSource(r.Source)
	if err != nil {
		return err
	}
	defer clientPool.Close()
	rows, err := clientPool.Query(r.Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err = processMysqlRows(r, ic, rows); err != nil {
		return err
	}

	return nil
}

// processMysqlRows processes rows from a MySQL source and sends them to the insert channel.
func processMysqlRows(r *Retriever, ic chan []driver.Value, rows *sql.Rows) error {
	// Get the column types from the rows and create a slice of pointers to scan into.
	valuePtrs, err := processMysqlColumns(rows)
	for rows.Next() {
		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}
		driverRow := make([]driver.Value, len(valuePtrs)+1)
		driverRow[0] = r.Source.Name
		for i, ptr := range valuePtrs {
			if ptr == nil {
				driverRow[i+1] = nil
				continue
			}
			switch reflect.TypeOf(ptr).String() {
			case "*engine.mysqlBool":
				value := reflect.ValueOf(ptr).Elem().Interface()
				driverRow[i+1], err = value.(mysqlBool).Value()
				if err != nil {
					return err
				}
			case "*engine.duckdbDecimal":
				value := reflect.ValueOf(ptr).Elem().Interface()
				driverRow[i+1], err = value.(duckdbDecimal).Value()
				if err != nil {
					return err
				}
			default:
				// If the value is not a custom type, we can just use the value as is.
				driverRow[i+1] = reflect.ValueOf(ptr).Elem().Interface()
			}
		}
		ic <- driverRow
	}

	return nil
}

func processMysqlColumns(rows *sql.Rows) ([]any, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	valuePtrs := make([]any, len(columnTypes))

	for i, columnType := range columnTypes {
		switch columnType.DatabaseTypeName() {
		case "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL":
			valuePtrs[i] = new(duckdbDecimal)
		case "BIGINT":
			valuePtrs[i] = new(int64)
		case "INT":
			valuePtrs[i] = new(int32)
		case "SMALLINT":
			valuePtrs[i] = new(int16)
		case "TINYINT":
			valuePtrs[i] = new(mysqlBool)
		case "BIT":
			valuePtrs[i] = new([]byte)
		case "DATE", "TIME", "DATETIME", "TIMESTAMP":
			valuePtrs[i] = new(time.Time)
		case "CHAR", "VARCHAR", "TEXT", "BLOB":
			valuePtrs[i] = new(string)
		default:
			return nil, fmt.Errorf("unsupported column type: %s", columnType.DatabaseTypeName())
		}
	}
	return valuePtrs, nil
}
