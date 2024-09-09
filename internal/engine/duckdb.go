package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"
)

// Returns a DuckDB appender instance for bulk loading of data
func ddbNewAppender(connector driver.Connector, schema string, table string) (*duckdb.Appender, error) {
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

func ddbCreateConnector() (driver.Connector, error) {
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

func ddbOpenDatabase(connector driver.Connector) (*sql.DB, error) {
	db := sql.OpenDB(connector)
	return db, nil
}

func ddbInsertResults(sourceName string, tableName string, c chan []any) error {
	fmt.Println(len(c))
	rowCounter := 0
	Debug(fmt.Sprintf("Inserting rows into %s", tableName))
	connector, err := ddbCreateConnector()
	if err != nil {
		return err
	}

	appender, err := ddbNewAppender(connector, "main", tableName)
	if err != nil {
		return err
	}

	for row := range c {
		rowCounter++
		if row[0] == "done" {
			break
		}
		err := ddbProcessRow(appender, row, sourceName)
		if err != nil {
			return err
		}
	}

	fmt.Printf("Inserted %d rows into %s", rowCounter, tableName)

	return nil
}

func ddbProcessRow(appender *duckdb.Appender, row []any, sourceName string) error {
	driverRow := make([]driver.Value, len(row)+1)
	driverRow[0] = sourceName
	for i, value := range row {
		if value == nil {
			driverRow[i+1] = nil
			continue
		}
		if reflect.TypeOf(value).String() == "pgtype.Numeric" {
			val := duckdb.Decimal{Value: value.(pgtype.Numeric).Int, Scale: uint8(math.Abs(float64(value.(pgtype.Numeric).Exp)))}
			driverRow[i+1] = val.Float64()
		} else {
			driverRow[i+1] = value
		}
	}
	err := appender.AppendRow(driverRow...)
	if err != nil {
		return err
	}
	return nil
}

func ddbDmlQuery(queryString string) error {
	connector, err := ddbCreateConnector()
	if err != nil {
		return err
	}

	db, err := ddbOpenDatabase(connector)
	if err != nil {
		return err
	}

	defer db.Close()
	Debug("querying duckdb database with query: ", queryString)
	_, err = db.Exec(queryString)
	if err != nil {
		return err
	}
	return err
}

func ddbQuery(queryString string, c chan map[string]any) ([]string, error) {
	connector, err := ddbCreateConnector()
	if err != nil {
		return nil, err
	}

	db, err := ddbOpenDatabase(connector)
	if err != nil {
		return nil, err
	}

	defer db.Close()
	Debug("querying duckdb database with query: ", queryString)
	rows, err := db.Query(queryString)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	err = ReadRows(rows, c)

	if err != nil {
		return nil, err
	}
	return columns, err
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
