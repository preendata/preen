package duckdb

import (
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"

	"github.com/hyphasql/hypha/internal/utils"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"
)

func InsertResults(sourceName string, tableName string, c chan []any) error {
	fmt.Println(len(c))
	rowCounter := 0
	utils.Debug(fmt.Sprintf("Inserting rows into %s", tableName))
	connector, err := CreateConnector()
	if err != nil {
		return err
	}

	appender, err := NewAppender(connector, "main", tableName)
	if err != nil {
		return err
	}

	for row := range c {
		rowCounter++
		if row[0] == "done" {
			break
		}
		err := processRow(appender, row, sourceName)
		if err != nil {
			return err
		}
	}

	fmt.Printf("Inserted %d rows into %s", rowCounter, tableName)

	return nil
}

func processRow(appender *duckdb.Appender, row []any, sourceName string) error {
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
