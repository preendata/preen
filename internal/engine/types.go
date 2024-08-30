package engine

import (
	"database/sql/driver"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgtype"
)

// Implements the Scanner and Valuer interfaces for custom data types.
// https://pkg.go.dev/database/sql#Scanner

// duckdbDecimal is a custom type for scanning and valuing float64 values.
// The MySQL driver returns numeric types as strings, so we need to convert them to float64.
// The PG driver returns numeric types as a custom type, so we need to convert them to float64.
type duckdbDecimal float64

// mysqlBool is a custom type for scanning and valuing bool values.
// The MySQL driver returns boolean types as int8 values, so we need to convert them to bool.
type mysqlBool bool

func (d *duckdbDecimal) Scan(s any) error {
	switch s.(type) {
	// The byte array is from the MySQL driver.
	case []byte:
		if float, err := strconv.ParseFloat(string(s.([]byte)), 64); err == nil {
			*d = duckdbDecimal(float)
		} else {
			return fmt.Errorf("error scanning duckdbDecimal: %w", err)
		}
	// The numeric type is from the PG driver.
	case pgtype.Numeric:
	}
	return nil
}

func (d duckdbDecimal) Value() (driver.Value, error) {
	return float64(d), nil
}

func (b *mysqlBool) Scan(s any) error {
	switch s.(type) {
	case int8:
		*b = mysqlBool(s.(int64) == 1)
	}
	return nil
}

func (b mysqlBool) Value() (driver.Value, error) {
	if b {
		return true, nil
	}
	return false, nil
}

var PgTypeMap = map[string]string{
	"integer":           "integer",
	"bigint":            "bigint",
	"smallint":          "smallint",
	"mediumint":         "integer",
	"int":               "integer",
	"double precision":  "double",
	"real":              "real",
	"float4":            "real",
	"character varying": "varchar",
	"text":              "varchar",
	"character":         "varchar",
	"boolean":           "boolean",
	"date":              "date",
	"numeric":           "double",
	"decimal":           "double",
	"timestamp":         "timestamp",
	"varchar":           "varchar",
	"tinyint":           "tinyint",
	"char":              "varchar",
}
