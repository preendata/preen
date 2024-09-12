package engine

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"
)

// Implements the Scanner and Valuer interfaces for custom data types.
// https://pkg.go.dev/database/sql#Scanner

// duckdbDecimal is a custom type for scanning and valuing float64 values.
// The MySQL driver returns numeric types as strings, so we need to convert them to float64.
// The PG driver returns numeric types as a custom type, so we need to convert them to float64.
type duckdbDecimal float64

func (d *duckdbDecimal) Scan(s any) error {
	switch s.(type) {
	// The byte array is from the MySQL driver.
	case []byte:
		if float, err := strconv.ParseFloat(string(s.([]byte)), 64); err == nil {
			*d = duckdbDecimal(float)
		} else {
			return fmt.Errorf("error scanning duckdbDecimal: %w", err)
		}
	// The float32 type is from the MySQL driver.
	case float32:
		*d = duckdbDecimal(s.(float32))
	// The float64 type is from the MySQL driver.
	case float64:
		*d = duckdbDecimal(s.(float64))
	// The numeric type is from the PG driver.
	case pgtype.Numeric:
		numericType := s.(pgtype.Numeric)
		decimal := duckdb.Decimal{Value: numericType.Int, Scale: uint8(math.Abs(float64(numericType.Exp)))}
		*d = duckdbDecimal(decimal.Float64())
	case nil:
		*d = duckdbDecimal(0)
	default:
		fmt.Printf("type: %T\n", s)
		return fmt.Errorf("cannot sql.Scan() duckdbDecimal from: %#v", s)
	}
	return nil
}

func (d duckdbDecimal) Value() (driver.Value, error) {
	return float64(d), nil
}

// duckdbTime is a custom type for scanning and valuing time.Time values.
// The PG driver returns time types as a custom type, so we need to convert them to string.
// The database/sql driver doesn't respect time data types.
type duckdbTime string

func (t *duckdbTime) Scan(s any) error {
	switch s.(type) {
	case pgtype.Time:
		timeType := s.(pgtype.Time)
		// Create a Time object for midnight of the current day
		midnight := time.Now().Truncate(24 * time.Hour)
		resultTime := midnight.Add(time.Duration(timeType.Microseconds) * time.Microsecond)
		*t = duckdbTime(resultTime.String())
	case nil:
		*t = duckdbTime("")
	default:
		return fmt.Errorf("cannot sql.Scan() duckdbTime from: %#v", s)
	}
	return nil
}

func (t *duckdbTime) Value() (driver.Value, error) {
	return fmt.Sprint(t), nil
}

// duckdbDuration is a custom type for scanning and valuing time.Duration values.
// The PG driver returns interval types as a custom type, so we need to convert them to time.Duration.
type duckdbDuration time.Duration

func (d *duckdbDuration) Scan(s any) error {
	switch v := s.(type) {
	case int64:
		*d = duckdbDuration(v)
	case nil:
		*d = duckdbDuration(0)
	default:
		return fmt.Errorf("cannot sql.Scan() strfmt.Duration from: %#v", v)
	}
	return nil
}

func (d duckdbDuration) Value() (driver.Value, error) {
	return int64(d), nil
}

// duckdbNetIPPrefix is a custom type for scanning and valuing netip.Prefix values.
// The PG driver returns inet types as a custom type, so we need to convert them to string.
type duckdbNetIpPrefix string

func (d *duckdbNetIpPrefix) Scan(s any) error {
	switch v := s.(type) {
	case netip.Prefix:
		*d = duckdbNetIpPrefix(v.String())
	case nil:
		*d = duckdbNetIpPrefix("")
	default:
		return fmt.Errorf("cannot sql.Scan() netip.Prefix from: %#v", v)
	}
	return nil
}

func (d duckdbNetIpPrefix) Value() (driver.Value, error) {
	return string(d), nil
}

// duckdbHardwareAddr is a custom type for scanning and valuing net.HardwareAddr values.
// The PG driver returns macaddr types as a custom type, so we need to convert them to string.
type duckdbHardwareAddr string

func (d *duckdbHardwareAddr) Scan(s any) error {
	switch v := s.(type) {
	case net.HardwareAddr:
		*d = duckdbHardwareAddr(v.String())
	case nil:
		*d = duckdbHardwareAddr("")
	default:
		return fmt.Errorf("cannot sql.Scan() net.HardwareAddr from: %#v", v)
	}
	return nil
}

func (d duckdbHardwareAddr) Value() (driver.Value, error) {
	return string(d), nil
}

// duckdbJSON is a custom type for scanning and valuing json values.
// The PG driver returns json types as a custom type, so we need to convert them to string.
type duckdbJSON string

func (j *duckdbJSON) Scan(s any) error {
	switch v := s.(type) {
	case map[string]interface{}, []interface{}:
		jsonVal, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("error scanning duckdbJSON: %w", err)
		}
		*j = duckdbJSON(jsonVal)
	case nil:
		*j = duckdbJSON("")
	default:
		return fmt.Errorf("cannot sql.Scan() duckdbJSON from: %#v", v)
	}
	return nil
}

func (j duckdbJSON) Value() (driver.Value, error) {
	return string(j), nil
}

// duckdbUUID is a custom type for scanning and valuing UUID values.
// The PG driver returns UUID types as a custom type, so we need to convert them to string.
type duckdbUUID duckdb.UUID

func (u *duckdbUUID) Scan(s any) error {
	switch v := s.(type) {
	case [16]uint8:
		value := duckdb.UUID(v)
		*u = duckdbUUID(value)
	case nil:
		*u = duckdbUUID(duckdb.UUID([]uint8{}))
	default:
		return fmt.Errorf("cannot sql.Scan() duckdbUUID from: %#v", v)
	}
	return nil
}

func (u duckdbUUID) Value() (driver.Value, error) {
	return duckdb.UUID(u), nil
}

var duckdbTypeMap = map[string]string{
	"integer":                     "integer",
	"bigint":                      "bigint",
	"interval":                    "bigint",
	"smallint":                    "smallint",
	"mediumint":                   "integer",
	"int":                         "integer",
	"year":                        "smallint",
	"double precision":            "double",
	"double":                      "double",
	"numeric":                     "double",
	"decimal":                     "double",
	"real":                        "real",
	"float4":                      "real",
	"float":                       "real",
	"boolean":                     "boolean",
	"date":                        "date",
	"timestamp":                   "timestamp",
	"datetime":                    "timestamp",
	"timestamp without time zone": "timestamp",
	"binary":                      "blob",
	"varbinary":                   "blob",
	"tinyblob":                    "blob",
	"blob":                        "blob",
	"mediumblob":                  "blob",
	"longblob":                    "blob",
	"bytea":                       "blob",
	"json":                        "json",
	"jsonb":                       "json",
	"inet":                        "varchar",
	"cidr":                        "varchar",
	"macaddr":                     "varchar",
	"ARRAY":                       "json",
	"xml":                         "varchar",
	"int4range":                   "varchar",
	"varchar":                     "varchar",
	"tinyint":                     "tinyint",
	"char":                        "varchar",
	"tinytext":                    "varchar",
	"mediumtext":                  "varchar",
	"longtext":                    "varchar",
	"character varying":           "varchar",
	"text":                        "varchar",
	"character":                   "varchar",
	"enum":                        "varchar",
	"set":                         "varchar",
	"time without time zone":      "varchar",
	"time":                        "varchar",
	"uuid":                        "uuid",
}
