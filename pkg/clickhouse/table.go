package clickhouse

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

var typeMap = map[string]string{
	"character varying": "String",
	"smallint":          "Int16",
	"integer":           "Int32",
	"bigint":            "Int64",
	"boolean":           "Boolean",
	"date":              "Date",
	"timestamp":         "DateTime",
	"jsonb":             "String",
	"json":              "JSON",
	"numeric":           "Float",
	"double precision":  "Float",
	"real":              "Float",
}

func CreateTable(conn *ch.Client, ctx context.Context) error {

	return conn.Do(ctx, ch.Query{
		Body: `create table if not exists users (
			id String,
			first_name String,
			last_name String,
			email String,
			gender String,
			ip_address String,
			is_active String,
			source String
		) engine = Memory`,
	})
}
