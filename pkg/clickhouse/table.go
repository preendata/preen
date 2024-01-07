package clickhouse

import (
	"log/slog"

	"github.com/scalecraft/plex-db/pkg/config"
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

func CreateTables(cfg *config.Config) error {
	slog.Info("Creating tables in Clickhouse.")
	// ctx := context.Background()

	// conn := Connect(ctx, cfg)

	// for k, v := range *colTypes {
	// 	fmt.Println(k, v)
	// }

	return nil
}
