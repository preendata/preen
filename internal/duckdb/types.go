package duckdb

var PgTypeMap = map[string]string{
	"integer":           "integer",
	"bigint":            "int8",
	"double precision":  "double",
	"real":              "real",
	"float4":            "real",
	"smallint":          "smallint",
	"character varying": "varchar",
	"text":              "varchar",
	"character":         "varchar",
	"boolean":           "boolean",
	"date":              "date",
	"numeric":           "double",
	"decimal":           "double",
	// begin mysql types
	"varchar": "varchar",
	"tinyint": "smallint",
}
