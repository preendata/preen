package duckdb

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
