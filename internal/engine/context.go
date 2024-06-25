package engine

import (
	"fmt"
	"strings"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/hlog"
	"github.com/hyphadb/hyphadb/internal/pg"
)

type QueryContext struct {
	Validator *pg.Validator
	Valid     bool
}

func (q *Query) BuildContext() error {
	q.QueryContext = QueryContext{}
	validator, err := pg.Validate(q.Cfg)

	if err != nil {
		return err
	}

	q.QueryContext.Validator = validator

	err = q.BuildTables()

	if err != nil {
		return err
	}

	return nil
}

func (q *Query) BuildTables() error {
	tables := make(map[string]string)
	connector, err := duckdb.CreateConnector()

	if err != nil {
		return err
	}

	db, err := duckdb.OpenDatabase(connector)

	if err != nil {
		return err
	}

	defer db.Close()

	for key := range q.Nodes[0].Columns {
		split := strings.Split(key, ".")
		tableName := split[0]
		columnName := split[1]
		if tableName != "results" {
			sourceDataType := q.QueryContext.Validator.ColumnTypes[tableName][columnName].MajorityType
			duckDbDataType := duckdb.PgTypeMap[sourceDataType]
			if len(tables[tableName]) == 0 {
				tables[tableName] += columnName + " " + duckDbDataType
			} else {
				tables[tableName] += ", " + columnName + " " + duckDbDataType
			}
		}
	}

	for tableName, columnString := range tables {
		statement := fmt.Sprintf("create table if not exists main.%s (%s)", tableName, columnString)
		hlog.Debug("Creating table in DuckDB: ", statement)
		_, err := db.Exec(statement)

		if err != nil {
			return err
		}
	}

	q.QueryContext.Valid = true

	return nil
}
