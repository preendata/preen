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
	for key := range q.Main.Columns {
		split := strings.Split(key, ".")
		tableName := split[0]
		columnName := split[1]
		if tableName != "results" {
			sourceDataType, ok := q.QueryContext.Validator.ColumnTypes[tableName][columnName]
			if !ok {
				hlog.Debug(fmt.Sprintf("column %s does not exist in table %s", columnName, tableName))
				continue
			}
			duckDbDataType := duckdb.PgTypeMap[sourceDataType.MajorityType]
			if len(tables[tableName]) == 0 {
				tables[tableName] += "hypha_source_name string"
			}
			tables[tableName] += ", " + columnName + " " + duckDbDataType
		}
	}

	for tableName, columnString := range tables {
		var tableExists bool
		for table := range q.Cfg.Tables {
			if q.Cfg.Tables[table].Name == tableName {
				tableExists = true
			}
		}
		if !tableExists {
			hlog.Debug("Table does not exist in config: ", tableName)
			continue
		}

		dropTableStatement := fmt.Sprintf("drop table if exists main.%s", tableName)
		createTableStatement := fmt.Sprintf("create table if not exists main.%s (%s)", tableName, columnString)
		hlog.Debug("Dropping table in DuckDB: ", dropTableStatement)
		_, err := db.Exec(dropTableStatement)

		if err != nil {
			return err
		}
		hlog.Debug("Creating table in DuckDB: ", createTableStatement)
		_, err = db.Exec(createTableStatement)

		if err != nil {
			return err
		}
	}

	q.QueryContext.Valid = true

	return nil
}
