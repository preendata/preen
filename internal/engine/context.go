package engine

import (
	"database/sql"
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

	connector, err := duckdb.CreateConnector()

	if err != nil {
		return err
	}

	db, err := duckdb.OpenDatabase(connector)

	if err != nil {
		return err
	}
	defer db.Close()

	tables := q.generateTableDDL()
	if err = q.dropTableIfDDLHasChanged(db, tables); err != nil {
		return err
	}
	err = q.BuildTables(db, tables)

	if err != nil {
		return err
	}

	return nil
}

func (q *Query) BuildTables(db *sql.DB, tables map[string]string) error {
	for tableName, columnString := range tables {

		createTableStatement := fmt.Sprintf("create table if not exists main.%s (%s)", tableName, columnString)

		hlog.Debug("Creating table in DuckDB: ", createTableStatement)
		_, err := db.Exec(createTableStatement)

		if err != nil {
			return err
		}
	}

	q.QueryContext.Valid = true

	return nil
}

func (q *Query) generateTableDDL() map[string]string {
	tables := make(map[string]string)

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
				tables[tableName] += "hypha_source_name VARCHAR"
			}
			tables[tableName] += ", " + columnName + " " + duckDbDataType
		}
	}
	return tables
}

func (q *Query) dropTableIfDDLHasChanged(db *sql.DB, tables map[string]string) error {
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

		rows, err := db.Query(fmt.Sprintf("select column_name, data_type from information_schema.columns where table_name = '%s'", tableName))
		if err != nil {
			hlog.Debug("Error querying information_schema.columns: ", err)
			return err
		}
		defer rows.Close()
		var columns []string
		for rows.Next() {
			var columnName, dataType string
			err = rows.Scan(&columnName, &dataType)
			if err != nil {
				hlog.Debug("Error scanning rows: ", err)
				return err
			}
			columns = append(columns, columnName+" "+dataType)
		}

		columnParts := strings.Split(columnString, ",")

		for _, column := range columns {
			if !strings.Contains(columnString, column) || len(columnParts) != len(columns) {
				hlog.Debug("Table DDL has changed for table ", tableName)
				dropTableStatement := fmt.Sprintf("drop table if exists main.%s", tableName)
				hlog.Debug("Dropping table in DuckDB: ", dropTableStatement)
				_, err := db.Exec(dropTableStatement)

				return err
			}
		}
	}

	return nil
}
