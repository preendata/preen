package engine

import (
	"database/sql/driver"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/hlog"
)

func (p *ParsedQuery) InsertResults(tableName string, rows []map[string]any) error {
	hlog.Debug(fmt.Sprintf("Inserting %d rows into %s", len(rows), tableName))
	connector, err := duckdb.CreateConnector()
	if err != nil {
		return err
	}

	appender, err := duckdb.NewAppender(connector, "main", tableName)
	if err != nil {
		return err
	}

	columns, err := duckdb.GetDuckDbColumns(connector, "main", tableName)

	if err != nil {
		return err
	}

	for _, row := range rows {
		duckDbRow := make([]driver.Value, len(columns))
		for i, column := range columns {
			duckDbRow[i] = row[column]
		}
		err = appender.AppendRow(duckDbRow...)
		if err != nil {
			return err
		}
	}
	err = appender.Close()
	if err != nil {
		return err
	}

	return nil
}
