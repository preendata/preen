package engine

import (
	"database/sql/driver"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/utils"
)

func Insert(contextName string, ic chan []driver.Value, dc chan []int64) error {
	connector, err := duckdb.CreateConnector()
	if err != nil {
		panic(err)
	}
	appender, err := duckdb.NewAppender(connector, "main", contextName)
	if err != nil {
		panic(err)
	}
	rowCounter := 0
	for c := range ic {
		if c[0] == "EOF" {
			break
		}
		if err := appender.AppendRow(c...); err != nil {
			panic(err)
		}
		rowCounter++
		if rowCounter%1000000 == 0 {
			utils.Debug(fmt.Sprintf(
				"Flushing 1M rows from appender to DuckDB for context: %s, %d", contextName, rowCounter,
			))
			err := appender.Flush()
			if err != nil {
				panic(err)
			}
		}
	}
	err = appender.Close()
	if err != nil {
		panic(err)
	}
	dc <- []int64{int64(rowCounter)}
	return nil
}
