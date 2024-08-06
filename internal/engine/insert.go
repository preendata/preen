package engine

import (
	"database/sql/driver"

	"github.com/hyphadb/hyphadb/internal/duckdb"
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
		// if err := appender.AppendRow(c...); err != nil {
		// 	panic(err)
		// }
		rowCounter++
	}
	err = appender.Close()
	if err != nil {
		panic(err)
	}
	dc <- []int64{int64(rowCounter)}
	return nil
}
