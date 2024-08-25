package engine

import (
	"database/sql/driver"
	"fmt"

	"github.com/hyphasql/hypha/internal/duckdb"
	"github.com/hyphasql/hypha/internal/utils"
)

func Insert(modelName string, ic <-chan []driver.Value, dc chan<- []int64) {
	connector, err := duckdb.CreateConnector()
	if err != nil {
		panic(err)
	}
	appender, err := duckdb.NewAppender(connector, "main", modelName)
	if err != nil {
		panic(err)
	}
	rowCounter := 0
	for c := range ic {
		if c[0] == "quit" {
			break
		}
		if err := appender.AppendRow(c...); err != nil {
			panic(err)
		}
		rowCounter++
		if rowCounter%10000000 == 0 {
			utils.Debug(fmt.Sprintf(
				"Flushing 10M rows from appender to DuckDB for model: %s, %d", modelName, rowCounter,
			))
			if err := appender.Flush(); err != nil {
				panic(err)
			}
		}
	}
	if err = appender.Close(); err != nil {
		panic(err)
	}
	dc <- []int64{int64(rowCounter)}
}

func ConfirmInsert(modelName string, dc chan []int64, rowsExpected int64) {
	for {
		select {
		case message := <-dc:
			if rowsExpected == 0 {
				utils.Info(fmt.Sprintf("Inserted %d rows into model %s", message[0], modelName))
				return
			}
			if message[0] == rowsExpected {
				utils.Info(fmt.Sprintf("Inserted %d rows into model %s. Expected %d rows", message[0], modelName, rowsExpected))
				return
			}
			if message[0] != rowsExpected {
				utils.Error(fmt.Sprintf("Inserted %d rows into model %s. Expected %d rows", message[0], modelName, rowsExpected))
				return
			}
		}
	}
}
