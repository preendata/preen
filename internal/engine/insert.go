package engine

import (
	"database/sql/driver"
	"fmt"
)

func Insert(modelName string, ic <-chan []driver.Value, dc chan<- []int64) {
	connector, err := CreateConnector()
	if err != nil {
		panic(err)
	}
	appender, err := NewAppender(connector, "main", modelName)
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
			Debug(fmt.Sprintf(
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
				Info(fmt.Sprintf("Inserted %d rows into model %s", message[0], modelName))
				return
			}
			if message[0] == rowsExpected {
				Info(fmt.Sprintf("Inserted %d rows into model %s. Expected %d rows", message[0], modelName, rowsExpected))
				return
			}
			if message[0] != rowsExpected {
				Error(fmt.Sprintf("Inserted %d rows into model %s. Expected %d rows", message[0], modelName, rowsExpected))
				return
			}
		}
	}
}
