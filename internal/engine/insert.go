package engine

import (
	"database/sql/driver"
	"fmt"
)

func Insert(modelName ModelName, ic <-chan []driver.Value, dc chan<- []int64) {
	connector, err := ddbCreateConnector()
	if err != nil {
		panic(err)
	}
	appender, err := ddbNewAppender(connector, "main", string(modelName))
	if err != nil {
		panic(err)
	}
	rowCounter := 0
	for message := range ic {
		if message[0] == "quit" {
			break
		}
		Debug(fmt.Sprintf("Inserting row: %+v", message))
		for i, val := range message {
			Debug(fmt.Sprintf("Column %d: Type %T, Value %v", i, val, val))
		}
		if err := appender.AppendRow(message...); err != nil {
			Error(fmt.Sprintf("Failed to append row: %v", err))
			Error(fmt.Sprintf("Row data: %+v", message))
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
	for message := range dc {
		if rowsExpected == 0 {
			Debug(fmt.Sprintf("Inserted %d rows into model %s", message[0], modelName))
			break
		}
		if message[0] == rowsExpected {
			Debug(fmt.Sprintf("Inserted %d rows into model %s. Expected %d rows", message[0], modelName, rowsExpected))
			break
		}
		if message[0] != rowsExpected {
			Error(fmt.Sprintf("Inserted %d rows into model %s. Expected %d rows", message[0], modelName, rowsExpected))
			break
		}
	}
}
