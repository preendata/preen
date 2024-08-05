package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcboeker/go-duckdb"
)

type Retriever struct {
	ContextName string
	Query       string
	Source      config.Source
	Pool        *pgxpool.Pool
}

func Retrieve(cfg *config.Config, c Context) error {
	r := Retriever{}
	for _, contextName := range cfg.Contexts {
		wg := sync.WaitGroup{}
		r.ContextName = contextName
		r.Query = c.ContextQueries[contextName].Query
		ic := make(chan []driver.Value, 1000)
		dc := make(chan []int64)
		go Insert(contextName, ic, dc)
		if err != nil {
			return err
		}
		var rowCounter int64
		rowCounter = 0
		for _, source := range cfg.Sources {
			r.Source = source
			wg.Add(1)
			if !slices.Contains(source.Contexts, contextName) {
				utils.Debug(fmt.Sprintf("Skipping %s for %s", contextName, source.Name))
				continue
			}
			if source.Engine == "postgres" {
				r.Pool, err = pg.PoolFromSource(source)
				defer r.Pool.Close()
				if err != nil {
					return err
				}
				defer r.Pool.Close()
				go func(r Retriever, ic chan []driver.Value) error {
					defer wg.Done()

					contextRowCount, err := processPgSource(r, ic)
					if err != nil {
						return err
					}
					rowCounter += *contextRowCount
					return nil
				}(r, ic)
			} else {
				utils.Error(fmt.Sprintf("Engine %s not supported", source.Engine))
			}
		}
		wg.Wait()
		ic <- []driver.Value{"EOF"}
		confirmInsert(contextName, dc, rowCounter)

		if err != nil {
			return err
		}
	}
	return nil
}

func processPgSource(r Retriever, ic chan []driver.Value) (*int64, error) {
	utils.Info(fmt.Sprintf("Retrieving context %s for %s", r.ContextName, r.Source.Name))
	rows, err := r.Pool.Query(context.Background(), r.Query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var rowCounter int64
	rowCounter = 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rowCounter++
		driverRow := make([]driver.Value, len(values)+1)
		driverRow[0] = r.Source.Name
		for i, value := range values {
			if value == nil {
				driverRow[i+1] = nil
				continue
			}
			if reflect.TypeOf(value).String() == "pgtype.Numeric" {
				val := duckdb.Decimal{Value: value.(pgtype.Numeric).Int, Scale: uint8(math.Abs(float64(value.(pgtype.Numeric).Exp)))}
				driverRow[i+1] = val.Float64()
			} else {
				driverRow[i+1] = value
			}
		}
		ic <- driverRow
	}
	utils.Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ContextName))
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return &rowCounter, nil
}

func confirmInsert(contextName string, dc chan []int64, rowsExpected int64) {
	for {
		select {
		case message := <-dc:
			if message[0] == rowsExpected {
				utils.Info(fmt.Sprintf("Inserted %d rows into context %s. Expected %d rows", rowsExpected, contextName, rowsExpected))
				return
			}
			if message[0] != rowsExpected {
				utils.Warn(fmt.Sprintf("Inserted %d rows into context %s. Expected %d rows", message[0], contextName, rowsExpected))
				return
			}
		}
	}
}
