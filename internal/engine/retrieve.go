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
	duckdbInternal "github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcboeker/go-duckdb"
)

var poolMutex = &sync.Mutex{}
var rowMutex = &sync.Mutex{}

var rowWg = sync.WaitGroup{}

// This needs to be broken up into smaller functions and add goroutines
func Retrieve(cfg *config.Config, c Context) error {
	connector, err := duckdbInternal.CreateConnector()
	if err != nil {
		return err
	}

	for _, contextName := range cfg.Contexts {
		appender, err := duckdbInternal.NewAppender(connector, "main", contextName)
		if err != nil {
			return err
		}
		sourceWg := sync.WaitGroup{}

		for _, source := range cfg.Sources {
			sourceWg.Add(1)
			if !slices.Contains(source.Contexts, contextName) {
				utils.Debug(fmt.Sprintf("Skipping %s for %s", contextName, source.Name))
				continue
			}
			if source.Engine == "postgres" {
				poolMap := make(map[string]*pgxpool.Pool)
				poolMap[source.Name], err = pg.PoolFromSource(source)
				if err != nil {
					return err
				}
				defer poolMap[source.Name].Close()
				go func(source config.Source, poolMap map[string]*pgxpool.Pool) error {
					defer sourceWg.Done()

					query := c.ContextQueries[contextName].Query
					err := processPgSource(contextName, query, source, appender, poolMap)
					if err != nil {
						return err
					}
					return nil
				}(source, poolMap)
			} else {
				utils.Error(fmt.Sprintf("Engine %s not supported", source.Engine))
			}
		}
		fmt.Println("before wait")
		sourceWg.Wait()
		fmt.Println("after wait")
		err = appender.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func processPgSource(contextName string, query string, source config.Source, appender *duckdb.Appender, poolMap map[string]*pgxpool.Pool) error {
	utils.Info(fmt.Sprintf("Retrieving context %s for %s", contextName, source.Name))
	pool := poolMap[source.Name]
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err := processPgRows(rows, appender, source); err != nil {
		return err
	}

	err = rows.Err()
	if err != nil {
		return err
	}
	return nil
}

func processPgRows(rows pgx.Rows, appender *duckdb.Appender, source config.Source) error {
	defer poolMutex.Unlock()
	poolMutex.Lock()
	for rows.Next() {
		rowWg.Add(1)
		go func() error {
			defer rowWg.Done()
			if err := insertRow(rows, source.Name, appender); err != nil {
				return err
			}
			return nil
		}()
	}

	rowWg.Wait()
	return nil
}

func insertRow(rows pgx.Rows, sourceName string, appender *duckdb.Appender) error {
	defer rowMutex.Unlock()
	rowMutex.Lock()
	values, err := rows.Values()
	if err != nil {
		return err
	}
	driverRow := make([]driver.Value, len(values)+1)
	driverRow[0] = sourceName
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
	err = appender.AppendRow(driverRow...)
	if err != nil {
		return err
	}

	return nil
}
