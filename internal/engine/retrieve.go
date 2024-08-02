package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"

	"github.com/hyphadb/hyphadb/internal/config"
	duckdbInternal "github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb"
)

// This needs to be broken up into smaller functions and add goroutines
func Retrieve(cfg *config.Config, c Context) error {
	connector, err := duckdbInternal.CreateConnector()
	if err != nil {
		return err
	}

	for _, source := range cfg.Sources {
		pool, err := pg.PoolFromSource(source)
		if err != nil {
			return err
		}
		if source.Engine == "postgres" {
			for _, contextName := range source.Contexts {
				query := c.ContextQueries[contextName].Query
				rows, err := pool.Query(context.Background(), query)
				if err != nil {
					return err
				}
				defer rows.Close()

				appender, err := duckdbInternal.NewAppender(connector, "main", contextName)
				if err != nil {
					return err
				}

				for rows.Next() {
					values, err := rows.Values()
					if err != nil {
						return err
					}
					driverRow := make([]driver.Value, len(values)+1)
					driverRow[0] = source.Name
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
						fmt.Println(err)
						return err
					}
				}
				err = appender.Close()
				if err != nil {
					fmt.Println(err)
					return err
				}
				err = rows.Err()
				if err != nil {
					fmt.Println(err)
					return err
				}
			}
		}
	}
	return nil
}
