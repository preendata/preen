package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcboeker/go-duckdb"
)

type QueryResult struct {
	Rows    []map[string]any
	Columns []string
}

func getPostgresPool(url string) (*pgxpool.Pool, error) {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	dbpool, err := pgxpool.New(context.Background(), url)

	if err != nil {
		Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}
	return dbpool, nil
}

func getPostgresPoolFromSource(source Source) (*pgxpool.Pool, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		url.QueryEscape(source.Connection.Password),
		url.QueryEscape(source.Connection.Host),
		source.Connection.Port,
		source.Connection.Database,
	)
	dbpool, err := getPostgresPool(url)

	if err != nil {
		return nil, err
	}

	return dbpool, nil
}

func ingestPostgresModel(r *Retriever, ic chan []driver.Value) error {
	Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	clientPool, err := getPostgresPoolFromSource(r.Source)
	if err != nil {
		return err
	}
	defer clientPool.Close()
	rows, err := clientPool.Query(context.Background(), r.Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if err = processPostgresRows(r, ic, rows); err != nil {
		return err
	}

	return nil
}

func processPostgresRows(r *Retriever, ic chan []driver.Value, rows pgx.Rows) error {
	var rowCounter int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		rowCounter++
		driverRow := make([]driver.Value, len(values)+1)
		driverRow[0] = r.Source.Name
		for i, value := range values {
			if value == nil {
				driverRow[i+1] = nil
				continue
			}
			switch reflect.TypeOf(value).String() {
			case "pgtype.Numeric":
				decimal := duckdbDecimal(0)
				if err = decimal.Scan(value); err != nil {
					return err
				}
				driverRow[i+1], err = decimal.Value()
				if err != nil {
					return err
				}
			case "pgtype.Time":
				timeVal := duckdbTime("")
				if err = timeVal.Scan(value); err != nil {
					return err
				}
				driverRow[i+1], err = timeVal.Value()
				if err != nil {
					return err
				}
			case "pgtype.Interval":
				duration := duckdbDuration("")
				if err = duration.Scan(value); err != nil {
					return err
				}
				driverRow[i+1], err = duration.Value()
				if err != nil {
					return err
				}
			case "netip.Prefix":
				prefix := duckdbNetIpPrefix("")
				if err = prefix.Scan(value); err != nil {
					return err
				}
				driverRow[i+1], err = prefix.Value()
				if err != nil {
					return err
				}
			case "net.HardwareAddr":
				hwAddr := duckdbHardwareAddr("")
				if err = hwAddr.Scan(value); err != nil {
					return err
				}
				driverRow[i+1], err = hwAddr.Value()
				if err != nil {
					return err
				}
			case "map[string]interface {}", "[]interface {}":
				jsonVal := duckdbJSON("")
				if err = jsonVal.Scan(value); err != nil {
					return err
				}
				driverRow[i+1], err = jsonVal.Value()
				if err != nil {
					return err
				}
			// These are UUIDs
			case "[16]uint8":
				uuid := duckdbUUID(duckdb.UUID{})
				if err = uuid.Scan(value); err != nil {
					return err
				}
				driverRow[i+1], err = uuid.Value()
				if err != nil {
					return err
				}
			default:
				driverRow[i+1] = value
			}
		}
		ic <- driverRow
	}
	Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ModelName))
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
