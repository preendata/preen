package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/mysql"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/hyphadb/hyphadb/internal/utils"
	"golang.org/x/sync/errgroup"
)

func BuildInformationSchema(cfg *config.Config) error {
	// Ensure info schema table exists
	if err := prepareDDBInformationSchema(); err != nil {
		return err
	}

	// Reuse the insert function to insert data to the information schema
	ic := make(chan []driver.Value, 10)
	dc := make(chan []int64)

	go Insert("hypha_information_schema", ic, dc)

	// Group sources by engine to distribute across specific engine handlers
	hyphaSourcesByEngine := groupSourceByEngine(cfg)

	sourceErrGroup := new(errgroup.Group)

	for engine, sources := range hyphaSourcesByEngine {
		engine := engine
		sources := sources
		sourceErrGroup.Go(func() error {
			switch engine {
			case "postgres":
				if err = buildPostgresInformationSchema(sources, ic); err != nil {
					return err
				}
			case "mysql":
				if err = buildMySQLInformationSchema(sources, ic); err != nil {
					return err
				}
			case "mongodb":
				utils.Debug("No information schema required for MongoDB")
			default:
				return fmt.Errorf("unsupported engine: %s", engine)
			}

			return nil
		})
	}

	if err := sourceErrGroup.Wait(); err != nil {
		return err
	}
	ic <- []driver.Value{"quit"}
	ConfirmInsert("hypha_information_schema", dc, 0)

	return nil
}

// buildMySQLInformationSchema builds the information schema for all mysql sources in the config
func buildMySQLInformationSchema(sources []config.Source, ic chan<- []driver.Value) error {
	schemaErrGroup := new(errgroup.Group)

	for _, source := range sources {
		func(source config.Source) error {
			schemaErrGroup.Go(func() error {
				// Open new pool for every source
				pool, err := mysql.PoolFromSource(source)
				if err != nil {
					return err
				}

				defer pool.Close()

				// Run through all models in the source, inserting its information schema into the local hyphaContext in raw form
				for _, model := range source.Models {
					table := model
					// MySQL does not have schemas, so we use the database name
					schema := source.Connection.Database

					query := fmt.Sprintf(`
						SELECT column_name, data_type 
						FROM information_schema.columns 
						WHERE table_schema = '%s' AND table_name = '%s';
					`, schema, table)

					rows, err := pool.Query(query)
					if err != nil {
						return err
					}

					defer rows.Close()

					for rows.Next() {
						var column_name string
						var data_type string
						err = rows.Scan(&column_name, &data_type)

						if err != nil {
							return err
						}
						ic <- []driver.Value{source.Name, table, column_name, data_type}
					}
				}
				return nil
			})
			return nil
		}(source)
	}
	if err := schemaErrGroup.Wait(); err != nil {
		return err
	}

	return nil
}

// buildPostgresInformationSchema builds the information schema for all postgres sources in the config
func buildPostgresInformationSchema(sources []config.Source, ic chan<- []driver.Value) error {
	schemaErrGroup := new(errgroup.Group)

	for _, source := range sources {
		func(source config.Source) error {
			schemaErrGroup.Go(func() error {
				// Open new pool for every source
				pool, err := pg.PoolFromSource(source)
				if err != nil {
					return err
				}

				defer pool.Close()

				// Run through all models in the source, inserting its information schema into the local hyphaContext in raw form
				for _, model := range source.Models {
					table := model
					schema := "public"

					query := fmt.Sprintf(`
						select column_name, data_type from information_schema.columns
						where table_schema = '%s' and table_name = '%s';
					`, schema, table)

					rows, err := pool.Query(context.Background(), query)
					if err != nil {
						return err
					}

					defer rows.Close()

					for rows.Next() {
						values, err := rows.Values()
						if err != nil {
							return err
						}
						ic <- []driver.Value{source.Name, table, values[0], values[1]}
					}
				}
				return nil
			})
			return nil
		}(source)
	}
	if err := schemaErrGroup.Wait(); err != nil {
		return err
	}

	return nil
}

// groupSourceByEngine reduces the raw config.Sources into a map of engine -> sources
func groupSourceByEngine(cfg *config.Config) map[string][]config.Source {
	engines := make(map[string][]config.Source)
	for _, source := range cfg.Sources {
		engines[source.Engine] = append(engines[source.Engine], source)
	}

	return engines
}

// prepareDDBInformationSchema creates the table for the information schema in duckDB
func prepareDDBInformationSchema() error {
	informationSchemaColumnNames := []string{"source_name varchar", "table_name varchar", "column_name varchar", "data_type varchar"}
	informationSchemaTableName := "main.hypha_information_schema"
	utils.Debug(fmt.Sprintf("Creating table %s", informationSchemaTableName))
	err := duckdb.DMLQuery(fmt.Sprintf("create or replace table %s (%s)", informationSchemaTableName, strings.Join(informationSchemaColumnNames, ", ")))
	if err != nil {
		return err
	}

	return nil
}
