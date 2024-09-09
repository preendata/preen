package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"golang.org/x/sync/errgroup"
)

func BuildInformationSchema(sc *SourceConfig, mc *ModelConfig) error {
	// Ensure info schema table exists
	if err := prepareDDBInformationSchema(); err != nil {
		return err
	}

	// Reuse the insert function to insert data to the information schema
	ic := make(chan []driver.Value, 10)
	dc := make(chan []int64)

	go Insert("hypha_information_schema", ic, dc)

	// Group sources by engine to distribute across specific engine handlers
	hyphaSourcesByEngine := groupSourceByEngine(sc)

	sourceErrGroup := new(errgroup.Group)

	for engine, sources := range hyphaSourcesByEngine {
		sourceErrGroup.Go(func() error {
			switch engine {
			case "postgres":
				if err = buildPostgresInformationSchema(sources, ic, mc); err != nil {
					return fmt.Errorf("error building postgres information schema: %w", err)
				}
			case "mysql":
				if err = buildMySQLInformationSchema(sources, ic, mc); err != nil {
					return fmt.Errorf("error building mysql information schema: %w", err)
				}
			case "mongodb":
				Debug("No information schema required for MongoDB")
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
func buildMySQLInformationSchema(sources []Source, ic chan<- []driver.Value, mc *ModelConfig) error {
	schemaErrGroup := new(errgroup.Group)

	for _, source := range sources {
		func(source Source) error {
			schemaErrGroup.Go(func() error {
				// Open new pool for every source
				pool, err := GetMysqlPoolFromSource(source)
				if err != nil {
					return err
				}

				defer pool.Close()

				// Iterate over all models and get the tables for each model
				for modelName, modelConfig := range mc.Models {
					if modelConfig.Type == "sql" {
						tablesQueryString := ""
						for _, tableName := range modelConfig.TableSet {
							if tablesQueryString != "" {
								tablesQueryString += fmt.Sprintf(",'%s'", tableName)
							} else {
								tablesQueryString += fmt.Sprintf("'%s'", tableName)
							}
						}

						// MySQL does not have schemas, so we use the database name
						schema := source.Connection.Database

						query := fmt.Sprintf(`
							select table_name, column_name, data_type from information_schema.columns 
							where table_schema = '%s' and table_name in (%s);
						`, schema, tablesQueryString)

						rows, err := pool.Query(query)
						if err != nil {
							return err
						}

						defer rows.Close()

						for rows.Next() {
							var table_name string
							var column_name string
							var data_type string
							err = rows.Scan(&table_name, &column_name, &data_type)

							if err != nil {
								return err
							}
							ic <- []driver.Value{source.Name, string(modelName), table_name, column_name, data_type}
						}
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
func buildPostgresInformationSchema(sources []Source, ic chan<- []driver.Value, mc *ModelConfig) error {
	schemaErrGroup := new(errgroup.Group)

	for _, source := range sources {
		func(source Source) error {
			schemaErrGroup.Go(func() error {
				// Open new pool for every source
				pool, err := getPostgresPoolFromSource(source)
				if err != nil {
					return err
				}

				defer pool.Close()
				schema := "public"

				// Iterate over all models and get the tables for each model
				for modelName, modelConfig := range mc.Models {
					if modelConfig.Type == "sql" {
						tablesQueryString := ""
						for _, tableName := range modelConfig.TableSet {
							if tablesQueryString != "" {
								tablesQueryString += fmt.Sprintf(",'%s'", tableName)
							} else {
								tablesQueryString += fmt.Sprintf("'%s'", tableName)
							}
						}

						query := fmt.Sprintf(`
							select table_name, column_name, data_type from information_schema.columns
							where table_schema = '%s' and table_name in (%s);
						`, schema, tablesQueryString)

						rows, err := pool.Query(context.Background(), query)
						if err != nil {
							return fmt.Errorf("error querying postgres information schema: %w", err)
						}

						defer rows.Close()

						for rows.Next() {
							values, err := rows.Values()
							if err != nil {
								return err
							}
							ic <- []driver.Value{source.Name, string(modelName), values[0], values[1], values[2]}
						}
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
func groupSourceByEngine(sc *SourceConfig) map[string][]Source {
	engines := make(map[string][]Source)
	for _, source := range sc.Sources {
		engines[source.Engine] = append(engines[source.Engine], source)
	}

	return engines
}

// prepareDDBInformationSchema creates the table for the information schema in duckDB
func prepareDDBInformationSchema() error {
	informationSchemaColumnNames := []string{"source_name varchar", "model_name varchar", "table_name varchar", "column_name varchar", "data_type varchar"}
	informationSchemaTableName := "main.hypha_information_schema"
	Debug(fmt.Sprintf("Creating table %s", informationSchemaTableName))
	err := ddbDmlQuery(fmt.Sprintf("create or replace table %s (%s)", informationSchemaTableName, strings.Join(informationSchemaColumnNames, ", ")))
	if err != nil {
		return err
	}

	return nil
}
