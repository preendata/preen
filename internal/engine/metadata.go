package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
)

// BuildMetadata builds any required metadata for the sources in the sources.yaml config.
// Postgres and MySQL sources require an information schema to be built.
// S3 sources require duckDB secrets to be stored.
func BuildMetadata(sc *SourceConfig, mc *ModelConfig) error {
	// Ensure info schema table exists
	if err := prepareDDBInformationSchema(); err != nil {
		return err
	}

	// Reuse the insert function to insert data to the information schema
	ic := make(chan []driver.Value, 10)
	dc := make(chan []int64)

	go Insert("preen_information_schema", ic, dc)

	// Group sources by engine to distribute across specific engine handlers
	preenSourcesByEngine := groupSourceByEngine(sc)

	sourceErrGroup := new(errgroup.Group)

	for engine, sources := range preenSourcesByEngine {
		sourceErrGroup.Go(func() error {
			switch engine {
			case "postgres":
				if err := buildPostgresInformationSchema(sources, ic, mc); err != nil {
					return fmt.Errorf("error building postgres information schema: %w", err)
				}
			case "mysql":
				if err := buildMySQLInformationSchema(sources, ic, mc); err != nil {
					return fmt.Errorf("error building mysql information schema: %w", err)
				}
			case "snowflake":
				if err := buildSnowflakeInformationSchema(sources, ic, mc); err != nil {
					return fmt.Errorf("error building snowflake information schema: %w", err)
				}
			case "mongodb":
				Debug("No information schema required for MongoDB")
			case "s3":
				if len(sources) > 1 {
					return fmt.Errorf("only one s3 source is supported")
				}
				if err := buildS3Secrets(sources[0]); err != nil {
					return fmt.Errorf("error configuring s3 access: %w", err)
				}
				if err := confirmS3Connection(sources[0]); err != nil {
					return fmt.Errorf("error confirming s3 objects: %w", err)
				}
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
	ConfirmInsert("preen_information_schema", dc, 0)
	Info("Metadata build completed successfully")

	return nil
}

// buildS3Secrets builds the secrets for all s3 sources in the config
// This is required to access the S3 bucket, https://duckdb.org/docs/extensions/httpfs/s3api.html
func buildS3Secrets(s Source) error {
	query := fmt.Sprintf(`
		install aws;
		load aws;
		create or replace persistent secret aws_s3 (
			type S3,
			region '%s',
			provider CREDENTIAL_CHAIN
		)
	`, s.Connection.Region)
	if err := ddbExec(query); err != nil {
		return err
	}
	return nil
}

// confirmS3Connection confirms that the S3 connection is working,
// and that at least one object is present inside the bucket.
func confirmS3Connection(s Source) error {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(s.Connection.Region),
	)
	if err != nil {
		return fmt.Errorf("error loading default config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	input := &s3.ListObjectsV2Input{
		Bucket: &s.Connection.BucketName,
	}

	result, err := s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return fmt.Errorf("unable to list items in bucket %q: %w", s.Connection.BucketName, err)
	}
	if len(result.Contents) == 0 {
		return fmt.Errorf("no objects found in bucket %q", s.Connection.BucketName)
	} else {
		Debug(fmt.Sprintf("Found %d objects in bucket %q", len(result.Contents), s.Connection.BucketName))
	}
	return nil
}

// buildMySQLInformationSchema builds the information schema for all mysql sources in the config
func buildMySQLInformationSchema(sources []Source, ic chan<- []driver.Value, mc *ModelConfig) error {
	schemaErrGroup := new(errgroup.Group)

	for _, source := range sources {
		err := func(source Source) error {
			schemaErrGroup.Go(func() error {
				// Open new pool for every source
				pool, err := GetMysqlPoolFromSource(source)
				if err != nil {
					return err
				}

				defer pool.Close()

				// Iterate over all models and get the tables for each model
				for _, model := range mc.Models {
					if model.Type == "database" && model.Parsed != nil {
						tablesQueryString := ""
						for _, tableName := range model.TableSet {
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
							ic <- []driver.Value{source.Name, string(model.Name), table_name, column_name, data_type}
						}
					}
				}
				return nil
			})
			return nil
		}(source)
		if err != nil {
			return err
		}
	}
	if err := schemaErrGroup.Wait(); err != nil {
		return err
	}

	return nil
}

// buildSnowflakeInformationSchema builds the information schema for all snowflake sources in the config
func buildSnowflakeInformationSchema(sources []Source, ic chan<- []driver.Value, mc *ModelConfig) error {
	schemaErrGroup := new(errgroup.Group)

	for _, source := range sources {
		schemaErrGroup.Go(func() error {
			pool, err := getSnowflakePoolFromSource(source)
			if err != nil {
				return err
			}
			defer pool.Close()
			schema := "'PUBLIC'"

			for _, model := range mc.Models {
				if model.Type == "database" && model.Parsed != nil {
					tablesQueryString := ""
					for _, tableName := range model.TableSet {
						if tablesQueryString != "" {
							tablesQueryString += fmt.Sprintf(",'%s'", tableName)
						} else {
							tablesQueryString += fmt.Sprintf("'%s'", tableName)
						}
					}

					query := fmt.Sprintf(`
							select table_name, column_name, data_type from %s.information_schema.columns
								where TABLE_SCHEMA = upper(%s) and table_name = upper(%s);
						`, source.Connection.Database, schema, tablesQueryString)
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
						ic <- []driver.Value{source.Name, string(model.Name), table_name, column_name, data_type}
					}
				}
			}
			return nil
		})
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
		err := func(source Source) error {
			schemaErrGroup.Go(func() error {
				// Open new pool for every source
				pool, err := getPostgresPoolFromSource(source)
				if err != nil {
					return err
				}

				defer pool.Close()
				schema := "public"

				// Iterate over all models and get the tables for each model
				for _, model := range mc.Models {
					if model.Type == "database" && model.Parsed != nil {
						tablesQueryString := ""
						for _, tableName := range model.TableSet {
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
							ic <- []driver.Value{source.Name, string(model.Name), values[0], values[1], values[2]}
						}
					}
				}
				return nil
			})
			return nil
		}(source)
		if err != nil {
			return err
		}
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
	informationSchemaTableName := "main.preen_information_schema"
	Debug(fmt.Sprintf("Creating table %s", informationSchemaTableName))
	err := ddbExec(fmt.Sprintf("create or replace table %s (%s)", informationSchemaTableName, strings.Join(informationSchemaColumnNames, ", ")))
	if err != nil {
		return err
	}

	return nil
}
