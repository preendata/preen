package engine

import (
	goContext "context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/hyphadb/hyphadb/internal/utils"
	goddb "github.com/marcboeker/go-duckdb"
	"golang.org/x/sync/errgroup"
)

type InformationSchema struct {
	db        *sql.DB
	connector driver.Connector
	appender  *goddb.Appender
}

func BuildInformationSchema(cfg *config.Config) error {
	infoSchema := InformationSchema{}
	err := infoSchema.openDDBConnection()
	if err != nil {
		return err
	}

	defer infoSchema.db.Close()

	// Ensure info schema table exists
	if err = infoSchema.prepareDDBInformationSchema(); err != nil {
		return err
	}

	appender, err := duckdb.NewAppender(infoSchema.connector, "main", "hypha_information_schema")
	if err != nil {
		return err
	}
	infoSchema.appender = appender
	defer infoSchema.appender.Close()

	// Group sources by engine to distribute across specific engine handlers
	hyphaSourcesByEngine := groupSourceByEngine(cfg)

	g := new(errgroup.Group)

	for engine, sources := range hyphaSourcesByEngine {
		engine := engine
		sources := sources
		g.Go(func() error {
			switch engine {
			case "postgres":
				err := infoSchema.buildPostgresInformationSchema(sources)
				if err != nil {
					return err
				}
			case "mysql":
				// buildMySQLInformationSchema(sources)
			case "mongodb":
				utils.Debug("No information schema required for MongoDB")
			default:
				return fmt.Errorf("unsupported engine: %s", engine)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// buildPostgresInformationSchema builds the information schema for all postgres sources in the config
func (is *InformationSchema) buildPostgresInformationSchema(sources []config.Source) error {
	for _, source := range sources {
		// Open new pool for every source
		pool, err := pg.PoolFromSource(source)
		if err != nil {
			return err
		}

		defer pool.Close()

		// Run through all contexts in the source, inserting its information schema into the local hyphaContext in raw form
		for _, context := range source.Contexts {
			table := context
			schema := "public"

			query := fmt.Sprintf(`
				select column_name, data_type from information_schema.columns
				where table_schema = '%s' and table_name = '%s';
			`, schema, table)

			rows, err := pool.Query(goContext.Background(), query)
			if err != nil {
				return err
			}

			defer rows.Close()

			for rows.Next() {
				values, err := rows.Values()
				if err != nil {
					return err
				}

				// append rows into duck db appender
				err = is.appender.AppendRow([]driver.Value{source.Name, table, values[0], values[1]}...)
				if err != nil {
					fmt.Println(err)
					return err
				}
			}
		}
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

// openDDBConnection is a local duckDB conn creator. Eventually should be offloaded to something generic
func (is *InformationSchema) openDDBConnection() error {
	connector, err := duckdb.CreateConnector()
	if err != nil {
		return err
	}
	is.connector = connector

	db, err := duckdb.OpenDatabase(connector)
	if err != nil {
		return err
	}
	is.db = db

	return nil
}

// prepareDDBInformationSchema creates the table for the information schema in duckDB
func (is *InformationSchema) prepareDDBInformationSchema() error {
	informationSchemaColumnNames := []string{"source_name varchar", "table_name varchar", "column_name varchar", "data_type varchar"}
	informationSchemaTableName := "main.hypha_information_schema"
	utils.Debug(fmt.Sprintf("Creating table %s", informationSchemaTableName))
	_, err = is.db.Exec(fmt.Sprintf("create or replace table %s (%s)", informationSchemaTableName, strings.Join(informationSchemaColumnNames, ", ")))
	if err != nil {
		return err
	}

	return nil
}
