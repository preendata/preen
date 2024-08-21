package engine

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/duckdb"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/xwb1989/sqlparser"
)

type ModelQuery struct {
	Query     string
	Parsed    sqlparser.Statement
	DDLString string
	Columns   map[string]map[string]Column
	IsSql     bool
}

type Model struct {
	ModelQueries map[string]ModelQuery
}

func BuildModel(cfg *config.Config) error {
	err := BuildInformationSchema(cfg)

	if err != nil {
		return fmt.Errorf("error building information schema: %w", err)
	}

	columnMetadata, err := BuildColumnMetadata(cfg)
	if err != nil {
		return fmt.Errorf("error building column metadata: %w", err)
	}

	model := Model{}

	utils.Debug("Building model")
	model.ModelQueries, err = readModelFiles(cfg)
	if err != nil {
		return fmt.Errorf("error reading model files: %w", err)
	}

	model.ModelQueries, err = ParseModelColumns(model.ModelQueries, columnMetadata)
	if err != nil {
		return fmt.Errorf("error parsing columns: %w", err)
	}

	if err = buildTables(model.ModelQueries); err != nil {
		return fmt.Errorf("error building model tables: %w", err)
	}

	utils.Info(fmt.Sprintf("Fetching data from %d configured sources", len(cfg.Sources)))
	if err = Retrieve(cfg, model); err != nil {
		return fmt.Errorf("error retrieving data: %w", err)
	}

	return nil
}

func readModelFiles(cfg *config.Config) (map[string]ModelQuery, error) {
	ModelQueries := make(map[string]ModelQuery, 0)
	files, err := os.ReadDir(cfg.Env.HyphaModelPath)
	if err != nil {
		return nil, err
	}

	modelFileCount := 0

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".sql") {
			modelFileCount++
			modelName := strings.TrimSuffix(file.Name(), ".sql")
			if !slices.Contains(cfg.Models, modelName) {
				utils.Debug(fmt.Sprintf("Skipping file %s", modelName))
				continue
			}
			utils.Debug("Loading ", file.Name())
			bytes, err := os.ReadFile(cfg.Env.HyphaModelPath + "/" + file.Name())
			if err != nil {
				return nil, err
			}
			cq := ModelQuery{
				Query: string(bytes),
				IsSql: true,
			}
			utils.Debug(fmt.Sprintf("Parsing query: %s", cq.Query))
			parsedQuery, err := sqlparser.Parse(cq.Query)
			if err != nil {
				return nil, err
			}
			cq.Parsed = parsedQuery
			ModelQueries[strings.TrimSuffix(file.Name(), ".sql")] = cq
		} else if strings.HasSuffix(file.Name(), ".json") {
			modelFileCount++
			modelName := strings.TrimSuffix(file.Name(), ".json")
			if !slices.Contains(cfg.Models, modelName) {
				utils.Debug(fmt.Sprintf("Skipping file %s", modelName))
				continue
			}
			utils.Debug("Loading ", file.Name())
			bytes, err := os.ReadFile(cfg.Env.HyphaModelPath + "/" + file.Name())
			if err != nil {
				return nil, err
			}
			cq := ModelQuery{
				Query: string(bytes),
				IsSql: false,
			}
			ModelQueries[strings.TrimSuffix(file.Name(), ".json")] = cq
		}
	}

	if modelFileCount == 0 {
		return nil, fmt.Errorf("no model files found in %s", cfg.Env.HyphaModelPath)
	}

	utils.Debug(fmt.Sprintf("Loaded %d model files from %s", modelFileCount, cfg.Env.HyphaModelPath))

	return ModelQueries, nil
}

func buildTables(ModelQueries map[string]ModelQuery) error {
	connector, err := duckdb.CreateConnector()
	if err != nil {
		return err
	}

	db, err := duckdb.OpenDatabase(connector)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for modelName, ModelQuery := range ModelQueries {
		utils.Debug(fmt.Sprintf("Creating table %s", modelName))
		dropTable := fmt.Sprintf("drop table if exists main.%s;", modelName)
		_, err := db.Exec(dropTable)
		if err != nil {
			return err
		}
		createTable := fmt.Sprintf("create table main.%s (%s);", modelName, ModelQuery.DDLString)
		_, err = db.Exec(createTable)
		if err != nil {
			utils.Debug(fmt.Sprintf("Error creating table %s: %s", modelName, createTable))
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}
