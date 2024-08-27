package engine

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/hyphasql/hypha/internal/config"
	"github.com/hyphasql/hypha/internal/duckdb"
	"github.com/hyphasql/hypha/internal/utils"
	"github.com/xwb1989/sqlparser"
)

type ModelName string

type ModelConfig struct {
	Query     string
	Parsed    sqlparser.Statement
	DDLString string
	Columns   map[TableName]map[ColumnName]Column
	TableMap  TableMap
	TableSet  TableSet
	IsSql     bool
}

type Models struct {
	Config map[ModelName]*ModelConfig
}

func BuildModel(cfg *config.Config) error {

	models, err := ParseModels(cfg)
	if err != nil {
		return fmt.Errorf("error parsing models: %w", err)
	}

	if err := BuildInformationSchema(cfg, models); err != nil {
		return fmt.Errorf("error building information schema: %w", err)
	}

	columnMetadata, err := BuildColumnMetadata(cfg)
	if err != nil {
		return fmt.Errorf("error building column metadata: %w", err)
	}

	if err = ParseModelColumns(models.Config, columnMetadata); err != nil {
		return fmt.Errorf("error parsing model columns: %w", err)
	}

	if err = buildDuckDBTables(models.Config); err != nil {
		return fmt.Errorf("error building model tables: %w", err)
	}

	utils.Info(fmt.Sprintf("Fetching data from %d configured sources", len(cfg.Sources)))
	if err = Retrieve(cfg, *models); err != nil {
		return fmt.Errorf("error retrieving data: %w", err)
	}

	return nil
}

func ParseModels(cfg *config.Config) (*Models, error) {
	models, err := readModelFiles(cfg)
	if err != nil {
		return nil, err
	}

	if err = ParseModelTables(models); err != nil {
		return nil, err
	}

	return &Models{Config: models}, nil
}

func readModelFiles(cfg *config.Config) (map[ModelName]*ModelConfig, error) {
	ModelQueries := make(map[ModelName]*ModelConfig, 0)
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
			cq := &ModelConfig{
				Query: string(bytes),
				IsSql: true,
			}
			utils.Debug(fmt.Sprintf("Parsing query: %s", cq.Query))
			parsedQuery, err := sqlparser.Parse(cq.Query)
			if err != nil {
				return nil, err
			}
			cq.Parsed = parsedQuery
			ModelQueries[ModelName(strings.TrimSuffix(file.Name(), ".sql"))] = cq
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
			cq := &ModelConfig{
				Query: string(bytes),
				IsSql: false,
			}
			ModelQueries[ModelName(strings.TrimSuffix(file.Name(), ".json"))] = cq
		}
	}

	if modelFileCount == 0 {
		return nil, fmt.Errorf("no model files found in %s", cfg.Env.HyphaModelPath)
	}

	utils.Debug(fmt.Sprintf("Loaded %d model files from %s", modelFileCount, cfg.Env.HyphaModelPath))

	return ModelQueries, nil
}

// Create each model's destination table in DuckDB
func buildDuckDBTables(models map[ModelName]*ModelConfig) error {
	for modelName, modelConfig := range models {
		utils.Debug(fmt.Sprintf("Creating table %s", modelName))

		createTableStmt := fmt.Sprintf("CREATE OR REPLACE table main.%s (%s);", modelName, modelConfig.DDLString)
		err = duckdb.DMLQuery(createTableStmt)
		if err != nil {
			utils.Debug(fmt.Sprintf("Error creating table %s: %s", modelName, createTableStmt))
			return err
		}
	}
	return nil
}
