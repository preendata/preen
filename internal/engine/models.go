package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/hyphasql/sqlparser"
	yaml "gopkg.in/yaml.v3"
)

type ModelName string

type Model struct {
	Name      ModelName `yaml:"name"`
	Type      string    `yaml:"type"`
	Query     string    `yaml:"query"`
	Parsed    sqlparser.Statement
	DDLString string
	Columns   map[TableName]map[ColumnName]Column
	TableMap  TableMap
	TableSet  TableSet
}

type ModelConfig struct {
	Models []*Model `yaml:"models"`
	Env    *Env     `yaml:"-"`
}

// Models can be defined in a models.yaml file in the hypha config directory.
// Models can also be defined in individual .yaml files in the hypha models directory.

func GetModelConfigs() (*ModelConfig, error) {
	mc := ModelConfig{}
	mc.Env, err = EnvInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing environment: %w", err)
	}

	configFilePath := filepath.Join(mc.Env.HyphaConfigPath, "models.yaml")
	modelsDir := mc.Env.HyphaModelsPath

	// Check if a models.yaml file exists in the config directory.
	// If it does, parse it.
	if _, err = os.Stat(configFilePath); err == nil {
		err = parseModelsYamlFile(configFilePath, &mc)
		if err != nil {
			return nil, fmt.Errorf("error parsing models.yaml file: %w", err)
		}
	}

	// Process any .yaml files in the models directory
	err = parseModelDirectoryFiles(modelsDir, &mc)
	if err != nil {
		return nil, fmt.Errorf("error parsing models directory: %w", err)
	}

	// If no models are detected, return an error
	if len(mc.Models) == 0 {
		return nil, fmt.Errorf(
			"no models detected in %s/models.yaml file or %s directory",
			mc.Env.HyphaConfigPath, mc.Env.HyphaModelsPath,
		)
	}

	// Override config with environment variables
	fromEnv(&mc)
	if err = parseSQLModels(&mc); err != nil {
		return nil, fmt.Errorf("error parsing sql models: %w", err)
	}

	if err = ParseModelTables(&mc); err != nil {
		return nil, fmt.Errorf("error parsing model tables: %w", err)
	}

	return &mc, nil
}

// This is the main entry point for building models. The CLI commands call this function.
func BuildModels(sc *SourceConfig, mc *ModelConfig) error {
	if err := BuildInformationSchema(sc, mc); err != nil {
		return fmt.Errorf("error building information schema: %w", err)
	}

	columnMetadata, err := BuildColumnMetadata()
	if err != nil {
		return fmt.Errorf("error building column metadata: %w", err)
	}

	if err = ParseModelColumns(mc, columnMetadata); err != nil {
		return fmt.Errorf("error parsing model columns: %w", err)
	}

	if err = buildDuckDBTables(mc); err != nil {
		return fmt.Errorf("error building model tables: %w", err)
	}

	Info(fmt.Sprintf("Fetching data from %d configured sources", len(sc.Sources)))
	if err = Retrieve(sc, mc); err != nil {
		return fmt.Errorf("error retrieving data: %w", err)
	}

	return nil
}

// Parse the models.yaml file in the hypha config directory. This file can contain multiple models.
// It is optional, but if it exists, it will be parsed.
func parseModelsYamlFile(filePath string, mc *ModelConfig) error {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read model file: %w", err)
	}

	if err = yaml.Unmarshal(file, &mc); err != nil {
		return fmt.Errorf("failed to parse model file: %w", err)
	}

	return nil
}

// Parse the models directory which is supplied as a possible environment value.
// Each .yaml file in this directory is a model.
func parseModelDirectoryFiles(modelsDir string, mc *ModelConfig) error {
	files, err := os.ReadDir(modelsDir)
	if err != nil {
		return fmt.Errorf("failed to read models directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(modelsDir, file.Name())
		if strings.HasSuffix(filePath, ".yaml") {
			file, err := os.ReadFile(filePath)
			m := Model{}
			err = yaml.Unmarshal(file, &m)
			if err != nil {
				return fmt.Errorf("error parsing model file %s: %w", filePath, err)
			}
			if m.Name != "" {
				fmt.Println(m.Name)
				mc.Models = append(mc.Models, &m)
			} else {
				Warn(fmt.Sprintf("Unrecognized model file %s: no model name detected", filePath))
			}
		}
	}

	return nil
}

func ValidateConfigs(sc *SourceConfig, mc *ModelConfig) error {
	if err = errorOnMissingModels(sc, mc); err != nil {
		return fmt.Errorf("error on missing models: %w", err)
	}

	if err = parseSQLModels(mc); err != nil {
		return fmt.Errorf("error parsing sql models: %w", err)
	}

	return nil
}

func parseSQLModels(mc *ModelConfig) error {
	for modelName, model := range mc.Models {
		if model.Type == "sql" {
			stmt, err := sqlparser.Parse(model.Query)
			if err != nil {
				return fmt.Errorf("error parsing sql model %v: %w", modelName, err)
			}
			model.Parsed = stmt
			mc.Models[modelName] = model
		}
	}
	return nil
}

// Create each model's destination table in DuckDB
func buildDuckDBTables(mc *ModelConfig) error {
	for _, model := range mc.Models {
		Debug(fmt.Sprintf("Creating table %s", model.Name))
		tableName := strings.ReplaceAll(string(model.Name), "-", "_")
		createTableStmt := fmt.Sprintf("create or replace table main.%s (%s);", tableName, model.DDLString)
		err = ddbDmlQuery(createTableStmt)
		if err != nil {
			Debug(fmt.Sprintf("Error creating table %s: %s", tableName, createTableStmt))
			return err
		}
	}
	return nil
}

func errorOnMissingModels(sc *SourceConfig, mc *ModelConfig) error {
	missingModels := make([]string, 0)
	for _, source := range sc.Sources {
		for _, modelName := range source.Models {
			modelFound := false
			for _, model := range mc.Models {
				if model.Name == ModelName(modelName) {
					modelFound = true
					break
				}
			}
			if !modelFound && !slices.Contains(missingModels, string(modelName)) {
				missingModels = append(missingModels, string(modelName))
			}
		}
	}
	if len(missingModels) > 0 {
		return fmt.Errorf("no model file detected for models: %s", strings.Join(missingModels, ", "))
	}
	return nil
}
