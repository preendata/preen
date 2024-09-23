package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/hyphasql/sqlparser"
	yaml "gopkg.in/yaml.v3"
)

type ModelName string

type Type struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

type Options struct {
	AllVarchar         *bool     `default:"false" yaml:"all_varchar"`
	AllowQuotedNulls   *bool     `default:"true" yaml:"allow_quoted_nulls"`
	AutoDetect         *bool     `default:"true" yaml:"auto_detect"`
	AutoTypeCandidates *[]string `default:"-" yaml:"auto_type_candidates"`
	Columns            *[]Type   `default:"-" yaml:"columns"`
	Compression        *string   `default:"auto" yaml:"compression"`
	DateFormat         *string   `default:"-" yaml:"date_format"`
	DecimalSeparator   *string   `default:"." yaml:"decimal_separator"`
	Delim              *string   `default:"," yaml:"delim"`
	Escape             *string   `default:"\"" yaml:"escape"`
	FileName           *bool     `default:"false" yaml:"filename"`
	ForceNotNull       *[]string `default:"[]" yaml:"force_not_null"`
	Header             *bool     `default:"false" yaml:"header"`
	HivePartitioning   *bool     `default:"false" yaml:"hive_partitioning"`
	IgnoreErrors       *bool     `default:"false" yaml:"ignore_errors"`
	MaxLineSize        *int64    `default:"2097152" yaml:"max_line_size"`
	Names              *[]string `default:"-" yaml:"names"`
	NewLine            *string   `default:"-" yaml:"new_line"`
	NormalizeNames     *bool     `default:"false" yaml:"normalize_names"`
	NullPadding        *bool     `default:"false" yaml:"null_padding"`
	NullString         *[]string `default:"-" yaml:"null_string"`
	Parallel           *bool     `default:"true" yaml:"parallel"`
	Quote              *string   `default:"\"" yaml:"quote"`
	SampleSize         *int64    `default:"20480" yaml:"sample_size"`
	Skip               *int64    `default:"0" yaml:"skip"`
	TimestampFormat    *string   `default:"-" yaml:"timestamp_format"`
	Types              *[]Type   `default:"-" yaml:"types"`
	UnionByName        *bool     `default:"false" yaml:"union_by_name"`
}

type Model struct {
	Name         ModelName `yaml:"name"`
	Type         string    `yaml:"type"`
	Format       string    `yaml:"format"`
	Options      Options   `yaml:"options"`
	Query        string    `yaml:"query"`
	FilePatterns *[]string `yaml:"file_patterns"`
	Parsed       sqlparser.Statement
	DDLString    string
	Columns      map[TableName]map[ColumnName]Column
	TableMap     TableMap
	TableSet     TableSet
}

type ModelConfig struct {
	Models []*Model `yaml:"models"`
	Env    *Env     `yaml:"-"`
}

// Models can be defined in a models.yaml file in the hypha config directory.
// Models can also be defined in individual .yaml files in the hypha models directory.

func GetModelConfigs(modelTarget string) (*ModelConfig, error) {
	mc := ModelConfig{}
	env, err := EnvInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing environment: %w", err)
	}
	mc.Env = env

	configFilePath := filepath.Join(mc.Env.HyphaConfigPath, "models.yaml")
	modelsDir := mc.Env.HyphaModelsPath

	// Check if a models.yaml file exists in the config directory.
	// If it does, parse it.

	if _, err = os.Stat(configFilePath); err == nil {
		err = parseModelsYamlFile(configFilePath, &mc)
		if err != nil {
			return nil, fmt.Errorf("error parsing models.yaml file: %w", err)
		}
	} else if os.IsNotExist(err) {
		_, err = os.Create(configFilePath)

		if err != nil {
			return nil, fmt.Errorf("error creating models.yaml file at %s with error %s", configFilePath, err)
		}

		return nil, errors.New(fmt.Sprintf("created empty models.yaml file at %s. Please configure valid models before proceeding", configFilePath))
	} else if err != nil {
		return nil, fmt.Errorf("error reading models.yaml file at %s with error %s", configFilePath, err)
	}

	// Process any .yaml files in the models directory
	err = parseModelDirectoryFiles(modelsDir, modelTarget, &mc)
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
	if err = parseModels(&mc); err != nil {
		return nil, fmt.Errorf("error parsing sql models: %w", err)
	}

	if err = ParseModelTables(&mc); err != nil {
		return nil, fmt.Errorf("error parsing model tables: %w", err)
	}

	return &mc, nil
}

// This is the main entry point for building models. The CLI commands call this function.
func BuildModels(sc *SourceConfig, mc *ModelConfig) error {
	if err := BuildMetadata(sc, mc); err != nil {
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
// The modelTarget is the user input prefix of any model files that should be used.
// Each .yaml file in this directory is a model.
func parseModelDirectoryFiles(modelsDir string, modelTarget string, mc *ModelConfig) error {
	_, err := os.ReadDir(modelsDir)
	if err != nil {
		return fmt.Errorf("failed to read models directory: %w", err)
	}

	err = filepath.WalkDir(modelsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error walking directory: %w", err)
		}

		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".yaml") && (modelTarget == "" || strings.HasPrefix(path, filepath.Join(modelsDir, modelTarget))) {
			file, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("error reading model file %s: %w", path, err)
			}
			m := Model{}
			err = yaml.Unmarshal(file, &m)
			if err != nil {
				return fmt.Errorf("error parsing model file %s: %w", path, err)
			}
			if m.Name != "" {
				mc.Models = append(mc.Models, &m)
			} else {
				Warn(fmt.Sprintf("Unrecognized model file %s: no model name detected", path))
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error parsing model directory: %w", err)
	}
	return nil
}

// Parse the models and create a parsed version of the model's required fields.
// This is where the SQL models are parsed into ASTs.
// This is where the file models are validated.
func parseModels(mc *ModelConfig) error {
	for modelName, model := range mc.Models {
		switch model.Type {
		case "sql":
			stmt, err := sqlparser.Parse(model.Query)
			if err != nil {
				return fmt.Errorf("error parsing sql model %v: %w", modelName, err)
			}
			model.Parsed = stmt
			mc.Models[modelName] = model
		case "file":
			if model.FilePatterns == nil {
				return fmt.Errorf("error parsing file model %v: file_pattern required", modelName)
			}
		}
	}
	return nil
}

// Create each model's destination table in DuckDB
func buildDuckDBTables(mc *ModelConfig) error {
	for _, model := range mc.Models {
		switch model.Type {
		case "sql":
			Debug(fmt.Sprintf("Creating table %s", model.Name))
			tableName := strings.ReplaceAll(string(model.Name), "-", "_")
			createTableStmt := fmt.Sprintf("create or replace table main.%s (%s);", tableName, model.DDLString)
			if err := ddbExec(createTableStmt); err != nil {
				return fmt.Errorf("error creating table %s: %w", tableName, err)
			}
		case "file":
			Debug("Tables for file models will be created on model retrieval")
		}
	}
	return nil
}

// If a model file is referenced in a source, but no model file exists, return an error.
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
