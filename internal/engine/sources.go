package engine

import (
	"fmt"
	"os"

	yaml "gopkg.in/yaml.v3"
)

type Connection struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Database   string `yaml:"database"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	AuthSource string `yaml:"auth_source"`
	BucketName string `yaml:"bucket_name"`
	Region     string `yaml:"region"`
	Schema     string `yaml:"schema"`
	Warehouse  string `yaml:"warehouse"`
	Role       string `yaml:"role"`
	Account    string `yaml:"account"`
}

type Source struct {
	Name       string     `yaml:"name"`
	Engine     string     `yaml:"engine"`
	Connection Connection `yaml:"connection"`
	Models     []string   `yaml:"models"`
}

type SourceConfig struct {
	Sources []Source `yaml:"sources"`
	Env     *Env     `yaml:"-"` // not in yaml
}

func GetSourceConfig() (*SourceConfig, error) {
	sc := SourceConfig{}
	env, err := EnvInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing environment: %w", err)
	}
	sc.Env = env

	// Create directory if not exists
	_, err = os.Stat(sc.Env.PreenConfigPath)

	if os.IsNotExist(err) {
		err = os.Mkdir(sc.Env.PreenConfigPath, os.ModePerm)

		if err != nil {
			return nil, fmt.Errorf("failed to create directory at %s with error %s", sc.Env.PreenConfigPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to access %s with error %s", sc.Env.PreenConfigPath, err)
	}

	configFilePath := getYmlorYamlPath(sc.Env.PreenConfigPath, "sources")

	// Create file if not exists
	file, err := os.ReadFile(configFilePath)

	if os.IsNotExist(err) {
		_, err = os.Create(configFilePath)

		if err != nil {
			return nil, fmt.Errorf("failed to create file at %s with error %s", configFilePath, err)
		}

		file, err = os.ReadFile(configFilePath)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read source config file: %s", err)
	}

	// Pull yaml out of config file
	if err = yaml.Unmarshal(file, &sc); err != nil {
		return nil, fmt.Errorf("failed to parse source file: %w", err)
	}

	// Override config with environment variables
	fromEnv(&sc)

	return &sc, nil
}
