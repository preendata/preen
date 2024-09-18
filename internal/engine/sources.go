package engine

import (
	"fmt"
	"os"
	"path/filepath"

	yaml "gopkg.in/yaml.v3"
)

type Connection struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Database   string `yaml:"database"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	AuthSource string `yaml:"auth_source"`
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
	_, err = os.Stat(sc.Env.HyphaConfigPath)

	if os.IsNotExist(err) {
		err = os.Mkdir(sc.Env.HyphaConfigPath, os.ModePerm)

		if err != nil {
			return nil, fmt.Errorf("failed to create directory at %s with error %s", sc.Env.HyphaConfigPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to access %s with error %s", sc.Env.HyphaConfigPath, err)
	}

	configFilePath := filepath.Join(sc.Env.HyphaConfigPath, "sources.yaml")

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
