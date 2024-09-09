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
	sc.Env, err = EnvInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing environment: %w", err)
	}

	configFilePath := filepath.Join(sc.Env.HyphaConfigPath, "sources.yaml")
	file, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read source config file: %s", err)
	}

	if err = yaml.Unmarshal(file, &sc); err != nil {
		return nil, fmt.Errorf("failed to parse source file: %w", err)
	}

	// Override config with environment variables
	fromEnv(&sc)

	return &sc, nil
}
