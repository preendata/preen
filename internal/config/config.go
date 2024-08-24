package config

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

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

type Config struct {
	Sources []Source `yaml:"sources"`
	Env     *Env     `yaml:"-"`
	Models  []string `yaml:"-"`
}

var err error

func GetConfig() (*Config, error) {
	c := Config{}
	c.Env, err = EnvInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing environment: %w", err)
	}

	configFilePath := filepath.Join(c.Env.HyphaConfigPath, "config.yaml")
	file, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %s", err)
	}

	if err = yaml.Unmarshal(file, &c); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override config with environment variables
	fromEnv(&c)

	collectModels(&c)

	if len(c.Models) == 0 {
		return nil, fmt.Errorf("no models defined in config file")
	}

	return &c, nil
}

func collectModels(config *Config) {
	for _, source := range config.Sources {
		for _, model := range source.Models {
			if !slices.Contains(config.Models, model) {
				config.Models = append(config.Models, model)
			}
		}
	}
}
