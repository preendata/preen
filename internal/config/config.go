package config

import (
	"fmt"
	"os"
	"path/filepath"

	yaml "gopkg.in/yaml.v3"
)

type Context struct {
	Name string
}

type Connection struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

type Source struct {
	Name       string
	Engine     string
	Connection Connection
	Contexts   []string
}

type Config struct {
	Sources  []Source `yaml:"sources"`
	Env      *Env     `yaml:"-"`
	Contexts []string `yaml:"contexts"`
}

var err error

func GetConfig() (*Config, error) {
	c := Config{}
	c.Env, err = EnvInit()
	if err != nil {
		return nil, fmt.Errorf("error initializing environment: %w", err)
	}

	err = validateLicenseKey(c.Env.LicenseKey)
	if err != nil {
		return nil, fmt.Errorf("error validating license key: %w", err)
	}

	configFilePath := filepath.Join(c.Env.HyphaConfigPath, "config.yaml")
	file, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %s", err)
	}

	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override config with environment variables
	fromEnv(&c)

	return &c, nil
}
