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
	Contexts   []string   `yaml:"contexts"`
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
