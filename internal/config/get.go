package config

import (
	"fmt"
	"os"

	yaml "gopkg.in/yaml.v3"
)

func GetConfig() (Config, error) {
	file, err := os.ReadFile(SingleConfigPath)

	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %s", err)
	}

	c := Config{}

	err = yaml.Unmarshal(file, &c)

	if err != nil {
		return Config{}, fmt.Errorf("failed to parse config file: %w", err)
	}

	fromEnv(&c)

	return c, nil
}
