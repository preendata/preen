package config

import (
	"os"

	"github.com/hyphadb/hyphadb/pkg/hlog"
	yaml "gopkg.in/yaml.v3"
)

func GetConfig() Config {
	file, err := os.ReadFile(SingleConfigPath)

	if err != nil {
		hlog.Fatalf("Failed to read config file: %s", err)
	}

	c := Config{}

	err = yaml.Unmarshal(file, &c)

	if err != nil {
		hlog.Fatalf("Failed to parse config file: %s", err)
	}

	fromEnv(&c)

	return c
}
