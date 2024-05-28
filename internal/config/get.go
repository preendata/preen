package config

import (
	"log"
	"os"

	yaml "gopkg.in/yaml.v3"
)

func GetConfig(filenames ...string) Config {
	filename := "plex.yaml"
	if len(filenames) > 0 {
		filename = filenames[0]
	}

	file, err := os.ReadFile(filename)

	if err != nil {
		log.Fatalf("Failed to read config file: %s", err)
	}

	c := Config{}

	err = yaml.Unmarshal(file, &c)

	if err != nil {
		log.Fatalf("Failed to parse config file: %s", err)
	}

	fromEnv(&c)

	return c
}
