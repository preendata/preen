package config

import (
	"fmt"
	"os"

	yaml "gopkg.in/yaml.v3"
)

func GetConfig() (Config, error) {
	licenseKey, ok := os.LookupEnv("HYPHADB_LICENSE_KEY")
	if !ok {
		return Config{}, fmt.Errorf("HYPHADB_LICENSE_KEY environment variable not set")
	}

	err := validateLicenseKey(licenseKey)
	if err != nil {
		return Config{}, fmt.Errorf("failed to validate license key: %w", err)
	}

	file, err := os.ReadFile(SingleConfigPath)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %s", err)
	}

	c := Config{}

	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return Config{}, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Override config with environment variables
	fromEnv(&c)

	return c, nil
}
