package config

import (
	"fmt"
	"github.com/hyphadb/hyphadb/internal/hlog"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
)

var envRegex = regexp.MustCompile(`^\${(\w+)}$`)

var GlobalConfig Config
var SingleConfigPath string

func Initialize() error {
	configDirectoryPath := os.Getenv("HYPHADB_CONFIG_PATH")

	// Default to home directory if none set by user
	// Get the current user's home directory
	usr, err := user.Current()
	if err != nil {
		return fmt.Errorf("failed to get current user: %w", err)
	}
	if configDirectoryPath == "" {
		configDirectoryPath = filepath.Join(usr.HomeDir, ".hyphadb") // fallback to default path
	}

	hlog.Debugf("Ensuring configuration exists: %s", configDirectoryPath)
	err = ensureConfigExists(configDirectoryPath)

	if err != nil {
		return fmt.Errorf("error ensuring configuration exists: %v", err)
	}

	SingleConfigPath = filepath.Join(configDirectoryPath, "config.yaml")

	GlobalConfig, err = GetConfig()
	if err != nil {
		return fmt.Errorf("error getting config: %w ", err)
	}

	return nil
}

func ensureConfigExists(configDirectoryPath string) error {
	hlog.Debugf("Checking if directory exists: %s", configDirectoryPath)

	// If no directory, create
	if _, err := os.Stat(configDirectoryPath); os.IsNotExist(err) {
		hlog.Debugf("Config directory does not exist, creating it: %s", configDirectoryPath)
		err = os.MkdirAll(configDirectoryPath, 0755)

		if err != nil {
			return fmt.Errorf("failed to create config directory: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("error checking directory: %w", err)
	} else {
		hlog.Debugf("Directory already exists: %s", configDirectoryPath)
	}

	// If no database config, create
	// TODO

	// If no DDL config, create
	// TODO

	return nil
}
