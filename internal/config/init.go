package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"

	"github.com/hyphadb/hyphadb/pkg/hlog"
)

var envRegex = regexp.MustCompile(`^\${(\w+)}$`)

var GlobalConfig Config
var SingleConfigPath string

func Initialize() {
	configDirectoryPath := os.Getenv("HYPHADB_CONFIG_PATH")

	// Default to home directory if none set by user
	// Get the current user's home directory
	usr, err := user.Current()
	if err != nil {
		hlog.Fatalf("failed to get current user: %v", err)
	}
	if configDirectoryPath == "" {
		configDirectoryPath = filepath.Join(usr.HomeDir, ".hyphadb") // fallback to default path
	}

	hlog.Debugf("Ensuring configuration exists: %s", configDirectoryPath)
	err = ensureConfigExists(configDirectoryPath)

	if err != nil {
		hlog.Fatalf("error ensuring configuration exists: %v", err)
	}

	SingleConfigPath = filepath.Join(configDirectoryPath, "config.yaml")

	GlobalConfig = GetConfig()

}

func ensureConfigExists(configDirectoryPath string) error {
	hlog.Debugf("Checking if directory exists: %s", configDirectoryPath)

	// If no directory, create
	if _, err := os.Stat(configDirectoryPath); os.IsNotExist(err) {
		hlog.Debugf("Config directory does not exist, creating it: %s", configDirectoryPath)
		err = os.MkdirAll(configDirectoryPath, 0755)

		if err != nil {
			return fmt.Errorf("failed to create config directory: %s", err)
		}
	} else if err != nil {
		return fmt.Errorf("error checking directory: %s", err)
	} else {
		hlog.Debugf("Directory already exists: %s", configDirectoryPath)
	}

	// If no database config, create
	// TODO

	// If no DDL config, create
	// TODO

	return nil
}
