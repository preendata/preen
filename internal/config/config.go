package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

var logger = logrus.New()
var envRegex = regexp.MustCompile(`^\${(\w+)}$`)

var GlobalConfig Config

// Defining our own initialization function because the logic is complex enough that I need to be able to return errors
func Initialize() error {
	err := godotenv.Load()
	if err != nil {
		logger.Warnf("warn: error loading .env file: %v", err)
	}

	// Initialize logger
	logLevel := os.Getenv("HYPHADB_LOG_LEVEL")

	if logLevel == "" {
		logLevel = "ERROR"
	}

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("invalid log level: %v", err)
	}
	logger.SetLevel(level)
	// End intialize logger

	configDirectoryPath := os.Getenv("HYPHADB_CONFIG_PATH")

	// Default to home directory if none set by user
	// Get the current user's home directory
	usr, err := user.Current()
	if err != nil {
		return fmt.Errorf("failed to get current user: %v", err)
	}
	if configDirectoryPath == "" {
		configDirectoryPath = filepath.Join(usr.HomeDir, ".hyphadb") // fallback to default path
	}

	logger.Debugf("Ensuring configuration exists: %s", configDirectoryPath)
	err = ensureConfigExists(configDirectoryPath)

	if err != nil {
		return fmt.Errorf("error ensuring configuration exists: %v", err)
	}

	configFilePath := filepath.Join(configDirectoryPath, "config.yaml")

	GlobalConfig = GetConfig(configFilePath)

	fmt.Println(configFilePath)

	return nil
}

func ensureConfigExists(configDirectoryPath string) error {
	logger.Debugf("Checking if directory exists: %s", configDirectoryPath)

	// If no directory, create
	if _, err := os.Stat(configDirectoryPath); os.IsNotExist(err) {
		logger.Debugf("Directory does not exist, creating it: %s", configDirectoryPath)
		err = os.MkdirAll(configDirectoryPath, 0755)

		if err != nil {
			return fmt.Errorf("failed to create directory: %s", err)
		}
	} else if err != nil {
		return fmt.Errorf("error checking directory: %s", err)
	} else {
		logger.Debugf("Directory already exists: %s", configDirectoryPath)
	}

	// If no database config, create
	// TODO

	// If no DDL config, create
	// TODO

	return nil
}
