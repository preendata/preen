package config

import (
	"fmt"
	"os"
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

	logLevel := os.Getenv("HYPHADB_LOG_LEVEL")

	if logLevel == "" {
		logLevel = "ERROR"
	}

	// Set the log level
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		return fmt.Errorf("invalid log level: %v", err)
	}
	logger.SetLevel(level)

	configDirectoryPath := os.Getenv("HYPHADB_CONFIG_PATH")

	// Default to home directory if none set by user
	if configDirectoryPath == "" {
		configDirectoryPath = "~/.hyphadb/" // fallback to default path
		//TODO this probably needs to be set in a more flexible way than just hardcoding ~/.
		// Windows? Who cares?
	}

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
	dir := filepath.Dir(configDirectoryPath)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// Create the directory
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory: %s", err)
		}
	}

	return nil
}
