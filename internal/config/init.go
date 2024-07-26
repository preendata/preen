package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"

	"github.com/hyphadb/hyphadb/internal/utils"
	yaml "gopkg.in/yaml.v3"
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

	utils.Debugf("Ensuring configuration exists: %s", configDirectoryPath)
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
	utils.Debugf("Checking if directory exists: %s", configDirectoryPath)

	// If no directory, create
	if _, err := os.Stat(configDirectoryPath); os.IsNotExist(err) {
		utils.Debugf("Config directory does not exist, creating it: %s", configDirectoryPath)
		err = os.MkdirAll(configDirectoryPath, 0755)

		if err != nil {
			return fmt.Errorf("failed to create config directory: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("error checking directory: %w", err)
	} else {
		utils.Debugf("Directory already exists: %s", configDirectoryPath)
	}

	// If no database config, create
	filePath := filepath.Join(configDirectoryPath, "config.yaml")
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		sampleConfig := createSampleConfig()
		err := writeConfigToFile(sampleConfig, filePath)
		if err != nil {
			return fmt.Errorf("failed to create config file: %w", err)
		}

		utils.Debugf("%s created with sample data", filePath)
		fmt.Println("\n===========================================================")
		fmt.Println("A sample configuration file for HyphaDB has been generated for you at:", filePath)
		fmt.Println(">>>>> Please edit this file to match your database configuration. <<<<<")
		fmt.Print("===========================================================\n\n")
	} else if err != nil {
		return fmt.Errorf("error checking config file: %w", err)
	} else {
		utils.Debug("config.yaml already exists.")
	}

	return nil
}

// writeConfigToFile writes a sample config to config.yaml
func writeConfigToFile(config Config, filePath string) error {
	data, err := yaml.Marshal(&config)
	if err != nil {
		return err
	}

	comment := "# This is a sample config file. Modify the values as needed.\n"
	fileContent := comment + string(data)

	err = os.WriteFile(filePath, []byte(fileContent), 0644)
	if err != nil {
		return err
	}

	return nil
}

func createSampleConfig() Config {
	return Config{
		Sources: []Source{
			{
				Name:   "ExampleSource1",
				Engine: "postgresql",
				Connection: Connection{
					Host:     "localhost",
					Port:     5432,
					Database: "exampledb1",
					Username: "exampleuser",
					Password: "examplepassword",
				},
			},
			{
				Name:   "ExampleSource2",
				Engine: "postgresql",
				Connection: Connection{
					Host:     "localhost",
					Port:     5432,
					Database: "exampledb2",
					Username: "exampleuser",
					Password: "examplepassword",
				},
			},
		},
		Tables: []Table{
			{
				Name:    "users",
				Schema:  "public",
				Columns: []string{"user_id", "email"},
			},
			{
				Name:    "transactions",
				Schema:  "public",
				Columns: []string{"user_id", "product_id"},
			},
		},
	}
}
