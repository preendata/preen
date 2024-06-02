package config

import (
	"fmt"
	"github.com/hyphadb/hyphadb/internal/hlog"
	"os"

	"gopkg.in/yaml.v3"
)

func AddSource(filename string, newSource Source) error {
	hlog.WithFields(hlog.Fields{
		"filename":  filename,
		"newSource": newSource,
	}).Debug("Executing config.AddSource with arguments")

	// Read the YAML file
	hlog.Debugf("Attempting to read in file %s", filename)
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("could not read file: %v", err)
	}

	// Unmarshal the YAML into a Config struct
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return fmt.Errorf("could not unmarshal YAML: %v", err)
	}

	for _, source := range config.Sources {
		err := validateNewSource(source, newSource)
		if err != nil {
			return err
		}
	}

	// Add the new source
	config.Sources = append(config.Sources, newSource)

	// Marshal the updated config back to YAML
	updatedData, err := yaml.Marshal(&config)
	if err != nil {
		return fmt.Errorf("could not marshal YAML: %v", err)
	}

	// Write the updated YAML back to the file
	err = os.WriteFile(filename, updatedData, 0644)
	if err != nil {
		return fmt.Errorf("could not write file: %v", err)
	}

	return nil
}

func validateNewSource(existingSource Source, newSource Source) error {
	if existingSource.Name == newSource.Name {
		return fmt.Errorf("a database source already exists with the name: %s", newSource.Name)
	}

	if existingSource.Connection.Host == newSource.Connection.Host &&
		existingSource.Connection.Database == newSource.Connection.Database &&
		existingSource.Connection.Port == newSource.Connection.Port {
		return fmt.Errorf("a connection with the same combination of host: %s, port: %d, and database: %s already exists", newSource.Connection.Host, newSource.Connection.Port, newSource.Connection.Database)
	}

	return nil
}
