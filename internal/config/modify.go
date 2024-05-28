package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func AddSource(filename string, newSource Source) error {
	// Read the YAML file
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
