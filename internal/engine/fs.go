package engine

import (
	"os"
	"path/filepath"
)

// getYmlorYamlPath returns the path to the sources.yml or sources.yaml file.
func getYmlorYamlPath(path string, fileName string) string {
	ymlFile := filepath.Join(path, fileName+".yml")
	yamlFile := filepath.Join(path, fileName+".yaml")

	if _, err := os.Stat(ymlFile); err == nil {
		return ymlFile
	}

	// Default return yaml, up to handlers to create if not exists
	return yamlFile

}
