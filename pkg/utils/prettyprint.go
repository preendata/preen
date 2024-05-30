package utils

import (
	"encoding/json"
	"fmt"
)

// PrettyPrintJSON pretty prints a slice of maps containing JSON objects.
func prettifyString(data []map[string]interface{}) (string, error) {
	prettyJSON, err := json.MarshalIndent(data, "", "    ")
	if err != nil {
		return "", err
	}
	return string(prettyJSON), nil
}

// PrintPrettyJSON prints the pretty JSON to the console.
func PrintPrettyJSON(data []map[string]interface{}) error {
	prettyJSON, err := prettifyString(data)
	if err != nil {
		return err
	}
	fmt.Println(prettyJSON)
	return nil
}

func prettifyStruct(v interface{}) (string, error) {
	// Marshal the struct with indentation
	prettyJSON, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal struct: %w", err)
	}
	return string(prettyJSON), nil
}

func PrintPrettyStruct(v interface{}) error {
	prettyJSON, err := prettifyStruct(v)
	if err != nil {
		return fmt.Errorf("failed to pretty print struct: %w", err)
	}
	fmt.Println(prettyJSON)
	return nil
}
