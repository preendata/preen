package engine

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
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

func WriteToTable(rows []map[string]any, columns []string, outputFormat string) error {
	// Set up
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)

	// Set table headers. This is fucked, non-deterministic order of fields.
	headers := table.Row{}
	for _, header := range columns {
		headers = append(headers, header)
	}
	t.AppendHeader(headers)

	// Populate table with data
	for _, row := range rows {
		values := table.Row{}
		for _, header := range headers {
			values = append(values, row[header.(string)])
		}
		t.AppendRow(values)
	}

	switch outputFormat {
	case "csv":
		t.RenderCSV()
	case "markdown":
		t.RenderMarkdown()
	default:
		t.Render()
	}

	return nil
}
