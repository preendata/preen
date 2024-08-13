package engine

import (
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
)

type ColumnType struct {
	Types        []string `json:"types"`
	MajorityType string   `json:"majority_type"`
}

type ColumnMetadata struct {
	infoSchemaResults []map[string]any
	ColumnTypes       map[string]map[string]ColumnType `json:"column_types"`
}

func BuildColumnMetadata(cfg *config.Config) error {
	// query data from hypha_information_schema
	results, err := Execute("SELECT * FROM hypha_information_schema", cfg)
	if err != nil {
		return err
	}
	columnMetadata := ColumnMetadata{
		infoSchemaResults: results.Rows,
		ColumnTypes:       make(map[string]map[string]ColumnType),
	}

	columnMetadata.ParseColumnMetadata()
	for _, row := range results.Rows {
		utils.Debug(fmt.Sprintf("Row: %v", row))
	}

	return nil
}

func (c *ColumnMetadata) ParseColumnMetadata() {
	for _, row := range c.infoSchemaResults {
		fmt.Println(row)
	}
}

func (c *ColumnMetadata) majority(tableName string, columnName string, types []string) {
	// Implement Boyer-Moore majority vote algorithm
	var majority string
	votes := 0

	for _, candidate := range types {
		if votes == 0 {
			majority = candidate
		}
		if candidate == majority {
			votes++
		} else {
			votes--
		}
	}

	count := 0

	// Checking if majority candidate occurs more than n/2 times
	for _, candidate := range types {
		if candidate == majority {
			count += 1
		}
	}
	if majority == "" {
		utils.Warn(
			fmt.Sprintf("Column: '%s' is missing from majority of tables!", columnName),
		)
	} else if count > len(types)/2 && count == len(types) {
		utils.Debug(
			fmt.Sprintf("Data type for column '%s' is: %s", columnName, majority),
		)
		if entry, ok := c.ColumnTypes[tableName][columnName]; ok {
			entry.MajorityType = majority
			c.ColumnTypes[tableName][columnName] = entry
		}
	} else if count > len(types)/2 && count != len(types) {
		utils.Warn(
			fmt.Sprintf("Discrepancy in data types for column '%s'! Using majority data type of %s", columnName, majority),
		)
		if entry, ok := c.ColumnTypes[tableName][columnName]; ok {
			entry.MajorityType = majority
			c.ColumnTypes[tableName][columnName] = entry
		}
	} else {
		utils.Warn(
			fmt.Sprintf("No majority data type found for column '%s'!", columnName),
		)
	}
}
