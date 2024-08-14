package engine

import (
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
)

type TableName string
type ColumnName string
type MajorityType string
type ColumnType struct {
	// Types is a slice of every data type found for a column from its sources
	Types        []string     `json:"types"`
	MajorityType MajorityType `json:"majority_type"`
}

type ColumnMetadata map[TableName]map[ColumnName]ColumnType

func BuildColumnMetadata(cfg *config.Config) (ColumnMetadata, error) {
	// query data from hypha_information_schema
	results, err := Execute("SELECT column_name, data_type, table_name FROM hypha_information_schema", cfg)
	if err != nil {
		return nil, err
	}

	columnMetadata := parseColumnMetadata(results.Rows)

	for tableName, tableStruct := range columnMetadata {
		for columnName, columnStruct := range tableStruct {
			majorityType := majority(columnName, columnStruct.Types)
			columnMetadata[tableName][columnName] = ColumnType{
				Types:        columnStruct.Types,
				MajorityType: majorityType,
			}
		}

	}

	return columnMetadata, nil
}

// Rearranges the result set from the information schema to make it easier to process for the majority type calculator
func parseColumnMetadata(rows []map[string]any) ColumnMetadata {
	columnMetadata := make(ColumnMetadata)

	for _, row := range rows {

		// Runtime panic waiting to happen. This depends on the information schema being built correctly and only with
		// type string
		tableName := TableName(row["table_name"].(string))
		columnName := ColumnName(row["column_name"].(string))
		dataType := (row["data_type"].(string))
		// Create table map if not exists
		_, exists := columnMetadata[tableName]
		if !exists {
			columnMetadata[tableName] = make(map[ColumnName]ColumnType)
		}

		// Create column map if not exists
		_, exists = columnMetadata[tableName][columnName]
		if !exists {
			columnMetadata[tableName][columnName] = ColumnType{
				Types: make([]string, 0),
			}
		}

		// Append data type to column map
		localTypesCopy := append(columnMetadata[tableName][columnName].Types, dataType)
		columnMetadata[tableName][columnName] = ColumnType{
			Types: localTypesCopy,
		}

	}

	return columnMetadata
}

func majority(columnName ColumnName, types []string) MajorityType {
	// Implement Boyer-Moore majority vote algorithm
	var majority MajorityType
	votes := 0

	for _, candidate := range types {
		mtCandidate := MajorityType(candidate)
		if votes == 0 {
			majority = mtCandidate
		}
		if mtCandidate == majority {
			votes++
		} else {
			votes--
		}
	}

	count := 0

	// Checking if majority candidate occurs more than n/2 times
	for _, candidate := range types {
		if MajorityType(candidate) == majority {
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
		return majority

	} else if count > len(types)/2 && count != len(types) {
		utils.Warn(
			fmt.Sprintf("Discrepancy in data types for column '%s'! Using majority data type of %s", columnName, majority),
		)
		return majority
	} else {
		utils.Warn(
			fmt.Sprintf("No majority data type found for column '%s'!", columnName),
		)
	}

	// This needs to be made unreachable
	return majority
}