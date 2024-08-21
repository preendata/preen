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

// BuildColumnMetadata does 2 things:
// 1) Acts as the interface between information schema data stored in DuckDB and the parts of the application that will
// need to consume that data, in particular the model builder
// 2) Performs type validation against each column pulled from the source databases, via the Boyer-Moore majority voting
// algorithm. This majority type is then packaged into the ColumnMetadata and return to the caller. This is important
// for typing the model tables created in DuckDB
func BuildColumnMetadata(cfg *config.Config) (ColumnMetadata, error) {
	// query data from hypha_information_schema
	results, err := Execute("SELECT column_name, data_type, table_name FROM hypha_information_schema", cfg)
	if err != nil {
		return nil, err
	}

	columnMetadata := buildColumnMetadataDataStructure(results.Rows)

	// For each column in each table as sourced from InformationSchema, determine the majority type
	for tableName, tableStruct := range columnMetadata {
		for columnName, columnStruct := range tableStruct {
			majorityType, err := majority(columnName, columnStruct.Types)
			if err != nil {
				return nil, err
			}
			columnMetadata[tableName][columnName] = ColumnType{
				Types:        columnStruct.Types,
				MajorityType: majorityType,
			}
		}

	}

	return columnMetadata, nil
}

// Rearranges the result set from the information schema to make it easier to process for the majority type calculator
func buildColumnMetadataDataStructure(rows []map[string]any) ColumnMetadata {
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

func majority(columnName ColumnName, types []string) (MajorityType, error) {
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
		return majority, nil

	} else if count > len(types)/2 && count != len(types) {
		utils.Warn(
			fmt.Sprintf("Discrepancy in data types for column '%s'! Using majority data type of %s", columnName, majority),
		)
		return majority, nil
	}

	utils.Warn(
		fmt.Sprintf("No majority data type found for column '%s'!", columnName),
	)
	// This needs to be made unreachable
	return "unknown", fmt.Errorf("no majority data type found for column '%s'", columnName)
}
