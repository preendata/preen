package pg

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/scalecraft/plex-db/pkg/config"
)

func Validate(cfg *config.Config) config.Validator {
	var v config.Validator
	v.Cfg = *cfg
	v.Databases = make(map[string]config.Database)
	v.ColumnTypes = make(map[string]map[string]config.ColumnType)

	for sourceIdx, source := range v.Cfg.Sources {
		v.Databases[source.Name] = config.Database{
			Url: fmt.Sprintf(
				"postgres://%s:%s@%s:%d/%s",
				source.Connection.Username,
				source.Connection.Password,
				source.Connection.Host,
				source.Connection.Port,
				source.Connection.Database,
			),
			TableResults: make(map[string]config.TableResult),
		}
		getDataTypes(v, sourceIdx, v.Databases[source.Name])
	}

	for table, columns := range v.ColumnTypes {
		for column, types := range columns {
			majority(v, table, column, types.Types)
		}
	}
	return v
}

func getDataTypes(v config.Validator, sourceIdx int, d config.Database) {
	conn := connect(d.Url)
	defer conn.Close(context.Background())

	query := `
		select column_name, data_type from information_schema.columns
		where table_schema = '%s' and table_name = '%s';
	`

	for tableIdx, table := range v.Cfg.Tables {
		if v.ColumnTypes[table.Name] == nil {
			v.ColumnTypes[table.Name] = make(map[string]config.ColumnType)
		}
		d.TableResults[table.Name] = config.TableResult{
			Schema:  table.Schema,
			Columns: make(map[string]string),
		}
		reader := conn.Exec(
			context.Background(),
			fmt.Sprintf(query, table.Schema, table.Name),
		)
		result, err := reader.ReadAll()

		if err != nil {
			slog.Error(
				fmt.Sprintf("Failed to get data types: %v", err),
			)
		}

		if len(result[0].Rows) == 0 {
			slog.Warn(
				fmt.Sprintf("Table '%s' not found for source '%s'\n", table.Name, d.Url),
			)
		}
		parseQueryResult(result, d.TableResults[table.Name])
		collectDataTypes(v, tableIdx, d.TableResults[table.Name], table.Name, sourceIdx)
	}
}

func parseQueryResult(result []*pgconn.Result, table config.TableResult) {
	for _, res := range result {
		for _, row := range res.Rows {
			table.Columns[string(row[0])] = string(row[1])
		}
	}
}

func collectDataTypes(v config.Validator, tableIdx int, table config.TableResult, tableName string, sourceIdx int) {
	for _, column := range v.Cfg.Tables[tableIdx].Columns {
		if sourceIdx == 0 {
			v.ColumnTypes[tableName][column] = config.ColumnType{
				Types:        make([]string, len(v.Cfg.Sources)),
				MajorityType: "unknown",
			}
		}
		v.ColumnTypes[tableName][column].Types[sourceIdx] = table.Columns[column]
	}
}

func majority(v config.Validator, tableName string, columnName string, types []string) {
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
		slog.Warn(
			fmt.Sprintf("Column: '%s' is missing from majority of tables!", columnName),
		)
	} else if count > len(types)/2 {
		slog.Debug(
			fmt.Sprintf("Data type majority for column '%s' is: %s", columnName, majority),
		)
		if entry, ok := v.ColumnTypes[tableName][columnName]; ok {
			entry.MajorityType = majority
			v.ColumnTypes[tableName][columnName] = entry
		}
	} else {
		slog.Warn(
			fmt.Sprintf("No majority data type found for column '%s'!", columnName),
		)
	}
}
