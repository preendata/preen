package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/scalecraft/plex-db/pkg/config"
)

type Table struct {
	Columns map[string]string `json:"columns"`
	Schema  string            `json:"schema"`
}

type ColumnTypes struct {
	Types        []string `json:"types"`
	MajorityType string   `json:"majority_type"`
}

type Source struct {
	Tables map[string]Table `json:"tables"`
	Url    string           `json:"url"`
}

type Validator struct {
	Sources     map[string]Source      `json:"sources"`
	ColumnTypes map[string]ColumnTypes `json:"column_types"`
	cfg         config.Config          `json:"-"`
}

func Validate(cfg *config.Config) {
	var v Validator
	v.cfg = *cfg
	v.Sources = make(map[string]Source)
	v.ColumnTypes = make(map[string]ColumnTypes)

	for _, source := range v.cfg.Sources {
		v.Sources[source.Name] = Source{
			Url: fmt.Sprintf(
				"postgres://%s:%s@%s:%d/%s",
				source.Connection.Username,
				source.Connection.Password,
				source.Connection.Host,
				source.Connection.Port,
				source.Connection.Database,
			),
			Tables: make(map[string]Table),
		}
		v.getDataTypes(v.Sources[source.Name])
	}

	for column, types := range v.ColumnTypes {
		v.majority(column, types.Types)
	}

	validatorJSON, err := json.Marshal(v)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Failed to marshal sources: %v", err),
		)
	}

	slog.Debug(string(validatorJSON))

}

func (v *Validator) getDataTypes(source Source) {
	conn := connect(source.Url)
	defer conn.Close(context.Background())

	query := `
		select column_name, data_type from information_schema.columns
		where table_schema = '%s' and table_name = '%s';
	`

	for tableIdx, table := range v.cfg.Tables {
		source.Tables[table.Name] = Table{
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
				fmt.Sprintf("Table '%s' not found for source '%s'\n", table.Name, source.Url),
			)
		}
		v.parseQueryResult(result, source.Tables[table.Name])
		v.collectDataTypes(tableIdx, source.Tables[table.Name])
	}
}

func (v *Validator) parseQueryResult(result []*pgconn.Result, table Table) {
	for _, res := range result {
		for _, row := range res.Rows {
			table.Columns[string(row[0])] = string(row[1])
		}
	}
}

func (v *Validator) collectDataTypes(tableIdx int, table Table) {
	for _, column := range v.cfg.Tables[tableIdx].Columns {
		if len(v.ColumnTypes[column].Types) == 0 {
			v.ColumnTypes[column] = ColumnTypes{
				Types:        make([]string, len(v.cfg.Sources)),
				MajorityType: "unknown",
			}
		}
		v.ColumnTypes[column].Types[len(v.Sources)-1] = table.Columns[column]
	}
}

func (v *Validator) majority(columnName string, types []string) {
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
		if entry, ok := v.ColumnTypes[columnName]; ok {
			entry.MajorityType = majority
			v.ColumnTypes[columnName] = entry
		}
	} else {
		slog.Warn(
			fmt.Sprintf("No majority data type found for column '%s'!", columnName),
		)
	}
}
