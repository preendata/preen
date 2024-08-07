package pg

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/errgroup"
)

type Table struct {
	Columns map[string]string `json:"columns"`
	Schema  string            `json:"schema"`
	Name    string            `json:"name"`
}

type ColumnType struct {
	Types        []string `json:"types"`
	MajorityType string   `json:"majority_type"`
}

type Source struct {
	Tables map[string]Table `json:"tables"`
	Url    string           `json:"url"`
}

type Validator struct {
	Sources     map[string]Source                `json:"sources"`
	ColumnTypes map[string]map[string]ColumnType `json:"column_types"`
	cfg         config.Config                    `json:"-"`
	mu          sync.Mutex
}

func Validate(cfg *config.Config) (*Validator, error) {
	var v Validator
	v.cfg = *cfg
	v.Sources = make(map[string]Source)
	v.ColumnTypes = make(map[string]map[string]ColumnType)

	g := new(errgroup.Group)
	for sourceIdx, source := range v.cfg.Sources {
		source := source
		sourceIdx := sourceIdx

		g.Go(func() error {
			sourceUrl := fmt.Sprintf(
				"postgres://%s:%s@%s:%d/%s",
				source.Connection.Username,
				url.QueryEscape(source.Connection.Password),
				url.QueryEscape(source.Connection.Host),
				source.Connection.Port,
				source.Connection.Database,
			)

			v.mu.Lock()
			v.Sources[source.Name] = Source{
				Url:    sourceUrl,
				Tables: make(map[string]Table),
			}
			v.mu.Unlock()

			err := v.getTables(v.Sources[source.Name])
			if err != nil {
				return err
			}

			err = v.getDataTypes(v.Sources[source.Name], sourceIdx)
			if err != nil {
				return err
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	for table, columns := range v.ColumnTypes {
		for column, types := range columns {
			v.majority(table, column, types.Types)
		}
	}

	utils.Info("Source table and data type validation completed successfully!")

	return &v, nil

}

func (v *Validator) getTables(source Source) error {
	conn, err := connect(source.Url)

	if err != nil {
		return err
	}

	query := `
		select table_schema, table_name from information_schema.tables
		where table_schema not in ('information_schema', 'pg_catalog');
	`

	reader := conn.Exec(context.Background(), query)
	result, err := reader.ReadAll()

	if err != nil {
		return err
	}

	if len(result[0].Rows) == 0 {
		utils.Warn(
			fmt.Sprintf("No tables found for source '%s'\n", source.Url),
		)
	}

	for _, res := range result {
		for _, row := range res.Rows {
			tableName := string(row[1])
			tableSchema := string(row[0])
			source.Tables[tableName] = Table{
				Schema:  tableSchema,
				Name:    tableName,
				Columns: make(map[string]string),
			}
		}
	}

	return nil
}

func (v *Validator) getDataTypes(source Source, sourceIdx int) error {
	conn, err := connect(source.Url)

	if err != nil {
		return err
	}

	defer conn.Close(context.Background())

	query := `
		select column_name, data_type from information_schema.columns
		where table_schema = '%s' and table_name = '%s';
	`

	for _, table := range source.Tables {

		// if v.ColumnTypes[name] == nil {
		// 	v.ColumnTypes[name] = make(map[string]ColumnType)
		// }

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
		v.collectDataTypes(source, table.Name, sourceIdx)
	}

	return nil
}

// I dont think this does anything, its not operating on a pointer and doesnt return anything
func (v *Validator) parseQueryResult(result []*pgconn.Result, table Table) {
	for _, res := range result {
		for _, row := range res.Rows {
			table.Columns[string(row[0])] = string(row[1])
		}
	}
}

func (v *Validator) collectDataTypes(source Source, tableName string, sourceIdx int) {
	for colName, colType := range source.Tables[tableName].Columns {
		if v.ColumnTypes[tableName] == nil {
			v.ColumnTypes[tableName] = make(map[string]ColumnType)
		}

		v.mu.Lock()
		if _, ok := v.ColumnTypes[tableName][colName]; !ok {
			v.ColumnTypes[tableName][colName] = ColumnType{
				Types:        make([]string, len(v.cfg.Sources)),
				MajorityType: "unknown",
			}
		}
		v.ColumnTypes[tableName][colName].Types[sourceIdx] = colType
		v.mu.Unlock()
	}
}

func (v *Validator) majority(tableName string, columnName string, types []string) {
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
	} else if count > len(types)/2 && count == len(types) {
		slog.Debug(
			fmt.Sprintf("Data type for column '%s' is: %s", columnName, majority),
		)
		if entry, ok := v.ColumnTypes[tableName][columnName]; ok {
			entry.MajorityType = majority
			v.ColumnTypes[tableName][columnName] = entry
		}
	} else if count > len(types)/2 && count != len(types) {
		slog.Warn(
			fmt.Sprintf("Discrepancy in data types for column '%s'!", columnName),
		)
	} else {
		slog.Warn(
			fmt.Sprintf("No majority data type found for column '%s'!", columnName),
		)
	}
}
