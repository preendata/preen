package pg

import (
	"context"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
}

type Validator struct {
	Sources     map[string]Source                `json:"sources"`
	ColumnTypes map[string]map[string]ColumnType `json:"column_types"`
	cfg         config.Config                    `json:"-"`
}

type ColumnTypeMessage struct {
	Table     string
	Column    string
	SourceIdx int
	Type      string
}

func Validate(cfg *config.Config) (*Validator, error) {
	var v Validator
	v.cfg = *cfg
	v.Sources = make(map[string]Source)
	v.ColumnTypes = make(map[string]map[string]ColumnType)
	typeChan := make(chan ColumnTypeMessage)
	quitChan := make(chan string)

	g := new(errgroup.Group)

	go processTableColumnType(typeChan, quitChan, v)

	for sourceIdx, cfgSource := range v.cfg.Sources {
		pool, err := PoolFromSource(cfgSource)
		if err != nil {
			return nil, err
		}

		defer pool.Close()

		utils.Debug(fmt.Sprintf("Validating data for source: %s", cfgSource.Name))

		source := Source{
			Tables: make(map[string]Table),
		}

		g.Go(func() error {
			source.Tables, err = getTables(pool)
			if err != nil {
				return err
			}

			err := getColumnTypes(cfg, source.Tables, sourceIdx, pool, typeChan)
			if err != nil {
				return err
			}
			return nil
		})
		v.Sources[cfgSource.Name] = source
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	quitChan <- "quit"
	for table, columns := range v.ColumnTypes {
		for column, types := range columns {
			v.majority(table, column, types.Types)
		}
	}
	// utils.Debug(utils.PrintPrettyStruct(v.ColumnTypes))

	utils.Info("Source table and data type validation completed successfully!")

	return &v, nil

}

func getTables(pool *pgxpool.Pool) (map[string]Table, error) {
	tables := make(map[string]Table)
	query := `
		select table_schema, table_name from information_schema.tables
		where table_schema not in ('information_schema', 'pg_catalog');
	`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		row := make([]string, 2)
		if err := rows.Scan(&row[0], &row[1]); err != nil {
			return nil, err
		}
		tables[row[1]] = Table{
			Schema:  row[0],
			Name:    row[1],
			Columns: make(map[string]string),
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func getColumnTypes(cfg *config.Config, tables map[string]Table, sourceIdx int, pool *pgxpool.Pool, c chan<- ColumnTypeMessage) error {

	query := `
		select column_name, data_type from information_schema.columns
		where table_schema = '%s' and table_name = '%s';
	`

	for _, table := range tables {
		utils.Debug(fmt.Sprintf("Validating data types for source index: %d table: %s", sourceIdx, table.Name))
		rows, err := pool.Query(
			context.Background(),
			fmt.Sprintf(query, table.Schema, table.Name),
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		columns, err := parseQueryResult(rows)
		if err != nil {
			return err
		}
		collectColumnTypes(cfg, sourceIdx, columns, table.Name, c)
	}

	return nil
}

func parseQueryResult(rows pgx.Rows) (map[string]string, error) {
	columns := make(map[string]string)
	for rows.Next() {
		row := make([]string, 2)
		rows.Scan(&row[0], &row[1])
		columns[row[0]] = row[1]
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func collectColumnTypes(cfg *config.Config, sourceIdx int, columns map[string]string, tableName string, c chan<- ColumnTypeMessage) {
	for colName, colType := range columns {
		c <- ColumnTypeMessage{
			Table:     tableName,
			Column:    colName,
			SourceIdx: sourceIdx,
			Type:      colType,
		}
	}
}

func processTableColumnType(typeChan <-chan ColumnTypeMessage, quitChan <-chan string, v Validator) {
	for {
		select {
		case <-quitChan:
			utils.Debug("Quitting table column type processing")
			return
		case message := <-typeChan:
			fmt.Println(message)
			if _, ok := v.ColumnTypes[message.Table]; !ok {
				v.ColumnTypes[message.Table] = make(map[string]ColumnType)
			}
			if _, ok := v.ColumnTypes[message.Table][message.Column]; !ok {
				v.ColumnTypes[message.Table][message.Column] = ColumnType{
					Types:        make([]string, len(v.cfg.Sources)),
					MajorityType: "unknown",
				}
			}
			v.ColumnTypes[message.Table][message.Column].Types[message.SourceIdx] = message.Type
		}
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
		utils.Warn(
			fmt.Sprintf("Column: '%s' is missing from majority of tables!", columnName),
		)
	} else if count > len(types)/2 && count == len(types) {
		utils.Debug(
			fmt.Sprintf("Data type for column '%s' is: %s", columnName, majority),
		)
		if entry, ok := v.ColumnTypes[tableName][columnName]; ok {
			entry.MajorityType = majority
			v.ColumnTypes[tableName][columnName] = entry
		}
	} else if count > len(types)/2 && count != len(types) {
		utils.Warn(
			fmt.Sprintf("Discrepancy in data types for column '%s'! Using majority data type of %s", columnName, majority),
		)
		if entry, ok := v.ColumnTypes[tableName][columnName]; ok {
			entry.MajorityType = majority
			v.ColumnTypes[tableName][columnName] = entry
		}
	} else {
		utils.Warn(
			fmt.Sprintf("No majority data type found for column '%s'!", columnName),
		)
	}
}
