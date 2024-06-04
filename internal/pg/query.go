package pg

import (
	"context"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/jackc/pgx/v5"
)

type QueryResult struct {
	Rows    []map[string]any
	Columns []string
}

// Execute a raw statement on all sources in the config.
func ExecuteRaw(statement string, cfg *config.Config, source config.Source) ([]map[string]any, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		source.Connection.Password,
		source.Connection.Host,
		source.Connection.Port,
		source.Connection.Database,
	)

	dbpool, err := dbpool(url)

	if err != nil {
		return nil, err
	}

	defer dbpool.Close()

	result, err := dbpool.Query(
		context.Background(),
		statement,
	)

	if err != nil {
		return nil, err
	}

	qr, err := buildResultSet(result)
	if err != nil {
		return nil, err
	}

	return qr.Rows, nil
}

func buildResultSet(result pgx.Rows) (QueryResult, error) {
	// Pull out the column names in order to preserve the order specified in the DB schema.
	fieldDescriptions := result.FieldDescriptions()
	columns := make([]string, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = string(fd.Name)
	}

	rows, err := pgx.CollectRows(result, pgx.RowToMap)
	if err != nil {
		return QueryResult{}, err
	}

	rv := QueryResult{
		Rows:    rows,
		Columns: columns,
	}

	return rv, nil

}
