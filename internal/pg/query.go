package pg

import (
	"context"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/jackc/pgx/v5"
)

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

	rows, err := pgx.CollectRows(result, pgx.RowToMap)

	if err != nil {
		return nil, err
	}

	return rows, nil
}
