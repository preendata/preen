package pg

import (
	"context"
	"fmt"

	"github.com/hyphasql/hypha/internal/config"
	"github.com/hyphasql/hypha/internal/utils"
	"github.com/jackc/pgx/v5"
)

type QueryResult struct {
	Rows    []map[string]any
	Columns []string
}

// Execute a raw statement on all sources in the config.
func ExecuteRaw(statement string, cfg *config.Config, source config.Source) (pgx.Rows, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		source.Connection.Password,
		source.Connection.Host,
		source.Connection.Port,
		source.Connection.Database,
	)

	dbpool, err := pool(url)

	if err != nil {
		return nil, err
	}

	defer dbpool.Close()
	utils.Debug("Executing query against Postgres: ", statement)

	rows, err := dbpool.Query(context.Background(), statement)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
