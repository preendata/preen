package pg

import (
	"context"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/sql"
	"github.com/jackc/pgx/v5"
)

func Execute(statement string, cfg *config.Config) ([]map[string]any, error) {
	returnResults := []map[string]any{}
	for idx, source := range cfg.Sources {

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

		parsedQuery, err := sql.Parse(statement, cfg, idx)

		if err != nil {
			return nil, err
		}

		if parsedQuery != nil {
			result, err := dbpool.Query(
				context.Background(),
				*parsedQuery,
			)

			if err != nil {
				return nil, err
			}

			rows, err := pgx.CollectRows(result, pgx.RowToMap)

			if err != nil {
				return nil, err
			}

			returnResults = append(returnResults, rows...)
		}
	}

	return returnResults, nil
}
