package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/scalecraft/plex-db/internal/config"
)

type Result map[string]any

func Query(query string, cfg *config.Config) ([]map[string]any, error) {
	returnResults := []map[string]any{}
	for _, source := range cfg.Sources {
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
			query,
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

	return returnResults, nil
}
