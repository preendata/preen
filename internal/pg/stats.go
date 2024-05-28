package pg

import (
	"github.com/hyphadb/hyphadb/internal/config"
)

type Stats struct {
	TableSchema string
	TableName   string
	RowCount    int64
}

func GetStats(cfg *config.Config) ([]Stats, error) {
	stats := []Stats{}

	query := `
		select 
			schemaname, relname, n_live_tup
		from 
			pg_stat_user_tables;
	`

	for _, source := range cfg.Sources {
		results, err := ExecuteRaw(query, cfg, source)

		if err != nil {
			return nil, err
		}

		for _, row := range results {
			stat := Stats{}

			stat.TableSchema = row["schemaname"].(string)
			stat.TableName = row["relname"].(string)
			stat.RowCount = row["n_live_tup"].(int64)

			stats = append(stats, stat)
		}
	}

	return stats, nil
}
