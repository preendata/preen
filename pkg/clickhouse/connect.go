package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/ClickHouse/ch-go"
	"github.com/scalecraft/plex-db/pkg/config"
)

func Connect(ctx context.Context, cfg *config.Config) *ch.Client {
	slog.Debug(
		fmt.Sprintf("Connecting to Clickhouse: %s:%d", cfg.Target.Connection.Host, cfg.Target.Connection.Port),
	)

	conn, err := ch.Dial(ctx, ch.Options{User: cfg.Target.Connection.Username, Password: cfg.Target.Connection.Password})

	if err != nil {
		slog.Error(
			fmt.Sprintf("Cannot connect to Clickhouse: %s:%d", cfg.Target.Connection.Host, cfg.Target.Connection.Port),
		)
		os.Exit(1)
	}

	return conn
}
