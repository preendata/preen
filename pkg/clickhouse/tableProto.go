package clickhouse

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/ch-go"
	"github.com/scalecraft/plex-db/pkg/config"
)

func CreateTables(cfg *config.Config, protoPath string, messageName string) {
	slog.Info("Creating tables in Clickhouse.")
	ctx := context.Background()

	conn := Connect(ctx, cfg)

	if err := conn.Do(ctx, ch.Query{
		Body: fmt.Sprintf(
			"CREATE TABLE if not exists %s  ENGINE=File('Protobuf', 'nonexist') SETTINGS format_schema='%s:%s'",
			messageName,
			protoPath,
			messageName,
		),
	}); err != nil {
		slog.Error("Failed to create table in Clickhouse.")
		panic(err)
	}
}
