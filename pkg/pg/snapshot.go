package pg

import (
	"context"
	"fmt"
	"log"

	"github.com/scalecraft/plex-db/pkg/clickhouse"
	"github.com/scalecraft/plex-db/pkg/config"

	"github.com/jackc/pgx/v5/pgconn"
)

type snapshotter struct {
	conn *pgconn.PgConn
	cfg  config.Config
	url  string
}

func Snapshot(cfg *config.Config) {
	var s snapshotter
	s.cfg = *cfg

	for _, source := range s.cfg.Sources {
		s.url = fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s",
			source.Connection.Username,
			source.Connection.Password,
			source.Connection.Host,
			source.Connection.Port,
			source.Connection.Database,
		)
		s.getSnapshot(source.Name)
	}
}

func (s *snapshotter) getSnapshot(sourceName string) {
	s.conn = connect(s.url)
	defer s.conn.Close(context.Background())

	for _, table := range s.cfg.Tables {
		result := s.conn.Exec(
			context.Background(),
			fmt.Sprintf("select * from %s;", table.Name),
		)

		snapshot, err := result.ReadAll()

		if err != nil {
			log.Fatalf("Failed to export snapshot: %s", err)
		}

		clickhouse.Insert(&s.cfg, snapshot, sourceName, table.Name)
	}
}
