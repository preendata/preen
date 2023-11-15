package plex

import (
	"github.com/scalecraft/plex-db/pkg/clickhouse"
	"github.com/scalecraft/plex-db/pkg/config"
	"github.com/scalecraft/plex-db/pkg/pg"

	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Snapshot the postgres data and load it into Clickhouse",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		ch := make(chan map[string]interface{}, 1)
		config := config.GetConfig()

		snapshot := pg.Snapshot(&config)
		clickhouse.Insert(&config, ch)

	},
}

func init() {
	rootCmd.AddCommand(snapshotCmd)
}
