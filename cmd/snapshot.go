package plex

import (
	"github.com/scalecraft/plex-db/pkg/config"
	"github.com/scalecraft/plex-db/pkg/pg"

	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Snapshot the postgres data and load it into Clickhouse",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		config := config.GetConfig()
		pg.Snapshot(&config)
	},
}

func init() {
	rootCmd.AddCommand(snapshotCmd)
}
