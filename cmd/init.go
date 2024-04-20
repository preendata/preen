package plex

import (
	"github.com/scalecraft/plex-db/pkg/clickhouse"
	"github.com/scalecraft/plex-db/pkg/config"
	"github.com/scalecraft/plex-db/pkg/pg"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Validate and Initialize the schema structures in target databases.",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		config := config.GetConfig()
		pg.Validate(&config)
		clickhouse.CreateTable(&config)
		// clickhouse.CreateTable(&config, "proto/transactions.proto", "transactions")
		// clickhouse.CreateTable(&config, "proto/users.proto", "users")
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}
