package plex

import (
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Validate and Initialize the schema structures in target databases.",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		// config := config.GetConfig()
		// columnTypes := pg.Validate(&config)
		// clickhouse.CreateTables(&config, &columnTypes)
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}
