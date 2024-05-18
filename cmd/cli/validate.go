package plex

import (
	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate that all of the tables in the config file exist with uniform data types.",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		// Execute the validate command
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
}