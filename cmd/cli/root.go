package plex

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "plex",
	Short: "PlexDB is an open source analytics replication CLI",
	Long: `
PlexDB is an open source analytics replication CLI.
Use it to stream data from multiple data sources into Clickhouse.
 _____  _           _____  ____  
|  __ \| |         |  __ \|  _ \ 
| |__) | | _____  _| |  | | |_) |
|  ___/| |/ _ \ \/ / |  | |  _ < 
| |    | |  __/>  <| |__| | |_) |
|_|    |_|\___/_/\_\_____/|____/ 
									
Full documenation is available at https://github.com/scalecraft/plex-db
	`,
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Whoops. There was an error while executing your CLI '%s'", err)
		os.Exit(1)
	}
}
