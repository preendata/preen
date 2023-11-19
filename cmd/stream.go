package plex

import (
	"fmt"
	"sync"

	"github.com/scalecraft/plex-db/pkg/clickhouse"
	"github.com/scalecraft/plex-db/pkg/config"
	"github.com/scalecraft/plex-db/pkg/pg"
	"github.com/spf13/cobra"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Stream data from a Postgres database to Clickhouse",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		var wg sync.WaitGroup
		ch := make(chan map[string]interface{}, 1)

		config := config.GetConfig()
		for _, source := range config.Sources {
			wg.Add(1)
			url := fmt.Sprintf(
				"postgres://%s:%s@%s:%d/%s?replication=database",
				source.Connection.Username,
				source.Connection.Password,
				source.Connection.Host,
				source.Connection.Port,
				source.Connection.Database,
			)
			go func(sourceName string) {
				defer wg.Done()
				pg.Stream(&config, url, ch, sourceName)
			}(source.Name)
		}
		go func() {
			defer wg.Done()
			clickhouse.StreamInsert(&config, ch)
		}()
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(streamCmd)
}
