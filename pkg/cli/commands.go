package cli

import (
	"encoding/json"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/pkg/hlog"
	"github.com/urfave/cli/v2"
)

func listConnections(c *cli.Context) error {
	config := config.GlobalConfig

	for _, conn := range config.Sources {
		c, err := json.MarshalIndent(conn, "", "  ")

		if err != nil {
			hlog.Fatal("Error unmarshalling config:", err)
			return nil
		}

		fmt.Println(string(c))
	}
	return nil
}

// BROKEN - this is hardcoded and incomplete for now.
func saveConnection(c *cli.Context) error {
	filename := config.SingleConfigPath
	newSource := config.Source{
		Name:   "users-db-us-east-3",
		Engine: "postgres",
		Connection: config.Connection{
			Host:     "127.0.0.1",
			Port:     54329,
			Database: "postgres",
			Username: "${POSTGRES_USER}",
			Password: "${POSTGRES_PASSWORD}",
		},
	}

	err := config.AddSource(filename, newSource)

	if err != nil {
		hlog.Fatal("Error saving new connection: ", err)
	}

	return nil
}
