package cli

import (
	"encoding/json"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/pkg/hlog"
	"github.com/urfave/cli/v2"
)

func query(c *cli.Context) error {
	hlog.Debug("Executing cli.query")

	stmt := c.Args().First()
	hlog.Debug("Query: ", stmt)

	return nil
}

func listConnections(c *cli.Context) error {
	hlog.Debug("Executing cli.listConnections")
	config, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("Error getting config %w", err)
	}

	for _, conn := range config.Sources {
		c, err := json.MarshalIndent(conn, "", "  ")

		if err != nil {
			return fmt.Errorf("error unmarshalling config %w", err)
		}

		fmt.Println(string(c))
	}
	return nil
}

// BROKEN - this is hardcoded and incomplete for now.
func saveConnection(c *cli.Context) error {
	hlog.Debug("Executing cli.saveConnection")
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
		return fmt.Errorf("error saving new connection: %w", err)
	}

	return nil
}
