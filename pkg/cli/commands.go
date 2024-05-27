package cli

import (
	"encoding/json"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/urfave/cli/v2"
)

func listConnections(c *cli.Context) error {
	config := config.GetConfig("test.yaml")

	for _, conn := range config.Sources {
		c, err := json.MarshalIndent(conn, "", "  ")

		if err != nil {
			fmt.Println("Error:", err)
			return nil
		}

		fmt.Println(string(c))
	}
	return nil
}

func saveConnection(c *cli.Context) error {
	filename := "test.yaml"
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
		fmt.Println("Error saving new connection: ", err)
	}

	return nil
}
