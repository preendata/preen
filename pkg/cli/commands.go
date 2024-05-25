package cli

import (
	"encoding/json"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/urfave/cli/v2"
)

func listConnections(c *cli.Context) error {
	config := config.GetConfig()

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
	fmt.Println("Saving password to plaintext")

	return nil
}
