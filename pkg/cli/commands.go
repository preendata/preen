package cli

import (
	"encoding/json"
	"fmt"

	"github.com/urfave/cli/v2"
)

type Connections struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func listConnections(c *cli.Context) error {
	connections := []Connections{
		{Host: "localhost", Port: 5433},
		{Host: "localhost", Port: 5434},
		{Host: "localhost", Port: 5435},
	}

	for _, conn := range connections {
		b, err := json.MarshalIndent(conn, "", "  ")

		if err != nil {
			fmt.Println("Error:", err)
			return nil
		}

		fmt.Println(string(b))
	}
	return nil
}
