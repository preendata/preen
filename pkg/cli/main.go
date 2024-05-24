package cli

import (
	"github.com/urfave/cli/v2"
)

func NewApp() *cli.App {
	app := &cli.App{
		Name:  "mycli",
		Usage: "A command-line application for MyProject",
		Commands: []*cli.Command{
			{
				Name:   "list-connections",
				Usage:  "Print stored connection credentials",
				Action: listConnections,
			},
		},
	}
	return app
}
