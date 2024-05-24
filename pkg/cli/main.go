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
				Name:    "list-connections",
				Aliases: []string{"lc"},
				Usage:   "Print stored connection credentials",
				Action:  listConnections,
			},
			{
				Name:    "save-connection",
				Aliases: []string{"sc"},
				Usage:   "Save new connection to config",
				Action:  saveConnection,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "name",
						Aliases: []string{"n"},
						Usage:   "Assign a name to your connection. This is for reference only and does not impact the connection string.",
					},
					&cli.StringFlag{
						Name:    "engine",
						Aliases: []string{"e"},
						Usage:   "Options are 'postgres' or 'mysql'",
					},
				},
			},
		},
	}
	return app
}
