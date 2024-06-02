package cli

import (
	"fmt"
	"github.com/hyphadb/hyphadb/internal/hlog"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/urfave/cli/v2"
)

func NewApp() *cli.App {
	app := &cli.App{
		Name:  "HyphaDB CLI",
		Usage: "A command-line application for HyphaDB",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "log-level",
				Aliases: []string{"l"},
				Usage:   "Set the log level (DEBUG, INFO, WARN, ERROR, FATAL, PANIC)",
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
				Usage:   "Set the log level to DEBUG",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "stats",
				Aliases: []string{"s"},
				Usage:   "Wtf does this do?",
				Action:  stats,
			},
			{
				Name:    "query",
				Aliases: []string{"q"},
				Usage:   "Execute a query",
				Action:  query,
			},
			{
				Name:   "validate",
				Usage:  "Validate config file",
				Action: validate,
			},
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
		Before: func(c *cli.Context) error {
			logLevel := ""

			// Check if log-level flag is set
			if c.IsSet("log-level") {
				logLevel = c.String("log-level")
			}

			// Check if verbose flag is set
			if c.Bool("verbose") {
				logLevel = "DEBUG"
			}

			err := hlog.IsValidLogLevel(logLevel)
			if logLevel != "" && err != nil {
				return fmt.Errorf("invalid log level: %s. Allowed values are: DEBUG, INFO, WARN, ERROR, FATAL, PANIC", logLevel)
			}

			// Initialize logger, passes empty string if no flag set which is handled by variadic Intialize function
			if err := hlog.Initialize(logLevel); err != nil {
				return err
			}

			// Initialize config
			if err := config.Initialize(); err != nil {
				return err
			}
			return nil
		},
	}
	return app
}
