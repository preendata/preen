package cli

import (
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
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
				Name:    "repl",
				Aliases: []string{"r"},
				Usage:   "Initiate interactive query session",
				Action:  Repl,
			},
			{
				Name:    "query",
				Aliases: []string{"q"},
				Usage:   "Execute a query",
				Action:  Query,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "format",
						Aliases:     []string{"f"},
						Usage:       "Set output format. Options are 'table' or 'json'",
						DefaultText: "table",
						Action: func(c *cli.Context, v string) error {
							format := c.String("format")
							if format != "table" && format != "json" {
								return fmt.Errorf("invalid format: %s. Allowed values are 'table' or 'json'", format)
							}
							return nil
						},
					},
				},
			},
			{
				Name:    "build-context",
				Aliases: []string{"bc"},
				Usage:   "Retrieve data from sources and load it for local queries",
				Action:  BuildContext,
			},
			{
				Name:    "build-i",
				Aliases: []string{"bi"},
				Usage:   "Retrieve data from sources and load it for local queries",
				Action:  BuildInformationSchema,
			},
			{
				Name:    "validate",
				Aliases: []string{"v"},
				Usage:   "Validate config file",
				Action:  Validate,
			},
			{
				Name:    "list-connections",
				Aliases: []string{"lc"},
				Usage:   "Print stored connection credentials",
				Action:  ListConnections,
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

			err := utils.IsValidLogLevel(logLevel)
			if logLevel != "" && err != nil {
				return fmt.Errorf("invalid log level: %s. Allowed values are: DEBUG, INFO, WARN, ERROR, FATAL, PANIC", logLevel)
			}

			// Initialize logger, passes empty string if no flag set which is handled by variadic Intialize function
			if err := utils.Initialize(logLevel); err != nil {
				return err
			}

			// Initialize config
			if _, err := config.GetConfig(); err != nil {
				return err
			}
			return nil
		},
	}
	return app
}
