package cli

import (
	"fmt"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/urfave/cli/v2"
)

func NewApp() *cli.App {
	app := &cli.App{
		Name:  "HyphaDB",
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
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "output-format",
						Aliases:     []string{"o"},
						Usage:       "Set output format. Options are 'table', 'csv', 'markdown'",
						DefaultText: "table",
						Action: func(c *cli.Context, v string) error {
							format := c.String("output-format")
							if format != "table" && format != "csv" && format != "markdown" {
								return fmt.Errorf("invalid format: %s. Allowed values are 'table', 'csv', 'markdown'", format)
							}
							return nil
						},
					},
				},
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
				Name:    "context",
				Aliases: []string{"c"},
				Usage:   "Commands to manage contexts",
				Subcommands: []*cli.Command{
					{
						Name:    "build",
						Action:  BuildContext,
						Aliases: []string{"b"},
						Usage:   "Build context",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:    "context-name",
								Aliases: []string{"cn"},
								Usage:   "Target a specific context",
							},
							&cli.BoolFlag{
								Name:    "source-name",
								Aliases: []string{"sn"},
								Usage:   "Target a specific source",
							},
						},
					},
				},
			},
			{
				Name:    "source",
				Aliases: []string{"s"},
				Usage:   "Commands to manage sources",
				Subcommands: []*cli.Command{
					{
						Name:    "list",
						Aliases: []string{"l"},
						Usage:   "Print stored sources.",
						Action:  ListSources,
					},
					{
						Name:    "validate",
						Aliases: []string{"v"},
						Usage:   "Validate config file and retrieve source data types",
						Action:  Validate,
					},
					{
						Name:    "info",
						Aliases: []string{"i"},
						Usage:   "Build source information schema",
						Action:  BuildInformationSchema,
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
