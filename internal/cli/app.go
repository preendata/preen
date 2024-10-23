package cli

import (
	"fmt"

	"github.com/preendata/preen/internal/engine"
	"github.com/urfave/cli/v2"
)

func NewApp() *cli.App {
	app := &cli.App{
		Name:  "preen",
		Usage: "A command-line application for preen",
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
				Name:    "model",
				Aliases: []string{"m"},
				Usage:   "Commands to manage models",
				Subcommands: []*cli.Command{
					{
						Name:    "build",
						Action:  BuildModel,
						Aliases: []string{"b"},
						Usage:   "Build model",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:    "target",
								Aliases: []string{"t"},
								Usage:   "Target a specific model(s). The default is all models. This is relative to the PREEN_MODELS_PATH.",
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
						Name:    "metadata",
						Aliases: []string{"i"},
						Usage:   "Build source metadata",
						Action:  BuildMetadata,
					},
				},
			},
			{
				Name:  "version",
				Usage: "Print the version of the application",
				Action: func(c *cli.Context) error {
					fmt.Println("Preen version:", engine.Version)
					return nil
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

			err := engine.IsValidLogLevel(logLevel)
			if logLevel != "" && err != nil {
				return fmt.Errorf("invalid log level: %s. Allowed values are: DEBUG, INFO, WARN, ERROR, FATAL, PANIC", logLevel)
			}

			// Initialize logger, passes empty string if no flag set which is handled by variadic Intialize function
			if err := engine.Initialize(logLevel); err != nil {
				return err
			}

			return nil
		},
	}
	return app
}
