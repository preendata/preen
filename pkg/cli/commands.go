package cli

import (
	"encoding/json"
	"fmt"
	"github.com/hyphadb/hyphadb/internal/hlog"
	"github.com/hyphadb/hyphadb/internal/utils"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/engine"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/urfave/cli/v2"
)

func query(c *cli.Context) error {
	hlog.Debug("Executing cli.query")

	stmt := c.Args().First()
	hlog.Debug("Query: ", stmt)

	config, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	rows, err := engine.Execute(stmt, &config)

	if err != nil {
		hlog.Debug("error executing query", err)
		return fmt.Errorf("error executing query %w", err)
	}

	err = utils.PrintPrettyJSON(rows)
	if err != nil {
		return fmt.Errorf("error pretty printing JSON %w", err)
	}

	//TODO allow for output to file

	return nil
}

func stats(c *cli.Context) error {
	hlog.Debug("Executing cli.stats")

	config, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	stats, err := pg.GetStats(&config)

	if err != nil {
		hlog.Debug("error getting stats", err)
		return fmt.Errorf("error getting stats %w", err)
	}

	err = utils.PrintPrettyStruct(stats)
	if err != nil {
		return fmt.Errorf("error pretty printing JSON %w", err)
	}

	//TODO allow for output to file

	return nil
}

func validate(c *cli.Context) error {
	hlog.Debug("Executing cli.stats")

	config, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	validator, err := pg.Validate(&config)

	if err != nil {
		hlog.Debug("error validating config", err)
		return fmt.Errorf("error validating config %w", err)
	}

	err = utils.PrintPrettyStruct(validator)
	if err != nil {
		return fmt.Errorf("error pretty printing JSON %w", err)
	}

	//TODO allow for output to file

	return nil
}

func listConnections(c *cli.Context) error {
	hlog.Debug("Executing cli.listConnections")
	config, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
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
