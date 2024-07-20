package cli_commands

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

func Query(c *cli.Context) error {
	hlog.Debug("Executing cli.query")
	format := c.String("format")
	stmt := c.Args().First()
	hlog.Debug("Query: ", stmt)

	conf, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	qr, err := engine.Execute(stmt, &conf)

	if err != nil {
		hlog.Debug("error executing query", err)
		return fmt.Errorf("error executing query %w", err)
	}
	if format == "json" {
		if err := utils.PrintPrettyJSON(qr.Rows); err != nil {
			return fmt.Errorf("error pretty printing JSON: %w", err)
		}
	} else {
		if err := utils.WriteToTable(qr.Rows, qr.Columns); err != nil {
			return fmt.Errorf("error writing to table: %w", err)
		}
	}

	return nil
}

func Validate(c *cli.Context) error {
	hlog.Debug("Executing cli.stats")

	conf, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	validator, err := pg.Validate(&conf)

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

func ListConnections(c *cli.Context) error {
	hlog.Debug("Executing cli.listConnections")
	conf, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	for _, conn := range conf.Sources {
		c, err := json.MarshalIndent(conn, "", "  ")

		if err != nil {
			return fmt.Errorf("error unmarshalling config %w", err)
		}

		fmt.Println(string(c))
	}
	return nil
}

// BROKEN - this is hardcoded and incomplete for now.
func SaveConnection(c *cli.Context) error {
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
