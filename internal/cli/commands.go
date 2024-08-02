package cli

import (
	"encoding/json"
	"fmt"

	"github.com/hyphadb/hyphadb/internal/utils"

	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/engine"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/urfave/cli/v2"
)

func Query(c *cli.Context) error {
	utils.Debug("Executing cli.query")
	format := c.String("format")
	stmt := c.Args().First()
	utils.Debug("Query: ", stmt)

	conf, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	qr, err := engine.Execute(stmt, conf)

	if err != nil {
		utils.Debug("error executing query", err)
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

func BuildContext(c *cli.Context) error {
	utils.Debug("Executing cli.buildcontext")

	conf, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	err = engine.BuildContext(conf)
	if err != nil {
		return fmt.Errorf("error building context %w", err)
	}

	return nil
}

func Validate(c *cli.Context) error {
	utils.Debug("Executing cli.validate")

	conf, err := config.GetConfig()

	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	_, err = pg.Validate(conf)

	if err != nil {
		utils.Debug("error validating config", err)
		return fmt.Errorf("error validating config %w", err)
	}

	return nil
}

func ListConnections(c *cli.Context) error {
	utils.Debug("Executing cli.listConnections")
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