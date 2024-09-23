package cli

import (
	"encoding/json"
	"fmt"

	"github.com/hyphasql/hypha/internal/engine"
	"github.com/urfave/cli/v2"
)

func Query(c *cli.Context) error {
	engine.Debug("Executing cli.query")
	format := c.String("format")
	stmt := c.Args().First()
	engine.Debug("Query: ", stmt)

	qr, err := engine.Execute(stmt)

	if err != nil {
		engine.Debug("error executing query", err)
		return fmt.Errorf("error executing query %w", err)
	}
	if format == "json" {
		if err := engine.PrintPrettyJSON(qr.Rows); err != nil {
			return fmt.Errorf("error pretty printing JSON: %w", err)
		}
	} else {
		if err := engine.WriteToTable(qr.Rows, qr.Columns, "table"); err != nil {
			return fmt.Errorf("error writing to table: %w", err)
		}
	}

	return nil
}

func BuildModel(c *cli.Context) error {
	engine.Debug("Executing cli.buildmodel")
	modelTarget := c.String("target")
	sc, mc, err := engine.GetConfig(modelTarget)
	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	err = engine.BuildModels(sc, mc)
	if err != nil {
		return fmt.Errorf("error building model %w", err)
	}

	return nil
}

func BuildMetadata(c *cli.Context) error {
	engine.Debug("Executing cli.buildInformationSchema")
	modelTarget := ""
	sc, mc, err := engine.GetConfig(modelTarget)
	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	err = engine.BuildMetadata(sc, mc)
	if err != nil {
		return fmt.Errorf("error building metadata %w", err)
	}

	return nil
}

func Validate(c *cli.Context) error {
	engine.Debug("Executing cli.validate")
	modelTarget := ""
	sc, mc, err := engine.GetConfig(modelTarget)
	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	if err := engine.ValidateConfigs(sc, mc); err != nil {
		return fmt.Errorf("error parsing models %w", err)
	}

	if err = engine.BuildMetadata(sc, mc); err != nil {
		return fmt.Errorf("error building metadata %w", err)
	}

	_, err = engine.BuildColumnMetadata()
	if err != nil {
		return fmt.Errorf("error building column metadata %w", err)
	}

	return nil
}

func ListSources(c *cli.Context) error {
	engine.Debug("Executing cli.listSources")
	modelTarget := ""
	sc, _, err := engine.GetConfig(modelTarget)
	if err != nil {
		return fmt.Errorf("error getting config %w", err)
	}

	for _, conn := range sc.Sources {
		_, err := json.MarshalIndent(conn, "", "  ")

		if err != nil {
			return fmt.Errorf("error unmarshalling config %w", err)
		}
	}
	return nil
}
