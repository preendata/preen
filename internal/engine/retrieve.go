package engine

import (
	"database/sql/driver"
	"fmt"
	"slices"

	"golang.org/x/sync/errgroup"
)

type Retriever struct {
	ModelName string
	Query     string
	Source    configSource
}

func Retrieve(cfg *Config, models Models) error {
	for _, modelName := range cfg.Models {
		ic := make(chan []driver.Value, 10000)
		dc := make(chan []int64)
		go Insert(modelName, ic, dc)
		if err != nil {
			return err
		}
		g := errgroup.Group{}
		g.SetLimit(200)
		for _, source := range cfg.ConfigSources {
			if !slices.Contains(source.Models, modelName) {
				Debug(fmt.Sprintf("Skipping %s for %s", modelName, source.Name))
				continue
			}
			r := Retriever{
				Source:    source,
				ModelName: modelName,
				Query:     models.Config[ModelName(modelName)].Query,
			}
			switch source.Engine {
			case "postgres":
				func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestPostgresSource(&r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
			case "mysql":
				func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestMysqlSource(&r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
			case "mongodb":
				func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestMongoSource(&r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
			default:
				Error(fmt.Sprintf("Engine %s not supported", source.Engine))
			}
		}
		if err = g.Wait(); err != nil {
			return err
		}

		ic <- []driver.Value{"quit"}
		ConfirmInsert(modelName, dc, 0)
	}
	return nil
}
