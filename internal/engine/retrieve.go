package engine

import (
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"
)

type Retriever struct {
	ModelName string
	Query     string
	Source    Source
}

func Retrieve(sc *SourceConfig, mc *ModelConfig) error {
	for _, model := range mc.Models {
		ic := make(chan []driver.Value, 10000)
		dc := make(chan []int64)
		tableName := strings.ReplaceAll(string(model.Name), "-", "_")
		go Insert(ModelName(tableName), ic, dc)
		if err != nil {
			return err
		}
		g := errgroup.Group{}
		g.SetLimit(200)
		for _, source := range sc.Sources {
			if !slices.Contains(source.Models, string(model.Name)) {
				Debug(fmt.Sprintf("Skipping %s for %s", model.Name, source.Name))
				continue
			}
			r := Retriever{
				Source:    source,
				ModelName: string(model.Name),
				Query:     model.Query,
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
		ConfirmInsert(string(model.Name), dc, 0)
	}
	return nil
}
