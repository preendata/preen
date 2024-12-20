package engine

import (
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"
)

type Retriever struct {
	ModelName    string
	TableName    string
	Query        string
	Source       Source
	Options      Options
	Format       string
	FilePatterns *[]string
	Collection   string
}

// Retrieve data from sources and insert into the duckDB database.
// Database sources are inserted via the Insert function.
// File sources are inserted via the native duckDB integrations.
func Retrieve(sc *SourceConfig, mc *ModelConfig) error {
	for _, model := range mc.Models {
		ic := make(chan []driver.Value, 10000)
		dc := make(chan []int64)
		tableName := strings.ReplaceAll(string(model.Name), "-", "_")
		// Only insert database models into DuckDB
		if model.Type == "database" {
			go Insert(ModelName(tableName), ic, dc)
		}
		g := errgroup.Group{}
		g.SetLimit(200)
		for _, source := range sc.Sources {
			if !slices.Contains(source.Models, string(model.Name)) {
				Debug(fmt.Sprintf("Skipping %s for %s", model.Name, source.Name))
				continue
			}
			r := Retriever{
				Source:       source,
				ModelName:    string(model.Name),
				Query:        model.Query,
				Options:      model.Options,
				Format:       model.Format,
				FilePatterns: model.FilePatterns,
				TableName:    tableName,
			}
			if model.Collection != "" {
				r.Collection = model.Collection
			} else {
				r.Collection = string(model.Name)
			}
			switch source.Engine {
			case "s3":
				err := func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestS3Model(&r); err != nil {
							return err
						}
						return nil
					})

					return nil
				}(r, ic)
				if err != nil {
					return err
				}
			case "snowflake":
				err := func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestSnowflakeModel(&r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
				if err != nil {
					return err
				}
			case "postgres":
				err := func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestPostgresModel(&r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
				if err != nil {
					return err
				}
			case "mysql":
				err := func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestMysqlModel(&r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
				if err != nil {
					return err
				}
			case "mongodb":
				err := func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := ingestMongoModel(&r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
				if err != nil {
					return err
				}
			default:
				Error(fmt.Sprintf("Engine %s not supported", source.Engine))
			}
		}
		if err := g.Wait(); err != nil {
			return err
		}
		ic <- []driver.Value{"quit"}
		if model.Type == "database" {
			ConfirmInsert(string(model.Name), dc, 0)
		}
	}
	return nil
}
