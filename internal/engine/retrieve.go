package engine

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcboeker/go-duckdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"golang.org/x/sync/errgroup"
)

type Source interface {
	ingest(r *Retriever) error
}

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
			switch source.Engine {
			case "postgres":
				pool, err := GetPostgresPoolFromSource(source)
				if err != nil {
					return err
				}
				r := Retriever{
					Source:    source,
					ModelName: modelName,
					Query:     models.Config[ModelName(modelName)].Query,
				}
				defer pool.Close()
				Debug(fmt.Sprintf("Opened connection to %s. Pool stats: \n total conns: %d, ", source.Name, pool.Stat().TotalConns()))
				func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := processPgSource(r, ic, pool); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
			case "mysql":
				r := Retriever{
					Source:    source,
					ModelName: modelName,
					Query:     models.Config[ModelName(modelName)].Query,
				}
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
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				mongoClient, err := MongoConnFromSource(source, ctx)
				if err != nil {
					return err
				}
				r := Retriever{
					Source:    source,
					ModelName: modelName,
					Query:     models.Config[ModelName(modelName)].Query,
				}
				defer mongoClient.Disconnect(context.Background())
				func(r Retriever, ic chan []driver.Value) error {
					g.Go(func() error {
						if err := processMongoSource(r, ic, mongoClient); err != nil {
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

func processPgSource(r Retriever, ic chan []driver.Value, pool *pgxpool.Pool) error {
	Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	rows, err := pool.Query(context.Background(), r.Query)
	if err != nil {
		return err
	}
	defer rows.Close()
	var rowCounter int64

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return err
		}
		rowCounter++
		driverRow := make([]driver.Value, len(values)+1)
		driverRow[0] = r.Source.Name
		for i, value := range values {
			if value == nil {
				driverRow[i+1] = nil
				continue
			}
			if reflect.TypeOf(value).String() == "pgtype.Numeric" {
				val := duckdb.Decimal{Value: value.(pgtype.Numeric).Int, Scale: uint8(math.Abs(float64(value.(pgtype.Numeric).Exp)))}
				driverRow[i+1] = val.Float64()
			} else {
				driverRow[i+1] = value
			}
		}
		ic <- driverRow
	}
	Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ModelName))
	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}

func processMongoSource(r Retriever, ic chan []driver.Value, mongoClient *mongo.Client) error {
	Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	collection := mongoClient.Database(r.Source.Connection.Database).Collection(r.ModelName)
	jsonQuery := make(map[string]interface{})
	if err := json.Unmarshal([]byte(r.Query), &jsonQuery); err != nil {
		Errorf("Error unmarshalling json query: %s", err)
		return err
	}
	bsonQuery, err := bson.Marshal(jsonQuery)
	if err != nil {
		Errorf("Error marshalling json query to BSON: %s", err)
		return err
	}
	cur, err := collection.Find(ctx, bsonQuery)
	if err != nil {
		Errorf("Error executing query: %s", err)
		return err
	}
	if err := cur.Err(); err != nil {
		Errorf("Error iterating cursor: %s", err)
		return err
	}
	defer cur.Close(ctx)
	var rowCounter int64
	for cur.Next(ctx) {
		var result bson.M
		if err := cur.Decode(&result); err != nil {
			Errorf("Error decoding result: %s", err)
			return err
		}
		jsonBytes, err := json.Marshal(result)
		if err != nil {
			Errorf("Error marshalling result: %s", err)
			return err
		}
		rowCounter++
		driverRow := make([]driver.Value, 2)
		driverRow[0] = r.Source.Name
		driverRow[1] = string(jsonBytes)
		ic <- driverRow
	}
	Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ModelName))
	return nil
}
