package engine

import (
	goContext "context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/hyphadb/hyphadb/internal/config"
	internalMongo "github.com/hyphadb/hyphadb/internal/mongo"
	"github.com/hyphadb/hyphadb/internal/pg"
	"github.com/hyphadb/hyphadb/internal/utils"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcboeker/go-duckdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"golang.org/x/sync/errgroup"
)

type Database interface {
	*pgxpool.Pool | *mongo.Client
}

type Retriever[T Database] struct {
	ContextName string
	Query       string
	Source      config.Source
	Client      T
}

func Retrieve(cfg *config.Config, c Context) error {
	for _, contextName := range cfg.Contexts {
		ic := make(chan []driver.Value, 10000)
		dc := make(chan []int64)
		go Insert(contextName, ic, dc)
		if err != nil {
			return err
		}
		g := errgroup.Group{}
		g.SetLimit(200)
		for _, source := range cfg.Sources {
			if !slices.Contains(source.Contexts, contextName) {
				utils.Debug(fmt.Sprintf("Skipping %s for %s", contextName, source.Name))
				continue
			}
			switch source.Engine {
			case "postgres":
				pool, err := pg.PoolFromSource(source)
				if err != nil {
					return err
				}
				r := Retriever[*pgxpool.Pool]{
					Source:      source,
					ContextName: contextName,
					Query:       c.ContextQueries[contextName].Query,
					Client:      pool,
				}
				defer r.Client.Close()
				utils.Debug(fmt.Sprintf("Opened connection to %s. Pool stats: \n total conns: %d, ", source.Name, r.Client.Stat().TotalConns()))
				func(r Retriever[*pgxpool.Pool], ic chan []driver.Value) error {
					g.Go(func() error {
						if err := processPgSource(r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
			case "mongodb":
				ctx, cancel := goContext.WithTimeout(goContext.Background(), 30*time.Second)
				defer cancel()
				mongoClient, err := internalMongo.ConnFromSource(source, ctx)
				if err != nil {
					return err
				}
				r := Retriever[*mongo.Client]{
					Source:      source,
					ContextName: contextName,
					Query:       c.ContextQueries[contextName].Query,
					Client:      mongoClient,
				}
				defer r.Client.Disconnect(goContext.Background())
				func(r Retriever[*mongo.Client], ic chan []driver.Value) error {
					g.Go(func() error {
						if err := processMongoSource(r, ic); err != nil {
							return err
						}
						return nil
					})
					return nil
				}(r, ic)
			default:
				utils.Error(fmt.Sprintf("Engine %s not supported", source.Engine))
			}
		}
		if err = g.Wait(); err != nil {
			return err
		}

		ic <- []driver.Value{"quit"}
		ConfirmInsert(contextName, dc, 0)
	}
	return nil
}

func processPgSource(r Retriever[*pgxpool.Pool], ic chan []driver.Value) error {
	utils.Debug(fmt.Sprintf("Retrieving context %s for %s", r.ContextName, r.Source.Name))
	rows, err := r.Client.Query(goContext.Background(), r.Query)
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
	utils.Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ContextName))
	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}

func processMongoSource(r Retriever[*mongo.Client], ic chan []driver.Value) error {
	utils.Debug(fmt.Sprintf("Retrieving context %s for %s", r.ContextName, r.Source.Name))
	ctx, cancel := goContext.WithTimeout(goContext.Background(), 60*time.Second)
	defer cancel()
	collection := r.Client.Database(r.Source.Connection.Database).Collection(r.ContextName)
	jsonQuery := make(map[string]interface{})
	if err := json.Unmarshal([]byte(r.Query), &jsonQuery); err != nil {
		utils.Errorf("Error unmarshalling json query: %s", err)
		return err
	}
	bsonQuery, err := bson.Marshal(jsonQuery)
	if err != nil {
		utils.Errorf("Error marshalling json query to BSON: %s", err)
		return err
	}
	cur, err := collection.Find(ctx, bsonQuery)
	if err != nil {
		utils.Errorf("Error executing query: %s", err)
		return err
	}
	if err := cur.Err(); err != nil {
		utils.Errorf("Error iterating cursor: %s", err)
		return err
	}
	defer cur.Close(ctx)
	var rowCounter int64
	for cur.Next(ctx) {
		var result bson.M
		if err := cur.Decode(&result); err != nil {
			utils.Errorf("Error decoding result: %s", err)
			return err
		}
		jsonBytes, err := json.Marshal(result)
		if err != nil {
			utils.Errorf("Error marshalling result: %s", err)
			return err
		}
		rowCounter++
		driverRow := make([]driver.Value, 2)
		driverRow[0] = r.Source.Name
		driverRow[1] = string(jsonBytes)
		ic <- driverRow
	}
	utils.Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ContextName))
	return nil
}
