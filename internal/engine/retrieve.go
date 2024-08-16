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
		var totalRows int64
		g := new(errgroup.Group)
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
						rowCount, err := processPgSource(r, ic)
						if err != nil {
							return err
						}
						totalRows += *rowCount
						return nil
					})
					return nil
				}(r, ic)
			case "mongodb":
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
				defer r.Client.Disconnect(context.Background())
				func(r Retriever[*mongo.Client], ic chan []driver.Value) error {
					g.Go(func() error {
						rowCount, err := processMongoSource(r, ic)
						if err != nil {
							return err
						}
						totalRows += *rowCount
						return nil
					})
					return nil
				}(r, ic)
			default:
				utils.Error(fmt.Sprintf("Engine %s not supported", source.Engine))
			}
		}
		err = g.Wait()
		ic <- []driver.Value{"quit"}
		confirmInsert(contextName, dc, totalRows)

		if err != nil {
			return err
		}
	}
	return nil
}

func processPgSource(r Retriever[*pgxpool.Pool], ic chan []driver.Value) (*int64, error) {
	utils.Info(fmt.Sprintf("Retrieving context %s for %s", r.ContextName, r.Source.Name))
	rows, err := r.Client.Query(context.Background(), r.Query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var rowCounter int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
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
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return &rowCounter, nil
}

func processMongoSource(r Retriever[*mongo.Client], ic chan []driver.Value) (*int64, error) {
	utils.Info(fmt.Sprintf("Retrieving context %s for %s", r.ContextName, r.Source.Name))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	collection := r.Client.Database(r.Source.Connection.Database).Collection(r.ContextName)
	jsonQuery := make(map[string]interface{})
	if err := json.Unmarshal([]byte(r.Query), &jsonQuery); err != nil {
		utils.Errorf("Error unmarshalling json query: %s", err)
		return nil, err
	}
	bsonQuery, err := bson.Marshal(jsonQuery)
	if err != nil {
		utils.Errorf("Error marshalling json query to BSON: %s", err)
		return nil, err
	}
	cur, err := collection.Find(ctx, bsonQuery)
	if err != nil {
		utils.Errorf("Error executing query: %s", err)
		return nil, err
	}
	if err := cur.Err(); err != nil {
		utils.Errorf("Error iterating cursor: %s", err)
		return nil, err
	}
	defer cur.Close(ctx)
	var rowCounter int64
	for cur.Next(ctx) {
		var result bson.M
		if err := cur.Decode(&result); err != nil {
			utils.Errorf("Error decoding result: %s", err)
			return nil, err
		}
		jsonBytes, err := json.Marshal(result)
		if err != nil {
			utils.Errorf("Error marshalling result: %s", err)
			return nil, err
		}
		rowCounter++
		driverRow := make([]driver.Value, 2)
		driverRow[0] = r.Source.Name
		driverRow[1] = string(jsonBytes)
		ic <- driverRow
	}
	utils.Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ContextName))
	return &rowCounter, nil
}

func confirmInsert(contextName string, dc chan []int64, rowsExpected int64) {
	for {
		select {
		case message := <-dc:
			if message[0] == rowsExpected {
				utils.Info(fmt.Sprintf("Inserted %d rows into context %s. Expected %d rows", rowsExpected, contextName, rowsExpected))
				return
			}
			if message[0] != rowsExpected {
				utils.Warn(fmt.Sprintf("Inserted %d rows into context %s. Expected %d rows", message[0], contextName, rowsExpected))
				return
			}
		}
	}
}
