package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/hyphasql/hypha/internal/config"
	internalMongo "github.com/hyphasql/hypha/internal/mongo"
	"github.com/hyphasql/hypha/internal/mysql"
	"github.com/hyphasql/hypha/internal/pg"
	"github.com/hyphasql/hypha/internal/utils"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcboeker/go-duckdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"golang.org/x/sync/errgroup"
)

type Database interface {
	*pgxpool.Pool | *sql.DB | *mongo.Client
}

type Retriever[T Database] struct {
	ModelName string
	Query     string
	Source    config.Source
	Client    T
}

func Retrieve(cfg *config.Config, models Models) error {
	for _, modelName := range cfg.Models {
		ic := make(chan []driver.Value, 10000)
		dc := make(chan []int64)
		go Insert(modelName, ic, dc)
		if err != nil {
			return err
		}
		g := errgroup.Group{}
		g.SetLimit(200)
		for _, source := range cfg.Sources {
			if !slices.Contains(source.Models, modelName) {
				utils.Debug(fmt.Sprintf("Skipping %s for %s", modelName, source.Name))
				continue
			}
			switch source.Engine {
			case "postgres":
				pool, err := pg.PoolFromSource(source)
				if err != nil {
					return err
				}
				r := Retriever[*pgxpool.Pool]{
					Source:    source,
					ModelName: modelName,
					Query:     models.Config[ModelName(modelName)].Query,
					Client:    pool,
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
			case "mysql":
				pool, err := mysql.PoolFromSource(source)
				if err != nil {
					return err
				}
				r := Retriever[*sql.DB]{
					Source:    source,
					ModelName: modelName,
					Query:     models.Config[ModelName(modelName)].Query,
					Client:    pool,
				}

				defer r.Client.Close()
				func(r Retriever[*sql.DB], ic chan []driver.Value) error {
					g.Go(func() error {
						if err := processMysqlSource(r, ic); err != nil {
							return err
						}
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
					Source:    source,
					ModelName: modelName,
					Query:     models.Config[ModelName(modelName)].Query,
					Client:    mongoClient,
				}
				defer r.Client.Disconnect(context.Background())
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
		ConfirmInsert(modelName, dc, 0)
	}
	return nil
}

func processMysqlSource(r Retriever[*sql.DB], ic chan []driver.Value) error {
	utils.Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	rows, err := r.Client.Query(r.Query)

	if err != nil {
		return err
	}

	defer rows.Close()
	// Get column types and count
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	columnNames, _ := rows.Columns()

	// Create a slice of empty interfaces
	valuePtrs := make([]any, len(columnTypes))

	for i := range columnTypes {
		fmt.Println(columnNames[i], columnTypes[i].ScanType().String())
		valueType := columnTypes[i].ScanType().String()
		switch valueType {
		case "sql.NullString", "string":
			valuePtrs[i] = new(string)
		case "sql.NullInt64", "int64":
			valuePtrs[i] = new(int64)
		case "sql.NullInt32", "int32":
			valuePtrs[i] = new(int32)
		case "sql.NullInt16", "int16":
			valuePtrs[i] = new(int16)
		case "sql.NullByte", "byte":
			valuePtrs[i] = new(byte)
		case "sql.NullFloat64", "float64":
			valuePtrs[i] = new(float64)
		case "sql.NullBool", "bool":
			valuePtrs[i] = new(bool)
		case "sql.NullTime", "time.Time":
			valuePtrs[i] = new(time.Time)
		default:
			utils.Error(fmt.Sprintf("Unsupported type %s", valueType))
		}

	}
	for rows.Next() {
		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}
		driverRow := make([]driver.Value, len(valuePtrs)+1)
		driverRow[0] = r.Source.Name
		for i, ptr := range valuePtrs {
			if ptr == nil {
				driverRow[i+1] = nil
				continue
			}
			driverRow[i+1] = reflect.ValueOf(ptr).Elem().Interface()
		}
		ic <- driverRow
	}

	return nil
}

func processPgSource(r Retriever[*pgxpool.Pool], ic chan []driver.Value) error {
	utils.Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	rows, err := r.Client.Query(context.Background(), r.Query)
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
	utils.Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ModelName))
	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}

func processMongoSource(r Retriever[*mongo.Client], ic chan []driver.Value) error {
	utils.Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	collection := r.Client.Database(r.Source.Connection.Database).Collection(r.ModelName)
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
	utils.Debug(fmt.Sprintf("Retrieved %d rows for %s - %s\n", rowCounter, r.Source.Name, r.ModelName))
	return nil
}
