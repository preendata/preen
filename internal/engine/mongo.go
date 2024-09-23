package engine

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func mongoConnFromSource(source Source, ctx context.Context) (*mongo.Client, error) {

	url := fmt.Sprintf(
		"mongodb://%s:%s@%s:%d/?authSource=%s",
		source.Connection.Username,
		url.QueryEscape(source.Connection.Password),
		url.QueryEscape(source.Connection.Host),
		source.Connection.Port,
		source.Connection.AuthSource,
	)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}
	return client, nil
}

func ingestMongoModel(r *Retriever, ic chan []driver.Value) error {
	Debug(fmt.Sprintf("Retrieving context %s for %s", r.ModelName, r.Source.Name))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mongoClient, err := mongoConnFromSource(r.Source, ctx)
	if err != nil {
		return err
	}

	// If this function returns an error, then it failed to disconnect from the mongo client
	defer func() {
		if err = mongoClient.Disconnect(context.Background()); err != nil {
			Errorf("Error disconnecting from mongo: %s", err)
		}
	}()

	defer cancel()

	if err = processMongoDocuments(r, mongoClient, ic); err != nil {
		return err
	}

	return nil
}

func processMongoDocuments(r *Retriever, client *mongo.Client, ic chan []driver.Value) error {
	collection := client.Database(r.Source.Connection.Database).Collection(r.ModelName)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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
