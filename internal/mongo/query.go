package mongo

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/hyphasql/hypha/internal/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func Query(statement string, cfg *config.Config, source config.Source, collection string) (*mongo.Cursor, error) {
	url := fmt.Sprintf(
		"mongodb://%s:%s@%s:%d/?authSource=%s",
		source.Connection.Username,
		url.QueryEscape(source.Connection.Password),
		url.QueryEscape(source.Connection.Host),
		source.Connection.Port,
		source.Connection.AuthSource,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := connect(url, ctx)
	if err != nil {
		return nil, err
	}

	coll := conn.Database(source.Connection.Database).Collection(collection)
	cur, err := coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	return cur, nil
}
