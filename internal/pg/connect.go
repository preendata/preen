package pg

import (
	"context"
	"fmt"
	"net/url"

	"github.com/hyphasql/hypha/internal/config"
	"github.com/hyphasql/hypha/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The connect function takes a database url as a string and returns the
// pgx.Conn object for use in querying the database
func connect(url string) (*pgx.Conn, error) {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	connection, err := pgx.Connect(context.Background(), url)

	if err != nil {
		utils.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}
	return connection, nil
}

func pool(url string) (*pgxpool.Pool, error) {
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	dbpool, err := pgxpool.New(context.Background(), url)

	if err != nil {
		utils.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}
	return dbpool, nil
}

func PoolFromSource(source config.Source) (*pgxpool.Pool, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		url.QueryEscape(source.Connection.Password),
		url.QueryEscape(source.Connection.Host),
		source.Connection.Port,
		source.Connection.Database,
	)
	dbpool, err := pool(url)

	if err != nil {
		return nil, err
	}

	return dbpool, nil
}

func ConnFromSource(source config.Source) (*pgx.Conn, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		url.QueryEscape(source.Connection.Password),
		url.QueryEscape(source.Connection.Host),
		source.Connection.Port,
		source.Connection.Database,
	)

	conn, err := connect(url)

	if err != nil {
		return nil, err
	}

	return conn, nil
}
