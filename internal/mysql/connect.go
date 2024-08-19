package mysql

import (
	"fmt"
	"log/slog"
	"net/url"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/jmoiron/sqlx"
)

func PoolFromSource(source config.Source) (*sqlx.DB, error) {
	// Example url := "root:thisisnotarealpassword@tcp(127.0.0.1:33061)/mysql_db_1"
	url := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
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

func pool(url string) (*sqlx.DB, error) {
	dbPool, err := sqlx.Open("mysql", url)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}

	return dbPool, nil
}
