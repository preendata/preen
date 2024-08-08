package main

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hyphadb/hyphadb/internal/config"
	"github.com/hyphadb/hyphadb/internal/utils"
)

func ExecuteRaw(statement string, cfg *config.Config, source config.Source) (*sql.Rows, error) {

	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		source.Connection.Username,
		source.Connection.Password,
		source.Connection.Host,
		source.Connection.Port,
		source.Connection.Database,
	)

	dbpool, err := pool(url)

	if err != nil {
		return nil, err
	}

	defer dbpool.Close()
	utils.Debug("Executing query against Postgres: ", statement)

	rows, err := dbpool.Query(statement)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func pool(url string) (*sql.DB, error) {
	dbPool, err := sql.Open("mysql", url)

	if err != nil {
		slog.Error(
			fmt.Sprintf("Unable to connect to database: %v\n", err),
		)
		return nil, err
	}

	return dbPool, nil
}

func main() {
	// Database connection details
	connStr := "root:thisisnotarealpassword@tcp(127.0.0.1:33061)/mysql_db_1"

	// Open the connection
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Verify connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Successfully connected to the database")

	// Simple query
	query := "SELECT user_id FROM users"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Iterate over the rows
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("ID: %d, Name:", id)
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}
}

// func connect() {
// 	// Database connection details
// 	conn
