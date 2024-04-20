package clickhouse

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/scalecraft/plex-db/pkg/config"
)

var typeMap = map[string]string{
	"character varying": "String",
	"smallint":          "Int16",
	"integer":           "Int32",
	"bigint":            "Int64",
	"boolean":           "Boolean",
	"date":              "Date",
	"timestamp":         "DateTime",
	"jsonb":             "String",
	"json":              "JSON",
	"numeric":           "Float",
	"double precision":  "Float",
	"real":              "Float",
}

func CreateTable(cfg *config.Config) error {

	ctx := context.Background()
	conn := Connect(ctx, cfg)
	err := conn.Do(ctx, ch.Query{
		Body: `create table if not exists users (
			user_id String,
			first_name String,
			last_name String,
			email String,
			gender String,
			ip_address String,
			is_active String,
			source String
		) engine = Memory`,
	})

	if err != nil {
		return err
	}
	return nil
}

func CreateUsersTable(cfg *config.Config) error {

	ctx := context.Background()
	conn := Connect(ctx, cfg)
	err := conn.Do(ctx, ch.Query{
		Body: `create table if not exists users (
			user_id String,
			first_name String,
			last_name String,
			email String,
			gender String,
			ip_address String,
			is_active String,
			source String
		) engine = Memory`,
	})

	if err != nil {
		return err
	}
	return nil
}

func CreateTransactionsTable(cfg *config.Config) error {
	ctx := context.Background()
	conn := Connect(ctx, cfg)
	err := conn.Do(ctx, ch.Query{
		Body: `create table if not exists transactions (
			transaction_id String,
			user_id String,
			product_id String,
			quantity String,
			price String,
			transaction_date String,
			payment_method String,
			shipping_address String,
			order_status String,
			discount_code String,
			source String
		) engine = Memory`,
	})

	if err != nil {
		return err
	}
	return nil
}
