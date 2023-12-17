package clickhouse

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/scalecraft/plex-db/pkg/config"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func StreamInsert(cfg *config.Config, results chan map[string]interface{}) {
	log.Println("Opening a stream of data updates to Clickhouse")
	ctx := context.Background()

	conn := Connect(ctx, cfg)

	if err := CreateTable(conn, ctx); err != nil {
		log.Panicf("Table creation error: %v", err)
	}

	// Define all columns of table.
	var (
		id         proto.ColBytes
		first_name proto.ColBytes
		last_name  proto.ColBytes
		email      proto.ColBytes
		gender     proto.ColBytes
		ip_address proto.ColBytes
		is_active  proto.ColBytes
		source     proto.ColBytes
	)
	for {
		message := <-results
		print(message)
		id.AppendBytes([]byte(message["id"].(string)))
		first_name.AppendBytes([]byte(message["first_name"].(string)))
		last_name.AppendBytes([]byte(message["last_name"].(string)))
		email.AppendBytes([]byte(message["email"].(string)))
		gender.AppendBytes([]byte(message["gender"].(string)))
		ip_address.AppendBytes([]byte(message["ip_address"].(string)))
		is_active.AppendBytes([]byte(message["is_active"].(string)))
		source.AppendBytes([]byte(message["sourceName"].(string)))

		// Insert single data block.
		input := proto.Input{
			{Name: "id", Data: &id},
			{Name: "first_name", Data: &first_name},
			{Name: "last_name", Data: &last_name},
			{Name: "email", Data: &email},
			{Name: "gender", Data: &gender},
			{Name: "ip_address", Data: &ip_address},
			{Name: "is_active", Data: &is_active},
			{Name: "source", Data: &source},
		}

		if err := conn.Do(ctx, ch.Query{
			Body:  "insert into users values",
			Input: input,
		}); err != nil {
			log.Panicf("Data Insertion Error: %v", err)

		}
	}
}

func Insert(cfg *config.Config, results []*pgconn.Result, sourceName string) {
	log.Println("Inserting snapshot into Clickhouse")
	ctx := context.Background()

	conn := Connect(ctx, cfg)

	if err := CreateTable(conn, ctx); err != nil {
		log.Panicf("Table creation error: %v", err)
	}

	// Define all columns of table.
	var (
		id         proto.ColBytes
		first_name proto.ColBytes
		last_name  proto.ColBytes
		email      proto.ColBytes
		gender     proto.ColBytes
		ip_address proto.ColBytes
		is_active  proto.ColBytes
		source     proto.ColBytes
	)
	for _, result := range results {

		for _, row := range result.Rows {
			id.AppendBytes(row[0])
			first_name.AppendBytes(row[1])
			last_name.AppendBytes(row[2])
			email.AppendBytes(row[3])
			gender.AppendBytes(row[4])
			ip_address.AppendBytes(row[5])
			is_active.AppendBytes(row[6])
			source.AppendBytes([]byte(sourceName))
		}

		// Insert single data block.
		input := proto.Input{
			{Name: "id", Data: &id},
			{Name: "first_name", Data: &first_name},
			{Name: "last_name", Data: &last_name},
			{Name: "email", Data: &email},
			{Name: "gender", Data: &gender},
			{Name: "ip_address", Data: &ip_address},
			{Name: "is_active", Data: &is_active},
			{Name: "source", Data: &source},
		}

		if err := conn.Do(ctx, ch.Query{
			Body:  "insert into users values",
			Input: input,
		}); err != nil {
			fmt.Println("Unable to insert data.")
			panic(err)

		}
	}
}
