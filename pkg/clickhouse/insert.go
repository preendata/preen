package clickhouse

import (
	"context"
	"fmt"

	"github.com/scalecraft/plex-db/pkg/config"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func Insert(cfg *config.Config, results chan map[string]interface{}) {
	fmt.Println("Inserting results into Clickhouse")
	ctx := context.Background()

	conn, err := ch.Dial(ctx, ch.Options{})
	if err != nil {
		fmt.Println("No connection to Clickhouse")
		panic(err)
	}

	if err := conn.Do(ctx, ch.Query{
		Body: `create table if not exists users (
			id Int32,
			first_name String,
			last_name String,
			email String,
			gender String,
			ip_address String,
			is_active Boolean,
			source String
		) engine = Memory`,
	}); err != nil {
		fmt.Println("Table creation error.")
		panic(err)
	}

	// Define all columns of table.
	var (
		id         proto.ColInt32
		first_name proto.ColBytes
		last_name  proto.ColBytes
		email      proto.ColBytes
		gender     proto.ColBytes
		ip_address proto.ColBytes
		is_active  proto.ColBool
		source     proto.ColBytes
	)
	for {
		message := <-results
		print(message)
		id.Append(message["id"].(int32))
		first_name.AppendBytes([]byte(message["first_name"].(string)))
		last_name.AppendBytes([]byte(message["last_name"].(string)))
		email.AppendBytes([]byte(message["email"].(string)))
		gender.AppendBytes([]byte(message["gender"].(string)))
		ip_address.AppendBytes([]byte(message["ip_address"].(string)))
		is_active.Append(message["is_active"].(bool))
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
			fmt.Println("Unable to insert data.")
			panic(err)

		}
	}
}
