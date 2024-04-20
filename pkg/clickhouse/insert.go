package clickhouse

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/scalecraft/plex-db/pkg/config"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

func CreateHash(value string) string {
	return uuid.NewSHA1(
		uuid.MustParse("2591a156-4deb-4c1b-a20e-98fd44421620"),
		[]byte(value),
	).String()
}

func StreamInsert(cfg *config.Config, results chan map[string]interface{}) {
	slog.Info("Opening a stream of data updates to Clickhouse")
	ctx := context.Background()

	conn := Connect(ctx, cfg)

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

func Insert(cfg *config.Config, results []*pgconn.Result, sourceName string, tableName string) {
	ctx := context.Background()
	fmt.Println(tableName)
	conn := Connect(ctx, cfg)

	if tableName == "users" {
		fmt.Println("Inserting users data into Clickhouse.")
		// Define all columns of table.
		var (
			user_id    proto.ColBytes
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
				ip_hashed := CreateHash(string(row[5]))
				user_id.AppendBytes(row[0])
				first_name.AppendBytes(row[1])
				last_name.AppendBytes(row[2])
				email.AppendBytes([]byte("**redacted**"))
				gender.AppendBytes(row[4])
				ip_address.AppendBytes([]byte(ip_hashed))
				is_active.AppendBytes(row[6])
				source.AppendBytes([]byte(sourceName))
			}

			// Insert single data block.
			input := proto.Input{
				{Name: "user_id", Data: &user_id},
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
				fmt.Println("Unable to insert user data.")
				panic(err)
			}
		}
	}

	if tableName == "transactions" {
		fmt.Println("Inserting transactions data into Clickhouse.")
		// Define all columns of table.
		var (
			transaction_id   proto.ColBytes
			user_id          proto.ColBytes
			product_id       proto.ColBytes
			quantity         proto.ColBytes
			price            proto.ColBytes
			transaction_date proto.ColBytes
			payment_method   proto.ColBytes
			shipping_address proto.ColBytes
			order_status     proto.ColBytes
			discount_code    proto.ColBytes
			source           proto.ColBytes
		)
		for _, result := range results {

			for _, row := range result.Rows {
				transaction_id.AppendBytes(row[0])
				user_id.AppendBytes(row[1])
				product_id.AppendBytes(row[2])
				quantity.AppendBytes(row[3])
				price.AppendBytes(row[4])
				transaction_date.AppendBytes(row[5])
				payment_method.AppendBytes(row[6])
				shipping_address.AppendBytes(row[7])
				order_status.AppendBytes(row[8])
				discount_code.AppendBytes(row[9])
				source.AppendBytes([]byte(sourceName))
			}

			// Insert single data block.
			input := proto.Input{
				{Name: "transaction_id", Data: &transaction_id},
				{Name: "user_id", Data: &user_id},
				{Name: "product_id", Data: &product_id},
				{Name: "quantity", Data: &quantity},
				{Name: "price", Data: &price},
				{Name: "transaction_date", Data: &transaction_date},
				{Name: "payment_method", Data: &payment_method},
				{Name: "shipping_address", Data: &shipping_address},
				{Name: "order_status", Data: &order_status},
				{Name: "discount_code", Data: &discount_code},
				{Name: "source", Data: &source},
			}

			if err := conn.Do(ctx, ch.Query{
				Body:  "insert into transactions values",
				Input: input,
			}); err != nil {
				fmt.Println("Unable to insert transactions data.")
				panic(err)
			}
		}
	}
}
