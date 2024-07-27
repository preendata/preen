#!/bin/bash

input_file="../data/transactions/mock-transaction-data-1.csv"
output_file="./data/transactions/mock-transaction-data-1.json"

if [ ! -f "$input_file" ]; then
    echo "Input file not found: $input_file"
    exit 1
fi

headers=("transaction_id" "user_id" "product_id" "quantity" "price" "transaction_date" "payment_method" "shipping_address" "order_status" "discount_code")

json_array=()

while IFS=',' read -r -a line
do
    json_object="{"
    for i in "${!headers[@]}"
    do
        if [ "${headers[i]}" = "is_active" ]; then
            if [ "${line[i]}" = "true" ] || [ "${line[i]}" = "TRUE" ] || [ "${line[i]}" = "1" ]; then
                json_object+="\"${headers[i]}\":true,"
            else
                json_object+="\"${headers[i]}\":false,"
            fi
        else
            json_object+="\"${headers[i]}\":\"${line[i]}\","
        fi
    done
    json_object="${json_object%,}}"
    json_array+=("$json_object")
done < "$input_file"

json_output="["
for json_object in "${json_array[@]}"
do
    json_output+="$json_object,"
done
json_output="${json_output%,}]"

echo "$json_output" > "$output_file"

echo "Conversion complete. JSON data saved to $output_file"