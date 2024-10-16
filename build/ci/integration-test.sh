#!/bin/bash

docker compose -f build/ci/docker-compose.yaml up -d
sleep 5

bin/preen model build

# Test that the MySQL model was built and can be queried. Query should return 1 row.
MYSQL_RESULTS_LENGTH=$(bin/preen query -f json "select * from mysql_data_types_test;" | jq length)
if [[ $MYSQL_RESULTS_LENGTH -ne 1 ]]; then
  echo "Expected 1 row in mysql_data_types_test, got $MYSQL_RESULTS_LENGTH"
  exit 1
fi

# Test that the PostgreSQL model was built and can be queried. Query should return 1 row.
PG_RESULTS_LENGTH=$(bin/preen query -f json "select * from pg_data_types_test;" | jq length)
if [[ $PG_RESULTS_LENGTH -ne 1 ]]; then
  echo "Expected 1 row in pg_data_types_test, got $PG_RESULTS_LENGTH"
  exit 1
fi

# Test that the MongoDB model was built and can be queried. Query should return 1 row.
MONGO_RESULTS_LENGTH=$(bin/preen query -f json "select * from mongodb_test;" | jq length)
if [[ $MONGO_RESULTS_LENGTH -ne 1 ]]; then
  echo "Expected 1 row in mongodb_test, got $MONGO_RESULTS_LENGTH"
  exit 1
fi


echo "Information schema results from test suite:"
echo $(bin/preen query -f json"select * from preen_information_schema;")