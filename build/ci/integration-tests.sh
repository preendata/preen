#!/bin/bash

bin/hypha model build

# Test that the MySQL model was built and can be queried. Query should return 1 row.
MYSQL_RESULTS_LENGTH=$(bin/hypha query -f json "select * from mysql_data_types_test;" | jq length)
if [[ $MYSQL_RESULTS_LENGTH -ne 1 ]]; then
  echo "Expected 1 row in mysql_data_types_test, got $MYSQL_RESULTS_LENGTH"
  exit 1
fi

# Test that the PostgreSQL model was built and can be queried. Query should return 1 row.
PG_RESULTS_LENGTH=$(bin/hypha query -f json "select * from pg_data_types_test;" | jq length)
if [[ $PG_RESULTS_LENGTH -ne 1 ]]; then
  echo "Expected 1 row in pg_data_types_test, got $PG_RESULTS_LENGTH"
  exit 1
fi