#!/bin/bash

### Required: brew install jq, colordiff

export HYPHADB_LOG_LEVEL="ERROR"
APP_DIR=$(dirname "$0")/..
QUERY_TITLE=
COMMAND=
EXPECTED=
DIFF=


QUERY_TITLE="SELECT WITH LIMIT"
COMMAND=$(go run $APP_DIR/cmd/cli/main.go q "SELECT * FROM users LIMIT 10")
EXPECTED=$(cat $APP_DIR/test/data/select_with_limit.json)

echo "Testing query: $QUERY_TITLE"
DIFF=$(diff <(echo "$COMMAND" | jq '.') <(echo "$EXPECTED" | jq '.'))
if [[ -n "$DIFF" ]]; then
    echo "${QUERY_TITLE} query does not match"
    echo -e "\nDiff:"
    echo "$DIFF" | colordiff
else
    echo "${QUERY_TITLE} query matches"
fi


QUERY_TITLE="WHERE CLAUSE"
COMMAND=$(go run $APP_DIR/cmd/cli/main.go q "SELECT * FROM users WHERE first_name = 'Devin'")
EXPECTED=$(cat $APP_DIR/test/data/where.json)

echo "Testing query: $QUERY_TITLE"
DIFF=$(diff <(echo "$COMMAND" | jq '.') <(echo "$EXPECTED" | jq '.'))
if [[ -n "$DIFF" ]]; then
    echo "${QUERY_TITLE} query does not match"
    echo -e "\nDiff:"
    echo "$DIFF" | colordiff
else
    echo "${QUERY_TITLE} query matches"
fi


exit 0
