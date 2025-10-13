#!/bin/bash

# Filename: run-clickhouse-indexer.sh

set -e

# Default values
DEFAULT_HOST="localhost"
DEFAULT_PORT=9000

# Parse command line arguments
START_TIME=$1
END_TIME=$2
CLICKHOUSE_HOST=${3:-$DEFAULT_HOST}
CLICKHOUSE_PORT=${4:-$DEFAULT_PORT}
CLICKHOUSE_PASSWORD=${5:-""}

if [ -z "$START_TIME" ] || [ -z "$END_TIME" ]; then
    echo "Usage: $0 <start-time> <end-time> [clickhouse-host] [clickhouse-port] [clickhouse-password]"
    echo "Example: $0 2025-01-01T00:00:00+00:00 2025-02-01T00:00:00+00:00"
    echo "Example: $0 2025-01-01T00:00:00+00:00 2025-02-01T00:00:00+00:00 localhost 9000 mypassword"
    exit 1
fi

# Create a temporary directory for ledger range file
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Extract date from start time for naming files
DATE_PREFIX=$(echo "$START_TIME" | cut -d'T' -f1)
RANGE_FILE="${TEMP_DIR}/${DATE_PREFIX}-exported_range.txt"

echo "Getting ledger range for time period: $START_TIME to $END_TIME"
stellar-etl get_ledger_range_from_times \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --output "$RANGE_FILE"

if [ ! -f "$RANGE_FILE" ]; then
    echo "Error: Ledger range file was not created. Exiting."
    exit 1
fi

# Parse the JSON to extract start and end ledgers
START_LEDGER=$(cat "$RANGE_FILE" | jq -r '.start')
END_LEDGER=$(cat "$RANGE_FILE" | jq -r '.end')

if [ -z "$START_LEDGER" ] || [ -z "$END_LEDGER" ]; then
    echo "Error: Failed to extract ledger range values."
    exit 1
fi

echo "Processing ledger range: $START_LEDGER to $END_LEDGER"
echo "ClickHouse: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo ""

# Build the command with optional password
CMD_ARGS="--start-ledger $START_LEDGER --end-ledger $END_LEDGER --clickhouse-host $CLICKHOUSE_HOST --clickhouse-port $CLICKHOUSE_PORT"

if [ -n "$CLICKHOUSE_PASSWORD" ]; then
    CMD_ARGS="$CMD_ARGS --clickhouse-password $CLICKHOUSE_PASSWORD"
fi

# Run the indexer
/tmp/clickhouse_address_indexer $CMD_ARGS

echo ""
echo "Processing complete!"