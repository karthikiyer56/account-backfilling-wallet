#!/bin/bash
# Filename: run-activity-indexer.sh

set -e

# Default values
DEFAULT_HOST="localhost"
DEFAULT_PORT=9001 # this one is 9001 since my addres->ledger indexer is running on the other port
DEFAULT_DATABASE="stellar"

# Parse arguments
START_TIME=$1
END_TIME=$2
CLICKHOUSE_HOST=${3:-$DEFAULT_HOST}
CLICKHOUSE_PORT=${4:-$DEFAULT_PORT}
CLICKHOUSE_PASSWORD=${5:-""}
DATABASE=${6:-$DEFAULT_DATABASE}

if [ -z "$START_TIME" ] || [ -z "$END_TIME" ]; then
    echo "Usage: $0 <start-time> <end-time> [host] [port] [password] [database]"
    echo ""
    echo "Examples:"
    echo "  $0 2025-01-01T00:00:00+00:00 2025-01-02T00:00:00+00:00"
    echo "  $0 2025-01-01T00:00:00+00:00 2025-01-02T00:00:00+00:00 localhost 9001 '' stellar"
    exit 1
fi

# Get ledger range
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

DATE_PREFIX=$(echo "$START_TIME" | cut -d'T' -f1)
RANGE_FILE="${TEMP_DIR}/${DATE_PREFIX}-range.txt"

echo "Getting ledger range: $START_TIME to $END_TIME"
stellar-etl get_ledger_range_from_times \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --output "$RANGE_FILE"

if [ ! -f "$RANGE_FILE" ]; then
    echo "Error: Failed to get ledger range"
    exit 1
fi

START_LEDGER=$(cat "$RANGE_FILE" | jq -r '.start')
END_LEDGER=$(cat "$RANGE_FILE" | jq -r '.end')

if [ -z "$START_LEDGER" ] || [ -z "$END_LEDGER" ]; then
    echo "Error: Failed to parse ledger range"
    exit 1
fi

echo "Ledger range: $START_LEDGER to $END_LEDGER"
echo "ClickHouse: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo "Database: $DATABASE"
echo ""

# Build command
CMD_ARGS="--start-ledger $START_LEDGER --end-ledger $END_LEDGER"
CMD_ARGS="$CMD_ARGS --clickhouse-host $CLICKHOUSE_HOST"
CMD_ARGS="$CMD_ARGS --clickhouse-port $CLICKHOUSE_PORT"
CMD_ARGS="$CMD_ARGS --database $DATABASE"

if [ -n "$CLICKHOUSE_PASSWORD" ]; then
    CMD_ARGS="$CMD_ARGS --clickhouse-password $CLICKHOUSE_PASSWORD"
fi

# Run indexer

echo "Command line args - $CMD_ARGS"
/tmp/clickhouse_activity_indexer $CMD_ARGS

echo ""
echo "✅ Processing complete!"