#!/bin/bash

set -e

# Parse command line arguments
START_TIME=$1
END_TIME=$2
DB_PATH=$3
DB_MODE=$4  # "new" or "existing"

if [ -z "$START_TIME" ] || [ -z "$END_TIME" ] || [ -z "$DB_PATH" ] || [ -z "$DB_MODE" ]; then
    echo "Usage: $0 <start-time> <end-time> <db-path> <new|existing>"
    echo "Example: $0 2025-01-01T00:00:00+00:00 2025-02-01T00:00:00+00:00 /path/to/rocksdb new"
    exit 1
fi

# Validate DB_MODE
if [ "$DB_MODE" != "new" ] && [ "$DB_MODE" != "existing" ]; then
    echo "Error: DB mode must be 'new' or 'existing'"
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
echo "Database path: $DB_PATH (mode: $DB_MODE)"

# Run the Go program with the extracted ledger range
if [ "$DB_MODE" = "new" ]; then
    /tmp/address_ledger_indexer \
        --start-ledger "$START_LEDGER" \
        --end-ledger "$END_LEDGER" \
        --newdb "$DB_PATH"
else
    /tmp/address_ledger_indexer \
        --start-ledger "$START_LEDGER" \
        --end-ledger "$END_LEDGER" \
        --existingdb "$DB_PATH"
fi

echo "Processing complete. RocksDB updated at: $DB_PATH"