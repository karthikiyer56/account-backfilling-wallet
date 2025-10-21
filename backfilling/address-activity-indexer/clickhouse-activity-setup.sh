#!/bin/bash
# Filename: clickhouse-activity-setup.sh

set -e

# Configurable parameters
CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_HTTP_PORT=${CLICKHOUSE_HTTP_PORT:-8124}
CLICKHOUSE_NATIVE_PORT=${CLICKHOUSE_NATIVE_PORT:-9001}
CLICKHOUSE_DATA_DIR=${CLICKHOUSE_DATA_DIR:-~/clickhouse-activity-data}
CONTAINER_NAME=${CONTAINER_NAME:-clickhouse-activity}
DATABASE_NAME=${DATABASE_NAME:-stellar}

echo "=========================================="
echo "ClickHouse Activity Table Setup"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Host: $CLICKHOUSE_HOST"
echo "  HTTP Port: $CLICKHOUSE_HTTP_PORT"
echo "  Native Port: $CLICKHOUSE_NATIVE_PORT"
echo "  Data Directory: $CLICKHOUSE_DATA_DIR"
echo "  Container Name: $CONTAINER_NAME"
echo "  Database: $DATABASE_NAME"
echo ""

# Function to run ClickHouse queries
run_query() {
    docker exec $CONTAINER_NAME clickhouse-client --query "$1"
}

# Check if we should start a new Docker container
START_DOCKER=${START_DOCKER:-false}

if [ "$START_DOCKER" = "true" ]; then
    echo "Checking Docker..."
    if ! docker info > /dev/null 2>&1; then
        echo "Error: Docker is not running. Please start Docker first."
        exit 1
    fi

    # Stop and remove existing container if it exists
    if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
        echo "Removing existing container $CONTAINER_NAME..."
        docker stop $CONTAINER_NAME 2>/dev/null || true
        docker rm $CONTAINER_NAME 2>/dev/null || true
    fi

    # Create data directories
    echo "Creating data directories at $CLICKHOUSE_DATA_DIR..."
    mkdir -p $CLICKHOUSE_DATA_DIR/data
    mkdir -p $CLICKHOUSE_DATA_DIR/logs
    mkdir -p $CLICKHOUSE_DATA_DIR/config

    # Create no-password configuration file
    echo "Creating no-password configuration..."
    cat > $CLICKHOUSE_DATA_DIR/config/no-password.xml << 'EOF'
<clickhouse>
    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
        </default>
    </users>
</clickhouse>
EOF

    # Start ClickHouse container with config volume
    echo "Starting ClickHouse container..."
    docker run -d \
        --name $CONTAINER_NAME \
        --ulimit nofile=262144:262144 \
        -p $CLICKHOUSE_HTTP_PORT:8123 \
        -p $CLICKHOUSE_NATIVE_PORT:9000 \
        -v $CLICKHOUSE_DATA_DIR/data:/var/lib/clickhouse \
        -v $CLICKHOUSE_DATA_DIR/logs:/var/log/clickhouse-server \
        -v $CLICKHOUSE_DATA_DIR/config/no-password.xml:/etc/clickhouse-server/users.d/no-password.xml \
        clickhouse/clickhouse-server:latest

    echo "Waiting for ClickHouse to start..."
    sleep 10

    # Check if ready
    echo "Checking ClickHouse status..."
    MAX_RETRIES=30
    RETRY_COUNT=0
    until run_query "SELECT 1" > /dev/null 2>&1; do
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
            echo "Error: ClickHouse failed to start after ${MAX_RETRIES} attempts"
            echo "Check logs with: docker logs $CONTAINER_NAME"
            exit 1
        fi
        echo "Waiting for ClickHouse to be ready... (attempt $RETRY_COUNT/$MAX_RETRIES)"
        sleep 2
    done

    echo "✅ ClickHouse is running!"
else
    echo "Using existing ClickHouse instance..."
    # Test connection
    if ! run_query "SELECT 1" > /dev/null 2>&1; then
        echo "Error: Cannot connect to ClickHouse at container '$CONTAINER_NAME'"
        echo "Make sure the container is running and accessible."
        exit 1
    fi
    echo "✅ Connected to existing ClickHouse instance"
fi

echo ""
echo "Creating database '$DATABASE_NAME'..."
run_query "CREATE DATABASE IF NOT EXISTS $DATABASE_NAME"

echo "Creating canonical events table..."
run_query "
CREATE TABLE IF NOT EXISTS ${DATABASE_NAME}.events_canonical
(
    ledger_sequence UInt32,
    closed_at DateTime64(3),
    tx_hash String,
    transaction_index UInt32,
    operation_index UInt32,
    event_index UInt32,
    contract_address String,
    event_type Enum8('transfer' = 1, 'mint' = 2, 'burn' = 3, 'clawback' = 4, 'fee' = 5, 'fee_refund' = 6),
    operation_type Nullable(UInt8),
    from_address String,
    to_address String,
    amount String,
    asset_type Enum8('none' = 0, 'native' = 1, 'issued' = 2),
    asset_code String,
    to_muxed_info_type Enum8('none' = 0, 'text' = 1, 'id' = 2, 'hash' = 3),
    to_muxed_info_text String,
    to_muxed_info_id UInt64,
    to_muxed_info_hash String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(closed_at)
ORDER BY (ledger_sequence, transaction_index, operation_index, event_index)
SETTINGS index_granularity = 8192;
"

echo "Creating materialized view for address lookups..."
run_query "
CREATE MATERIALIZED VIEW IF NOT EXISTS ${DATABASE_NAME}.address_activity
ENGINE = MergeTree()
PARTITION BY toYYYYMM(closed_at)
ORDER BY (address, closed_at, transaction_index, operation_index, event_index)
POPULATE
AS
SELECT
    from_address as address,
    ledger_sequence,
    closed_at,
    tx_hash,
    transaction_index,
    operation_index,
    event_index,
    contract_address,
    event_type,
    operation_type,
    from_address,
    to_address,
    amount,
    asset_type,
    asset_code,
    to_muxed_info_type,
    to_muxed_info_text,
    to_muxed_info_id,
    to_muxed_info_hash
FROM ${DATABASE_NAME}.events_canonical
WHERE from_address != ''

UNION ALL

SELECT
    to_address as address,
    ledger_sequence,
    closed_at,
    tx_hash,
    transaction_index,
    operation_index,
    event_index,
    contract_address,
    event_type,
    operation_type,
    from_address,
    to_address,
    amount,
    asset_type,
    asset_code,
    to_muxed_info_type,
    to_muxed_info_text,
    to_muxed_info_id,
    to_muxed_info_hash
FROM ${DATABASE_NAME}.events_canonical
WHERE to_address != '';
"

echo ""
echo "✅ Tables created successfully!"
echo ""
echo "Canonical table structure:"
run_query "DESCRIBE ${DATABASE_NAME}.events_canonical FORMAT PrettyCompact"

echo ""
echo "Materialized view structure:"
run_query "DESCRIBE ${DATABASE_NAME}.address_activity FORMAT PrettyCompact"

echo ""
echo "Verifying event types..."
run_query "
SELECT name, type
FROM system.columns
WHERE database = '${DATABASE_NAME}'
  AND table = 'events_canonical'
  AND name = 'event_type'
FORMAT PrettyCompact
"

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Tables created:"
echo "  ${DATABASE_NAME}.events_canonical    - Canonical storage (write here)"
echo "  ${DATABASE_NAME}.address_activity    - Auto-populated view (query here)"
echo ""
echo "Event types supported:"
echo "  1 = transfer"
echo "  2 = mint"
echo "  3 = burn"
echo "  4 = clawback"
echo "  5 = fee"
echo "  6 = fee_refund"
echo ""
echo "Schema notes:"
echo "  - Removed: asset_issuer (not needed)"
echo "  - Added: fee_refund event type"
echo "  - PARTITION BY: toYYYYMM(closed_at) for data lifecycle"
echo "  - ORDER BY (canonical): (ledger_sequence, tx, op, event)"
echo "  - ORDER BY (view): (address, closed_at, tx, op, event)"
echo ""
echo "Connection details:"
echo "  HTTP Port:    $CLICKHOUSE_HTTP_PORT"
echo "  Native Port:  $CLICKHOUSE_NATIVE_PORT"
echo "  Database:     $DATABASE_NAME"
echo "  Password:     (none - configured for no password)"
echo ""
if [ "$START_DOCKER" = "true" ]; then
echo "Docker volumes:"
echo "  Data:         $CLICKHOUSE_DATA_DIR/data"
echo "  Logs:         $CLICKHOUSE_DATA_DIR/logs"
echo "  Config:       $CLICKHOUSE_DATA_DIR/config"
echo ""
fi
echo "Usage examples:"
echo ""
echo "  # Use existing ClickHouse"
echo "  CLICKHOUSE_HOST=localhost CLICKHOUSE_NATIVE_PORT=9000 \\"
echo "    CONTAINER_NAME=clickhouse-server DATABASE_NAME=stellar \\"
echo "    ./clickhouse-activity-setup.sh"
echo ""
echo "  # Start new Docker instance"
echo "  START_DOCKER=true CLICKHOUSE_HTTP_PORT=8124 CLICKHOUSE_NATIVE_PORT=9001 \\"
echo "    CLICKHOUSE_DATA_DIR=~/clickhouse-activity-data \\"
echo "    CONTAINER_NAME=clickhouse-activity \\"
echo "    ./clickhouse-activity-setup.sh"
echo ""
echo "Useful commands:"
echo "  View logs:    docker logs -f $CONTAINER_NAME"
echo "  Stop:         docker stop $CONTAINER_NAME"
echo "  Start:        docker start $CONTAINER_NAME"
echo "  CLI:          docker exec -it $CONTAINER_NAME clickhouse-client"
echo "  Remove:       docker rm -f $CONTAINER_NAME"
echo ""