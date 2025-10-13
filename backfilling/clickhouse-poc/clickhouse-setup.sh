#!/bin/bash

# Filename: clickhouse-setup.sh
# Setup ClickHouse with Docker and create the address_ledgers table

set -e

echo "=========================================="
echo "ClickHouse Docker Setup"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Stop and remove existing ClickHouse container if it exists
if [ "$(docker ps -aq -f name=clickhouse-server)" ]; then
    echo "Removing existing ClickHouse container..."
    docker stop clickhouse-server 2>/dev/null || true
    docker rm clickhouse-server 2>/dev/null || true
fi

# Create directories for ClickHouse data persistence
echo "Creating data directories..."
mkdir -p ~/clickhouse-data/data
mkdir -p ~/clickhouse-data/logs

# Start ClickHouse container
echo ""
echo "Starting ClickHouse container..."
docker run -d \
    --name clickhouse-server \
    --ulimit nofile=262144:262144 \
    -p 8123:8123 \
    -p 9000:9000 \
    -v ~/clickhouse-data/data:/var/lib/clickhouse \
    -v ~/clickhouse-data/logs:/var/log/clickhouse-server \
    clickhouse/clickhouse-server:latest

echo ""
echo "Waiting for ClickHouse to start..."
sleep 10

# Check if ClickHouse is ready
echo "Checking ClickHouse status..."
until docker exec clickhouse-server clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do
    echo "Waiting for ClickHouse to be ready..."
    sleep 2
done

echo ""
echo "✅ ClickHouse is running!"
echo ""

# Create the database
echo "Creating database 'stellar'..."
docker exec clickhouse-server clickhouse-client --query "CREATE DATABASE IF NOT EXISTS stellar"

# Create the table with optimized schema
echo "Creating table 'address_ledgers'..."
docker exec clickhouse-server clickhouse-client --query "
CREATE TABLE IF NOT EXISTS stellar.address_ledgers
(
    address String,
    ledger_sequence UInt32,
    closed_at DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(closed_at)
ORDER BY (address, closed_at, ledger_sequence)
SETTINGS index_granularity = 8192;
"

echo ""
echo "✅ Table created successfully!"
echo ""

# Show table structure
echo "Table structure:"
docker exec clickhouse-server clickhouse-client --query "DESCRIBE stellar.address_ledgers"

echo ""
echo "Table details:"
docker exec clickhouse-server clickhouse-client --query "
SHOW CREATE TABLE stellar.address_ledgers
"

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Connection details:"
echo "  HTTP Port:    8123"
echo "  Native Port:  9000"
echo "  Database:     stellar"
echo "  Table:        address_ledgers"
echo ""
echo "Schema optimizations:"
echo "  - ORDER BY (address, closed_at, ledger_sequence)"
echo "    → Efficient for time-range queries per address"
echo "  - PARTITION BY toYYYYMM(closed_at)"
echo "    → Monthly partitions for easy data lifecycle management"
echo "  - closed_at uses actual ledger close time (not insertion time)"
echo ""
echo "Useful commands:"
echo "  View logs:    docker logs -f clickhouse-server"
echo "  Stop:         docker stop clickhouse-server"
echo "  Start:        docker start clickhouse-server"
echo "  CLI:          docker exec -it clickhouse-server clickhouse-client"
echo "  Remove:       docker rm -f clickhouse-server"
echo ""
echo "Example queries:"
echo ""
echo "  # Count total records"
echo "  docker exec clickhouse-server clickhouse-client --query 'SELECT count() FROM stellar.address_ledgers'"
echo ""
echo "  # Get ledgers for address in last year"
echo "  docker exec clickhouse-server clickhouse-client --query \\"
echo "    \"SELECT ledger_sequence FROM stellar.address_ledgers \\"
echo "    WHERE address = 'GXXX...' \\"
echo "    AND closed_at >= now() - INTERVAL 1 YEAR \\"
echo "    ORDER BY ledger_sequence\""
echo ""
echo "  # Count appearances per address"
echo "  docker exec clickhouse-server clickhouse-client --query \\"
echo "    \"SELECT address, count() as appearances \\"
echo "    FROM stellar.address_ledgers \\"
echo "    GROUP BY address \\"
echo "    ORDER BY appearances DESC \\"
echo "    LIMIT 10\""
echo ""
echo "  # Storage usage"
echo "  docker exec clickhouse-server clickhouse-client --query \\"
echo "    \"SELECT \\"
echo "    formatReadableSize(sum(bytes_on_disk)) as total_size, \\"
echo "    formatReadableSize(sum(data_compressed_bytes)) as compressed, \\"
echo "    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed \\"
echo "    FROM system.parts \\"
echo "    WHERE database = 'stellar' AND table = 'address_ledgers' AND active\""
echo ""