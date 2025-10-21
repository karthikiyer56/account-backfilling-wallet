# Stellar Activity Indexer for ClickHouse

A high-performance indexer for Stellar blockchain token transfer events, designed to enable efficient address-based activity queries with cursor-based pagination.

## Overview

This indexer processes Stellar ledgers from Google Cloud Storage (GCS) and stores token transfer events in ClickHouse. It uses a materialized view pattern to enable sub-10ms queries for address activity history.

### Architecture
```
Stellar Ledgers (GCS) → Go Indexer → ClickHouse
                                       ├── events_canonical (write once)
                                       └── address_activity (materialized view, query here)
```

**Key Features:**
- **Write once, query fast**: Insert events to canonical table, automatically duplicated to address-indexed view
- **Cursor-based pagination**: Stable, efficient pagination for mobile apps
- **Performance metrics**: Track GCS fetch, event processing, and DB write times
- **Monthly partitioning**: Easy data lifecycle management (drop old months)
- **Sub-10ms queries**: Optimized for address-based lookups

---

## Directory Structure
```
.
├── README.md                          # This file
├── clickhouse-activity-setup.sh       # Setup ClickHouse tables/views
├── clickhouse_activity_indexer.go     # Indexer (processes ledgers → ClickHouse)
└── run-activity-indexer.sh            # Helper script to run indexer with date ranges
```

---

## Prerequisites

### Required
- **Docker**: For running ClickHouse
- **Go 1.21+**: For building the indexer
- **stellar-etl**: For converting date ranges to ledger ranges
```bash
  go install github.com/stellar/stellar-etl/cmd/stellar-etl@latest
```

### Go Dependencies
```bash
go get github.com/ClickHouse/clickhouse-go/v2
go get github.com/stellar/go/ingest
go get github.com/stellar/go/ingest/ledgerbackend
go get github.com/stellar/go/network
go get github.com/stellar/go/processors/token_transfer
go get github.com/stellar/go/strkey
go get github.com/stellar/go/support/datastore
go get github.com/stellar/go/support/errors
go get github.com/stellar/go/xdr
```

---

## Quick Start

### 1. Setup ClickHouse

**Option A: Start new ClickHouse instance**
```bash
START_DOCKER=true \
  CLICKHOUSE_HTTP_PORT=8123 \
  CLICKHOUSE_NATIVE_PORT=9000 \
  CLICKHOUSE_DATA_DIR=~/clickhouse-activity-data \
  CONTAINER_NAME=clickhouse-activity \
  DATABASE_NAME=stellar \
  ./clickhouse-activity-setup.sh
```

**Option B: Use existing ClickHouse instance**
```bash
CLICKHOUSE_HOST=localhost \
  CLICKHOUSE_NATIVE_PORT=9000 \
  CONTAINER_NAME=clickhouse-server \
  DATABASE_NAME=stellar \
  ./clickhouse-activity-setup.sh
```

This creates:
- `stellar.events_canonical` - Canonical event storage (write here)
- `stellar.address_activity` - Materialized view (query here)

### 2. Build the Indexer
```bash
go build -o clickhouse_activity_indexer clickhouse_activity_indexer.go
```

### 3. Run the Indexer

**Using date range (recommended):**
```bash
./run-activity-indexer.sh \
  "2025-01-01T00:00:00+00:00" \
  "2025-01-02T00:00:00+00:00"
```

**Direct invocation with ledger range:**
```bash
./clickhouse_activity_indexer \
  --start-ledger 50000000 \
  --end-ledger 50010000 \
  --clickhouse-host localhost \
  --clickhouse-port 9000 \
  --database stellar
```

---

## Schema Details

### events_canonical Table

**Purpose**: Single source of truth for all events. Write here, never query directly.
```sql
CREATE TABLE stellar.events_canonical (
    ledger_sequence UInt32,
    closed_at DateTime64(3),
    tx_hash String,
    transaction_index UInt32,
    operation_index UInt32,
    event_index UInt32,
    contract_address String,
    event_type Enum8('transfer'=1, 'mint'=2, 'burn'=3, 'clawback'=4, 'fee'=5, 'fee_refund'=6),
    operation_type Nullable(UInt8),
    from_address String,
    to_address String,
    amount String,
    asset_type Enum8('none'=0, 'native'=1, 'issued'=2),
    asset_code String,
    to_muxed_info_type Enum8('none'=0, 'text'=1, 'id'=2, 'hash'=3),
    to_muxed_info_text String,
    to_muxed_info_id UInt64,
    to_muxed_info_hash String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(closed_at)
ORDER BY (ledger_sequence, transaction_index, operation_index, event_index);
```

**Key Design Decisions:**
- `PARTITION BY toYYYYMM(closed_at)`: Monthly partitions for data lifecycle (drop old months)
- `ORDER BY (ledger, tx, op, event)`: Natural chronological order, efficient writes
- No `PRIMARY KEY`: Uses ORDER BY as implicit primary key

### address_activity Materialized View

**Purpose**: Query-optimized view for address-based lookups. Query here.
```sql
CREATE MATERIALIZED VIEW stellar.address_activity
ENGINE = MergeTree()
PARTITION BY toYYYYMM(closed_at)
ORDER BY (address, closed_at, transaction_index, operation_index, event_index)
AS
SELECT 
    from_address as address, ...
FROM stellar.events_canonical
WHERE from_address != ''
UNION ALL
SELECT 
    to_address as address, ...
FROM stellar.events_canonical
WHERE to_address != '';
```

**Key Design Decisions:**
- `ORDER BY (address, closed_at, tx, op, event)`: Optimized for `WHERE address = 'X'` queries
- `address` first: Enables index-based lookup (10ms vs 30s table scan)
- `closed_at` second: Efficient time-range filtering and DESC sorting
- `(tx, op, event)` for uniqueness: Stable cursor pagination
- **Why no `ledger_sequence` in ORDER BY?**: It's 1:1 with `closed_at` (redundant), but kept in SELECT for display

---

## Performance Metrics

The indexer tracks and displays performance metrics every 1000 ledgers:
```
┌─────────────────────────────────────────────────────────────┐
│                   BATCH PERFORMANCE METRICS                 │
├─────────────────────────────────────────────────────────────┤
│ Ledger Fetch (GCS):                                         │
│   Avg Time:          16.93 ms/ledger                        │
│   Samples:            1000 ledgers                           │
│   Throughput:         59.1 ledgers/sec                      │
│                                                             │
│ Event Processing:                                           │
│   Avg Time:           2.91 ms/ledger                        │
│   Samples:            1000 ledgers                           │
│   Avg Events:          0.0 events/ledger                    │
│   Throughput:        344.1 ledgers/sec                      │
│                                                             │
│ Database Write (Batch):                                     │
│   Avg Time:           0.00 ms/batch                         │
│   Samples:               6 batches                           │
│   This Batch:       522402 rows inserted                     │
│                                                             │
│ Combined Performance:                                       │
│   Total Avg:         19.84 ms/ledger (fetch+process+write) │
│   Throughput:         50.4 ledgers/sec                      │
│   Breakdown:      85.4% fetch, 14.6% process, 0.0% write    │
└─────────────────────────────────────────────────────────────┘
```

**Metrics Tracked:**
1. **Ledger Fetch**: Time to download ledger from GCS (averaged over 1000 ledgers)
2. **Event Processing**: Time to extract events from ledger (averaged over 1000 ledgers)
3. **Database Write**: Time to insert batch to ClickHouse (averaged over batches)
4. **Combined**: Total time per ledger with breakdown percentages

**Use metrics to identify bottlenecks:**
- High fetch time → Network/GCS issue
- High process time → CPU bottleneck
- High write time → ClickHouse performance issue

---

## Query Patterns

### Recent Activities (20 at a time)
```go
// First request (no cursor)
response, err := GetRecentActivities(ctx, conn, "GABC...", 20, nil)

// Response includes cursor
{
  "data": [20 activities],
  "next_cursor": "eyJjbG9zZWRfYXQi...",
  "has_more": true
}

// Second request (with cursor)
response, err := GetRecentActivities(ctx, conn, "GABC...", 20, &response.NextCursor)
```

**SQL executed:**
```sql
-- First page
SELECT * FROM address_activity
WHERE address = 'GABC...'
ORDER BY closed_at DESC, ledger_sequence DESC, transaction_index DESC, 
         operation_index DESC, event_index DESC
LIMIT 20;

-- Second page (with cursor)
SELECT * FROM address_activity
WHERE address = 'GABC...'
  AND (closed_at, transaction_index, operation_index, event_index) < (cursor_values)
ORDER BY closed_at DESC, ledger_sequence DESC, transaction_index DESC,
         operation_index DESC, event_index DESC
LIMIT 20;
```

### Monthly Activities
```go
// October 2025 activities
response, err := GetMonthlyActivities(ctx, conn, "GABC...", 202510, 20, nil)
```

**SQL executed:**
```sql
SELECT * FROM address_activity
WHERE address = 'GABC...'
  AND toYYYYMM(closed_at) = 202510
ORDER BY closed_at DESC, ...
LIMIT 20;
```

**Benefits of monthly partitioning:**
- Only scans October partition (skips other 11 months)
- ~10x faster than scanning entire year

### Date Range Activities
```go
// Last 7 days
startDate := time.Now().AddDate(0, 0, -7)
endDate := time.Now()
response, err := GetActivitiesInRange(ctx, conn, "GABC...", startDate, endDate, 20, nil)
```

---

## Cursor-Based Pagination

### How It Works

1. **First Request**: No cursor, returns first 20 rows
2. **Extract Cursor**: From last row (row #20), extract `(closed_at, tx_index, op_index, event_index)`
3. **Encode Cursor**: Base64-encode as token
4. **Next Request**: Include cursor token
5. **Query**: Find rows "less than" cursor (older in DESC order)

### Cursor Structure
```go
type Cursor struct {
    ClosedAt         time.Time  // Timestamp of last seen event
    TransactionIndex uint32     // Transaction index within ledger
    OperationIndex   uint32     // Operation index within transaction
    EventIndex       uint32     // Event index within operation (for multiple events/op)
}
```

**Why these 4 fields?**
- `closed_at`: Primary sort key (time-based)
- `transaction_index, operation_index, event_index`: Ensure uniqueness (same address can have multiple events in same ledger)

**Why no `ledger_sequence`?**
- It's 1:1 with `closed_at` (redundant)
- Including it would make cursor 20% larger with no benefit

### Tuple Comparison

ClickHouse compares tuples lexicographically:
```sql
(closed_at, tx, op, event) < ('2025-10-20T10:15:00', 8, 3, 0)
```

Equivalent to:
```sql
closed_at < '2025-10-20T10:15:00'
OR (closed_at = '2025-10-20T10:15:00' AND transaction_index < 8)
OR (closed_at = '2025-10-20T10:15:00' AND transaction_index = 8 AND operation_index < 3)
OR (closed_at = '2025-10-20T10:15:00' AND transaction_index = 8 AND operation_index = 3 AND event_index < 0)
```

But much more efficient!

---

## Event Types

| Type | Value | Description | Fields |
|------|-------|-------------|--------|
| `transfer` | 1 | Transfer from one address to another | `from_address`, `to_address`, `amount` |
| `mint` | 2 | Create new tokens | `to_address`, `amount` |
| `burn` | 3 | Destroy tokens | `from_address`, `amount` |
| `clawback` | 4 | Issuer reclaims tokens | `from_address`, `amount` |
| `fee` | 5 | Transaction fee | `from_address`, `amount` |
| `fee_refund` | 6 | Fee refund (future) | TBD |

**Note**: `fee_refund` is supported in schema but not yet emitted by `stellar/go` processor.

---

## Configuration

### Environment Variables

**ClickHouse Setup:**
- `START_DOCKER`: Set to `true` to start new ClickHouse container
- `CLICKHOUSE_HOST`: ClickHouse host (default: `localhost`)
- `CLICKHOUSE_HTTP_PORT`: HTTP port (default: `8123`)
- `CLICKHOUSE_NATIVE_PORT`: Native protocol port (default: `9000`)
- `CLICKHOUSE_DATA_DIR`: Data directory for Docker volumes (default: `~/clickhouse-activity-data`)
- `CONTAINER_NAME`: Docker container name (default: `clickhouse-activity`)
- `DATABASE_NAME`: Database name (default: `stellar`)

**Indexer:**
- `--start-ledger`: Starting ledger sequence (required)
- `--end-ledger`: Ending ledger sequence (required)
- `--clickhouse-host`: ClickHouse host (default: `localhost`)
- `--clickhouse-port`: ClickHouse native port (default: `9000`)
- `--clickhouse-password`: ClickHouse password (default: empty)
- `--database`: Database name (default: `stellar`)

---

## Data Lifecycle Management

### Drop Old Partitions
```sql
-- Drop January 2025 partition (instant, no table scan)
ALTER TABLE stellar.events_canonical DROP PARTITION '202501';
ALTER TABLE stellar.address_activity DROP PARTITION '202501';
```

### Check Partition Sizes
```sql
SELECT
    partition,
    formatReadableSize(sum(bytes_on_disk)) as size,
    formatReadableQuantity(sum(rows)) as rows
FROM system.parts
WHERE database = 'stellar' 
  AND table IN ('events_canonical', 'address_activity')
  AND active
GROUP BY partition
ORDER BY partition DESC;
```

---

## Troubleshooting

### ClickHouse Won't Start
```bash
# Check logs
docker logs clickhouse-activity

# Common issues:
# 1. Port already in use → Change CLICKHOUSE_NATIVE_PORT
# 2. Permission issues → Check CLICKHOUSE_DATA_DIR permissions
# 3. Config errors → Verify no-password.xml was created
```

### Indexer Can't Connect
```bash
# Test ClickHouse connection
docker exec clickhouse-activity clickhouse-client --query "SELECT 1"

# Should return: 1

# If fails, check:
# 1. Container is running: docker ps | grep clickhouse-activity
# 2. Port is correct: echo $CLICKHOUSE_NATIVE_PORT
# 3. No password configured correctly
```

### Slow Queries
```sql
-- Check if query uses index
EXPLAIN indexes = 1
SELECT * FROM address_activity
WHERE address = 'GABC...'
ORDER BY closed_at DESC
LIMIT 20;

-- Should show: "Index: primary key (used)"
```

### Duplicate Events

The indexer uses `ORDER BY` as deduplication key. If you re-process the same ledger:
```sql
-- Check for duplicates
SELECT 
    ledger_sequence, 
    transaction_index, 
    operation_index, 
    event_index,
    count(*) as cnt
FROM events_canonical
GROUP BY ledger_sequence, transaction_index, operation_index, event_index
HAVING cnt > 1;
```

If duplicates exist, they'll be automatically deduplicated during background merges.

---

## Performance Benchmarks

### Expected Throughput

**Indexing:**
- 40-50 ledgers/sec (typical)
- 2,000-5,000 events/sec (typical)
- 50,000+ rows/sec insert rate

**Querying:**
- Recent 20 activities: ~5-10ms
- Monthly activities: ~10-20ms
- 100 pages of pagination: ~1-2 seconds total

### Storage Estimates

**Per year of data:**
- `events_canonical`: ~500GB compressed
- `address_activity`: ~1TB compressed (2x due to duplication)
- **Total**: ~1.5TB/year

**Partition sizes** (typical):
- ~125GB per month compressed
- ~10-15x compression ratio

---

## Production Recommendations

### Indexing Strategy

1. **Initial backfill**: Process in 1-month chunks
```bash
   ./run-activity-indexer.sh "2024-01-01T00:00:00Z" "2024-02-01T00:00:00Z"
   ./run-activity-indexer.sh "2024-02-01T00:00:00Z" "2024-03-01T00:00:00Z"
   # etc.
```

2. **Ongoing updates**: Process daily or hourly
```bash
   # Process yesterday
   ./run-activity-indexer.sh "$(date -d 'yesterday' -u +%Y-%m-%dT00:00:00Z)" "$(date -u +%Y-%m-%dT00:00:00Z)"
```

3. **Monitor metrics**: Watch for performance degradation


### Backup Strategy
```bash
# Backup specific partition
docker exec clickhouse-activity clickhouse-client --query \
  "BACKUP TABLE stellar.events_canonical PARTITION '202510' TO Disk('backups', '202510.zip')"

# Restore partition
docker exec clickhouse-activity clickhouse-client --query \
  "RESTORE TABLE stellar.events_canonical PARTITION '202510' FROM Disk('backups', '202510.zip')"
```

---

## API Integration Example
```go
// Example HTTP handler
func GetAddressActivitiesHandler(w http.ResponseWriter, r *http.Request) {
    address := r.URL.Query().Get("address")
    cursor := r.URL.Query().Get("cursor")
    
    var cursorPtr *string
    if cursor != "" {
        cursorPtr = &cursor
    }
    
    response, err := GetRecentActivities(r.Context(), conn, address, 20, cursorPtr)
    if err != nil {
        http.Error(w, err.Error(), 500)
        return
    }
    
    json.NewEncoder(w).Encode(response)
}

// Response format:
// {
//   "data": [...],
//   "next_cursor": "eyJjbG9zZWRfYXQi...",
//   "has_more": true
// }
```

---

## Running Queries with Docker Exec

You can run queries directly on the ClickHouse container without writing Go code.

### Basic Syntax
```bash
docker exec <container-name> clickhouse-client --query "SQL QUERY HERE"
```

---

## Count Queries

### Count Total Events for an Address
```bash
# Count all events for an address
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as total_events
FROM stellar.address_activity
WHERE address = 'GABC123...'
"
```

### Count Events for Address in Date Range
```bash
# Count events between specific dates
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as total_events
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
"
```

### Count Events for Address in Specific Month
```bash
# Count events in October 2025
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as total_events
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND toYYYYMM(closed_at) = 202510
"
```

### Count All Events in Date Range
```bash
# Count total events across all addresses
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as total_events
FROM stellar.events_canonical
WHERE closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
"
```

### Count Events by Type for an Address
```bash
# Breakdown by event type
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    event_type,
    count(*) as count
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
GROUP BY event_type
ORDER BY count DESC
FORMAT PrettyCompact
"
```

---

## Aggregation Queries

### Count Events Per Day for an Address
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    toDate(closed_at) as date,
    count(*) as events
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
GROUP BY date
ORDER BY date DESC
FORMAT PrettyCompact
"
```

### Total Amount Transferred by an Address
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    sum(toFloat64OrZero(amount)) as total_amount,
    asset_type,
    asset_code
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND event_type = 'transfer'
  AND from_address = 'GABC123...'
  AND closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
GROUP BY asset_type, asset_code
FORMAT PrettyCompact
"
```

### Most Active Addresses in Date Range
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    address,
    count(*) as activity_count
FROM stellar.address_activity
WHERE closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
GROUP BY address
ORDER BY activity_count DESC
LIMIT 20
FORMAT PrettyCompact
"
```

---

## Time-Based Queries

### Events Per Hour for an Address (Last 24 Hours)
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    toStartOfHour(closed_at) as hour,
    count(*) as events
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND closed_at >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC
FORMAT PrettyCompact
"
```

### First and Last Activity for an Address
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    min(closed_at) as first_activity,
    max(closed_at) as last_activity,
    dateDiff('day', min(closed_at), max(closed_at)) as active_days,
    count(*) as total_events
FROM stellar.address_activity
WHERE address = 'GABC123...'
FORMAT PrettyCompact
"
```

---

## Table Statistics

### Total Events and Storage Size
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    table,
    formatReadableQuantity(sum(rows)) as total_rows,
    formatReadableSize(sum(bytes_on_disk)) as disk_size,
    formatReadableSize(sum(data_compressed_bytes)) as compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) as compression_ratio
FROM system.parts
WHERE database = 'stellar'
  AND table IN ('events_canonical', 'address_activity')
  AND active
GROUP BY table
FORMAT PrettyCompact
"
```

### Events Per Partition (Per Month)
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    partition,
    formatReadableQuantity(sum(rows)) as events,
    formatReadableSize(sum(bytes_on_disk)) as size
FROM system.parts
WHERE database = 'stellar'
  AND table = 'address_activity'
  AND active
GROUP BY partition
ORDER BY partition DESC
FORMAT PrettyCompact
"
```

### Unique Addresses Count
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    formatReadableQuantity(count(DISTINCT address)) as unique_addresses
FROM stellar.address_activity
"
```

---

## Activity Distribution Queries

### Event Type Distribution (All Time)
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    event_type,
    formatReadableQuantity(count(*)) as count,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 2) as percentage
FROM stellar.events_canonical
GROUP BY event_type
ORDER BY count DESC
FORMAT PrettyCompact
"
```

### Asset Distribution for an Address
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    asset_type,
    asset_code,
    count(*) as transactions,
    round(count(*) * 100.0 / sum(count(*)) OVER (), 2) as percentage
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND asset_type != 'none'
GROUP BY asset_type, asset_code
ORDER BY transactions DESC
LIMIT 10
FORMAT PrettyCompact
"
```

### Activity Frequency Distribution
```bash
# How many addresses have 1 event, 2-10 events, etc.
docker exec clickhouse-activity clickhouse-client --query "
WITH address_counts AS (
    SELECT 
        address,
        count(*) as event_count
    FROM stellar.address_activity
    WHERE closed_at >= '2025-01-01 00:00:00'
      AND closed_at < '2025-02-01 00:00:00'
    GROUP BY address
)
SELECT 
    CASE
        WHEN event_count = 1 THEN '1 event'
        WHEN event_count <= 10 THEN '2-10 events'
        WHEN event_count <= 100 THEN '11-100 events'
        WHEN event_count <= 1000 THEN '101-1,000 events'
        ELSE '1,000+ events'
    END as frequency_range,
    formatReadableQuantity(count(*)) as address_count
FROM address_counts
GROUP BY frequency_range
ORDER BY min(event_count)
FORMAT PrettyCompact
"
```

---

## Sample Data Queries

### Get Last 5 Events for an Address
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    closed_at,
    event_type,
    from_address,
    to_address,
    amount,
    asset_type,
    asset_code
FROM stellar.address_activity
WHERE address = 'GABC123...'
ORDER BY closed_at DESC, ledger_sequence DESC
LIMIT 5
FORMAT PrettyCompact
"
```

### Get All Transfers Between Two Addresses
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    closed_at,
    amount,
    asset_type,
    asset_code,
    ledger_sequence,
    tx_hash
FROM stellar.address_activity
WHERE event_type = 'transfer'
  AND (
    (from_address = 'GABC123...' AND to_address = 'GDEF456...')
    OR
    (from_address = 'GDEF456...' AND to_address = 'GABC123...')
  )
ORDER BY closed_at DESC
LIMIT 20
FORMAT PrettyCompact
"
```

---

## Performance Testing Queries

### Query Performance Test
```bash
# Test query speed for an address
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as result
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND closed_at >= '2025-01-01 00:00:00'
SETTINGS max_threads = 1
FORMAT PrettyCompact
" --time
```

The `--time` flag shows execution time.

### Check If Index Is Used
```bash
docker exec clickhouse-activity clickhouse-client --query "
EXPLAIN indexes = 1
SELECT * FROM stellar.address_activity
WHERE address = 'GABC123...'
ORDER BY closed_at DESC
LIMIT 20
"
```

Should show: `Index: primary key (used)`

---

## Helper Bash Functions

Add these to your `~/.bashrc` or `~/.zshrc` for convenience:
```bash
# Query ClickHouse activity table
chquery() {
    docker exec clickhouse-activity clickhouse-client --query "$1"
}

# Count events for address
ch_count_address() {
    local address=$1
    chquery "SELECT count(*) FROM stellar.address_activity WHERE address = '$address'"
}

# Count events in date range
ch_count_range() {
    local start=$1
    local end=$2
    chquery "SELECT count(*) FROM stellar.events_canonical WHERE closed_at >= '$start' AND closed_at < '$end'"
}

# Last 5 events for address
ch_last_events() {
    local address=$1
    chquery "
    SELECT closed_at, event_type, amount, asset_type 
    FROM stellar.address_activity 
    WHERE address = '$address' 
    ORDER BY closed_at DESC 
    LIMIT 5 
    FORMAT PrettyCompact"
}
```

**Usage:**
```bash
# After adding to ~/.bashrc
source ~/.bashrc

ch_count_address "GABC123..."
ch_count_range "2025-01-01 00:00:00" "2025-02-01 00:00:00"
ch_last_events "GABC123..."
```

---

## Output Formats

ClickHouse supports multiple output formats:
```bash
# Pretty table format (default)
FORMAT PrettyCompact

# JSON
FORMAT JSON

# CSV
FORMAT CSV

# Tab-separated
FORMAT TSV

# Vertical (one column per line)
FORMAT Vertical
```

**Example:**
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT * FROM stellar.address_activity WHERE address = 'GABC123...' LIMIT 1
FORMAT JSON
"
```

---

## Tips

### Use Variables for Common Queries
```bash
# Set variables
ADDRESS="GABC123..."
START_DATE="2025-01-01 00:00:00"
END_DATE="2025-02-01 00:00:00"

# Use in query
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) 
FROM stellar.address_activity 
WHERE address = '$ADDRESS'
  AND closed_at >= '$START_DATE'
  AND closed_at < '$END_DATE'
"
```

### Multi-line Queries for Readability
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    toDate(closed_at) as date,
    event_type,
    count(*) as events
FROM stellar.address_activity
WHERE address = '$ADDRESS'
  AND closed_at >= '$START_DATE'
  AND closed_at < '$END_DATE'
GROUP BY date, event_type
ORDER BY date DESC, events DESC
FORMAT PrettyCompact
"
```

### Save Query Results to File
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT * FROM stellar.address_activity WHERE address = 'GABC123...'
FORMAT CSV
" > results.csv
```

---

## FAQs

**Q: Why write twice (materialized view duplication)?**  
A: Storage is cheap (~$20/month for duplication), query speed matters (1ms vs 100ms). The materialized view enables address-indexed queries.

**Q: Why not use OFFSET pagination?**  
A: OFFSET scans and discards rows, inefficient for large datasets. Cursor-based pagination jumps directly to the position.

**Q: Can I query by asset type?**  
A: Not optimized for it. Add `asset_type` to ORDER BY if needed:
```sql
ORDER BY (address, asset_type, closed_at, ...)
```

**Q: How do I add a new field?**  
A: Use ALTER TABLE:
```sql
ALTER TABLE events_canonical ADD COLUMN new_field String DEFAULT '';
```
Then recreate materialized view.
