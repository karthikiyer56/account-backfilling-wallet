# Querying the Address Activity Database

This guide covers all methods for querying the ClickHouse address activity database.

## Table of Contents

- [Query Application (query_address_activity.go)](#query-application-query_address_activitygo)
- [Direct Database Queries (Docker Exec)](#direct-database-queries-docker-exec)
    - [Count Queries](#count-queries)
    - [Aggregation Queries](#aggregation-queries)
    - [Time-Based Queries](#time-based-queries)
    - [Table Statistics](#table-statistics)
    - [Activity Distribution](#activity-distribution)
    - [Sample Data Queries](#sample-data-queries)
    - [Performance Testing](#performance-testing)
- [Bash Helper Functions](#bash-helper-functions)
- [Output Formats](#output-formats)

---

## Query Application (query_address_activity.go)

### What It Does

The `query_address_activity` application is a command-line tool for querying address activities with built-in pagination support. It provides:

- **Recent Activities**: Get the most recent 20 activities for an address in descending order (newest first)
- **Cursor-Based Pagination**: Navigate through large result sets efficiently using stateless cursors
- **Monthly Activities**: Query activities for a specific month in ascending order (oldest first in that month)
- **Query Transparency**: Shows the exact SQL executed, parameters, and execution time
- **Performance Metrics**: Displays query execution time, rows returned, and throughput

**Key Features:**
- Automatic mode detection (recent vs monthly based on `--month` flag)
- Base64-encoded cursors for stable pagination
- Formatted output with activity details and operation type names
- Next command suggestions for easy pagination
- No server-side session state required

### Usage

**Build:**
```bash
go build -o query_address_activity query_address_activity.go
```

**Examples:**
```bash
# Get last 20 recent activities (descending order)
./query_address_activity --address GABC123...

# Get next page with cursor
./query_address_activity --address GABC123... --cursor "eyJjbG9z..."

# Get activities for January 2025 (ascending order)
./query_address_activity --address GABC123... --month 2025-01

# Get next page of January activities
./query_address_activity --address GABC123... --month 2025-01 --cursor "eyJjbG9z..."
```

**What It Shows:**
```
┌────────────────────────────────────────────────────────────────────────────────┐
│                             QUERY DETAILS                                      │
└────────────────────────────────────────────────────────────────────────────────┘

SQL Query:
─────────────────────────────────────────────────────────────────────────────────
  SELECT address, ledger_sequence, closed_at, tx_hash, ...
  FROM stellar.address_activity
  WHERE address = ?
  ORDER BY closed_at DESC, ledger_sequence DESC, ...
  LIMIT ?

Parameters:
─────────────────────────────────────────────────────────────────────────────────
  [1] GABC123... (string)
  [2] 20 (int)

Execution Metrics:
─────────────────────────────────────────────────────────────────────────────────
  Execution Time:   8.23 ms
  Rows Returned:    20
  Rows/Second:      2429

═════════════════════════════════════════════════════════════════════════════════
```

**Options:**
- `--address` - Stellar address (required)
- `--cursor` - Pagination cursor (optional)
- `--month` - Month in YYYY-MM format (optional, e.g., 2025-01)
- `--clickhouse-host` - ClickHouse host (default: localhost)
- `--clickhouse-port` - ClickHouse port (default: 9000)
- `--clickhouse-password` - ClickHouse password (default: empty)
- `--database` - Database name (default: stellar)

---

## Direct Database Queries (Docker Exec)

These queries can be run directly against the ClickHouse container without using the Go application.

### Basic Syntax
```bash
docker exec <container-name> clickhouse-client --query "SQL QUERY HERE"
```

For our setup:
```bash
docker exec clickhouse-activity clickhouse-client --query "SELECT ..."
```

---

## Count Queries

### Count Total Events for an Address
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as total_events
FROM stellar.address_activity
WHERE address = 'GABC123...'
"
```

### Count Events for Address in Date Range
```bash
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
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as total_events
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND toYYYYMM(closed_at) = 202510
"
```

### Count All Events in Date Range
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT count(*) as total_events
FROM stellar.events_canonical
WHERE closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
"
```

### Count Events by Type for an Address
```bash
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

### Least Active Addresses in Date Range
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    address,
    count(*) as activity_count,
    min(closed_at) as first_activity,
    max(closed_at) as last_activity,
    dateDiff('day', min(closed_at), max(closed_at)) as active_days
FROM stellar.address_activity
WHERE closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
GROUP BY address
ORDER BY activity_count ASC
LIMIT 20
FORMAT PrettyCompact
"
```

### Addresses with Activity Count in Range
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    address,
    count(*) as activity_count,
    min(closed_at) as first_activity,
    max(closed_at) as last_activity,
    dateDiff('day', min(closed_at), max(closed_at)) as active_days,
    groupUniqArray(event_type) as event_types
FROM stellar.address_activity
WHERE closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
GROUP BY address
HAVING activity_count >= 5 AND activity_count <= 10
ORDER BY activity_count DESC
LIMIT 20
FORMAT PrettyCompact
"
```

---

## Time-Based Queries

### Events Per Hour for Last 24 Hours
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

## Activity Distribution

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

### Addresses with Exactly 1 Activity (One-Time Users)
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    address,
    event_type,
    closed_at,
    amount,
    asset_type,
    asset_code
FROM stellar.address_activity
WHERE address IN (
    SELECT address
    FROM stellar.address_activity
    WHERE closed_at >= '2025-01-01 00:00:00'
      AND closed_at < '2025-02-01 00:00:00'
    GROUP BY address
    HAVING count(*) = 1
)
AND closed_at >= '2025-01-01 00:00:00'
AND closed_at < '2025-02-01 00:00:00'
ORDER BY closed_at DESC
LIMIT 20
FORMAT PrettyCompact
"
```

---

## Performance Testing

### Query Performance Test
```bash
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

## Bash Helper Functions

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

# Get least active addresses in date range
ch_least_active() {
    local start_date=$1
    local end_date=$2
    local limit=${3:-20}
    
    chquery "
    SELECT 
        address,
        count(*) as activity_count,
        min(closed_at) as first_activity,
        max(closed_at) as last_activity,
        dateDiff('day', min(closed_at), max(closed_at)) as active_days
    FROM stellar.address_activity
    WHERE closed_at >= '$start_date'
      AND closed_at < '$end_date'
    GROUP BY address
    ORDER BY activity_count ASC
    LIMIT $limit
    FORMAT PrettyCompact
    "
}

# Get addresses with activity count in range
ch_activity_range() {
    local start_date=$1
    local end_date=$2
    local min_count=$3
    local max_count=$4
    local limit=${5:-20}
    
    if [ -z "$start_date" ] || [ -z "$end_date" ] || [ -z "$min_count" ] || [ -z "$max_count" ]; then
        echo "Usage: ch_activity_range <start-date> <end-date> <min-count> <max-count> [limit]"
        echo "Example: ch_activity_range '2025-01-01 00:00:00' '2025-02-01 00:00:00' 5 10 20"
        return 1
    fi
    
    chquery "
    SELECT 
        address,
        count(*) as activity_count,
        min(closed_at) as first_activity,
        max(closed_at) as last_activity,
        dateDiff('day', min(closed_at), max(closed_at)) as active_days,
        groupUniqArray(event_type) as event_types
    FROM stellar.address_activity
    WHERE closed_at >= '$start_date'
      AND closed_at < '$end_date'
    GROUP BY address
    HAVING activity_count >= $min_count AND activity_count <= $max_count
    ORDER BY activity_count DESC, address ASC
    LIMIT $limit
    FORMAT PrettyCompact
    "
}
```

**Usage:**
```bash
# After adding to ~/.bashrc
source ~/.bashrc

# Count events for an address
ch_count_address "GABC123..."

# Count events in date range
ch_count_range "2025-01-01 00:00:00" "2025-02-01 00:00:00"

# Last 5 events for address
ch_last_events "GABC123..."

# Least active addresses
ch_least_active "2025-01-01 00:00:00" "2025-02-01 00:00:00" 20

# Addresses with 5-10 activities
ch_activity_range "2025-01-01 00:00:00" "2025-02-01 00:00:00" 5 10 20
```

---

## Output Formats

ClickHouse supports multiple output formats:

### Available Formats

- `FORMAT PrettyCompact` - Pretty table format (default, human-readable)
- `FORMAT JSON` - JSON format
- `FORMAT CSV` - Comma-separated values
- `FORMAT TSV` - Tab-separated values
- `FORMAT Vertical` - Vertical format (one column per line)

### Examples

**JSON:**
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT * FROM stellar.address_activity WHERE address = 'GABC123...' LIMIT 1
FORMAT JSON
"
```

**CSV (save to file):**
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT * FROM stellar.address_activity WHERE address = 'GABC123...'
FORMAT CSV
" > results.csv
```

**Vertical (detailed view):**
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT * FROM stellar.address_activity WHERE address = 'GABC123...' LIMIT 1
FORMAT Vertical
"
```

---

## Using Variables

Make queries reusable with variables:
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

---

## Tips

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

### Pipe to jq for JSON Processing
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT address, count(*) as activity_count
FROM stellar.address_activity
GROUP BY address
LIMIT 10
FORMAT JSON
" | jq '.data[] | {address: .address, count: .activity_count}'
```

---

## Common Query Patterns

### Pattern 1: Address Activity Summary
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    count(*) as total_events,
    min(closed_at) as first_seen,
    max(closed_at) as last_seen,
    dateDiff('day', min(closed_at), max(closed_at)) as active_days,
    groupUniqArray(event_type) as event_types,
    groupUniqArray(asset_code) as assets_used
FROM stellar.address_activity
WHERE address = 'GABC123...'
FORMAT Vertical
"
```

### Pattern 2: Time-Series Activity
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    toStartOfDay(closed_at) as day,
    count(*) as events,
    countIf(event_type = 'transfer') as transfers,
    sum(toFloat64OrZero(amount)) as total_volume
FROM stellar.address_activity
WHERE address = 'GABC123...'
  AND closed_at >= now() - INTERVAL 30 DAY
  AND asset_type = 'native'
GROUP BY day
ORDER BY day DESC
FORMAT PrettyCompact
"
```

### Pattern 3: Comparative Analysis
```bash
docker exec clickhouse-activity clickhouse-client --query "
SELECT 
    address,
    count(*) as activity_count,
    countIf(event_type = 'transfer') as transfers,
    countIf(from_address = address) as sent,
    countIf(to_address = address) as received,
    round(countIf(from_address = address) * 100.0 / countIf(event_type = 'transfer'), 2) as sent_pct
FROM stellar.address_activity
WHERE address IN ('GABC123...', 'GDEF456...', 'GHIJ789...')
  AND closed_at >= '2025-01-01 00:00:00'
  AND closed_at < '2025-02-01 00:00:00'
GROUP BY address
FORMAT PrettyCompact
"
```

---

## Query Performance

**Expected Performance:**
- Address lookup queries: ????
- Monthly queries with partition pruning: ???
- Full table aggregations: ???

**Tips for Fast Queries:**
- Always filter by `address` first (uses primary key index)
- Use `toYYYYMM(closed_at) = YYYYMM` for monthly queries (partition pruning)
- Avoid `SELECT *` unless you need all columns
- Use `LIMIT` to restrict result sizes
- Add `--time` flag to see execution time

---

## Troubleshooting

### Query Times Out
```bash
# Increase execution time limit
docker exec clickhouse-activity clickhouse-client --query "
SELECT ...
SETTINGS max_execution_time = 300
"
```

### Container Not Found
```bash
# List running containers
docker ps

# If container has different name, use it
docker exec <your-container-name> clickhouse-client --query "..."
```

### Permission Denied
```bash
# Check if clickhouse user has permissions
docker exec clickhouse-activity clickhouse-client --query "
SHOW GRANTS FOR default
"
```