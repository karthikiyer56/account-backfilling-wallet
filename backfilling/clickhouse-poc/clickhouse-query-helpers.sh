#!/bin/bash

# Filename: clickhouse-query-helpers.sh
# Helper functions for querying the ClickHouse address_ledgers table

# Note: Don't use 'set -e' here since this script is meant to be sourced

CLICKHOUSE_HOST=${CLICKHOUSE_HOST:-localhost}
CLICKHOUSE_PORT=${CLICKHOUSE_PORT:-9000}

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper function to run ClickHouse queries
run_query() {
    docker exec clickhouse-server clickhouse-client --query "$1"
}

# Function to show table statistics
show_stats() {
    echo -e "${BLUE}=========================================="
    echo "Table Statistics"
    echo -e "==========================================${NC}"
    echo ""

    echo "Total records:"
    run_query "SELECT formatReadableQuantity(count()) as total_records FROM stellar.address_ledgers"
    echo ""

    echo "Unique addresses:"
    run_query "SELECT formatReadableQuantity(count(DISTINCT address)) as unique_addresses FROM stellar.address_ledgers"
    echo ""

    echo "Date range:"
    run_query "SELECT min(closed_at) as earliest, max(closed_at) as latest FROM stellar.address_ledgers"
    echo ""

    echo "Storage usage:"
    run_query "
    SELECT
        formatReadableSize(sum(bytes_on_disk)) as total_size,
        formatReadableSize(sum(data_compressed_bytes)) as compressed,
        formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed,
        round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) as compression_ratio
    FROM system.parts
    WHERE database = 'stellar' AND table = 'address_ledgers' AND active
    "
    echo ""

    echo "Records per partition (by month):"
    run_query "
    SELECT
        partition,
        formatReadableQuantity(sum(rows)) as records,
        formatReadableSize(sum(bytes_on_disk)) as size
    FROM system.parts
    WHERE database = 'stellar'
        AND table = 'address_ledgers'
        AND active
    GROUP BY partition
    ORDER BY partition DESC
    LIMIT 12
    "
}

# Function to get ledgers for an address in a time range
get_address_ledgers() {
    local ADDRESS=$1
    local DAYS=${2:-365}  # Default to 1 year

    if [ -z "$ADDRESS" ]; then
        echo "Usage: get_address_ledgers <address> [days_back]"
        echo "Example: get_address_ledgers GXXX... 30"
        return 1
    fi

    echo -e "${BLUE}Ledgers for address: ${ADDRESS}${NC}"
    echo "Time range: Last $DAYS days"
    echo ""

    run_query "
    SELECT
        ledger_sequence,
        closed_at
    FROM stellar.address_ledgers
    WHERE address = '$ADDRESS'
        AND closed_at >= now() - INTERVAL $DAYS DAY
    ORDER BY ledger_sequence
    FORMAT PrettyCompact
    "
}

# Function to count address appearances
count_address_appearances() {
    local ADDRESS=$1
    local DAYS=${2:-365}

    if [ -z "$ADDRESS" ]; then
        echo "Usage: count_address_appearances <address> [days_back]"
        return 1
    fi

    echo -e "${BLUE}Counting appearances for: ${ADDRESS}${NC}"
    echo ""

    run_query "
    SELECT
        count() as total_appearances,
        min(closed_at) as first_seen,
        max(closed_at) as last_seen,
        dateDiff('day', min(closed_at), max(closed_at)) as active_days
    FROM stellar.address_ledgers
    WHERE address = '$ADDRESS'
        AND closed_at >= now() - INTERVAL $DAYS DAY
    FORMAT PrettyCompact
    "
}

# Function to get top N most active addresses
top_addresses() {
    local LIMIT=${1:-10}
    local DAYS=${2:-30}

    echo -e "${BLUE}Top $LIMIT most active addresses (last $DAYS days)${NC}"
    echo ""

    run_query "
    SELECT
        address,
        count() as appearances,
        min(closed_at) as first_seen,
        max(closed_at) as last_seen
    FROM stellar.address_ledgers
    WHERE closed_at >= now() - INTERVAL $DAYS DAY
    GROUP BY address
    ORDER BY appearances DESC
    LIMIT $LIMIT
    FORMAT PrettyCompact
    "
}

# Function to show activity by day
activity_by_day() {
    local DAYS=${1:-30}

    echo -e "${BLUE}Daily activity (last $DAYS days)${NC}"
    echo ""

    run_query "
    SELECT
        toDate(closed_at) as date,
        formatReadableQuantity(count()) as ledger_entries,
        formatReadableQuantity(count(DISTINCT address)) as unique_addresses,
        formatReadableQuantity(count(DISTINCT ledger_sequence)) as unique_ledgers
    FROM stellar.address_ledgers
    WHERE closed_at >= now() - INTERVAL $DAYS DAY
    GROUP BY date
    ORDER BY date DESC
    FORMAT PrettyCompact
    "
}

# Function to analyze address density distribution
address_density() {
    local DAYS=${1:-30}

    echo -e "${BLUE}Address density distribution (last $DAYS days)${NC}"
    echo ""

    local WHERE_CLAUSE=""
    if [ "$DAYS" != "all" ] && [ "$DAYS" -gt 0 ]; then
        WHERE_CLAUSE="WHERE closed_at >= now() - INTERVAL $DAYS DAY"
    fi

    run_query "
    WITH address_counts AS (
        SELECT
            address,
            count() as appearances
        FROM stellar.address_ledgers
        $WHERE_CLAUSE
        GROUP BY address
    )
    SELECT
        CASE
            WHEN appearances = 1 THEN '1 time'
            WHEN appearances <= 9 THEN '2-9 times'
            WHEN appearances <= 49 THEN '10-49 times'
            WHEN appearances <= 99 THEN '50-99 times'
            WHEN appearances <= 499 THEN '100-499 times'
            WHEN appearances <= 999 THEN '500-999 times'
            WHEN appearances <= 4999 THEN '1,000-4,999 times'
            WHEN appearances <= 9999 THEN '5,000-9,999 times'
            ELSE '10,000+ times'
        END as frequency_range,
        formatReadableQuantity(count()) as address_count,
        round(count() * 100.0 / sum(count()) OVER (), 2) as percentage
    FROM address_counts
    GROUP BY frequency_range
    ORDER BY min(appearances)
    FORMAT PrettyCompact
    "
}

# Function to analyze address density distribution for a date range
address_density_date_range() {
    local START_TIME=$1
    local END_TIME=$2

    if [ -z "$START_TIME" ] || [ -z "$END_TIME" ]; then
        echo "Usage: address_density_date_range <start-time> <end-time>"
        echo "Example: address_density_date_range '2025-01-01 00:00:00' '2025-02-01 00:00:00'"
        return 1
    fi

    echo -e "${BLUE}Address density distribution${NC}"
    echo "From: $START_TIME"
    echo "To:   $END_TIME"
    echo ""

    run_query "
    WITH address_counts AS (
        SELECT
            address,
            count() as appearances
        FROM stellar.address_ledgers
        WHERE closed_at >= '$START_TIME'
            AND closed_at < '$END_TIME'
        GROUP BY address
    )
    SELECT
        CASE
            WHEN appearances = 1 THEN '1 time'
            WHEN appearances <= 9 THEN '2-9 times'
            WHEN appearances <= 49 THEN '10-49 times'
            WHEN appearances <= 99 THEN '50-99 times'
            WHEN appearances <= 499 THEN '100-499 times'
            WHEN appearances <= 999 THEN '500-999 times'
            WHEN appearances <= 4999 THEN '1,000-4,999 times'
            WHEN appearances <= 9999 THEN '5,000-9,999 times'
            ELSE '10,000+ times'
        END as frequency_range,
        formatReadableQuantity(count()) as address_count,
        round(count() * 100.0 / sum(count()) OVER (), 2) as percentage
    FROM address_counts
    GROUP BY frequency_range
    ORDER BY min(appearances)
    FORMAT PrettyCompact
    "
}

# Function to get top N most active addresses in a date range
top_addresses_date_range() {
    local START_TIME=$1
    local END_TIME=$2
    local LIMIT=${3:-10}

    if [ -z "$START_TIME" ] || [ -z "$END_TIME" ]; then
        echo "Usage: top_addresses_date_range <start-time> <end-time> [limit]"
        echo "Example: top_addresses_date_range '2025-01-01 00:00:00' '2025-02-01 00:00:00' 20"
        return 1
    fi

    echo -e "${BLUE}Top $LIMIT most active addresses${NC}"
    echo "From: $START_TIME"
    echo "To:   $END_TIME"
    echo ""

    run_query "
    SELECT
        address,
        count() as appearances,
        min(closed_at) as first_seen,
        max(closed_at) as last_seen
    FROM stellar.address_ledgers
    WHERE closed_at >= '$START_TIME'
        AND closed_at < '$END_TIME'
    GROUP BY address
    ORDER BY appearances DESC
    LIMIT $LIMIT
    FORMAT PrettyCompact
    "
}

# Function to count address appearances in a date range
count_address_appearances_date_range() {
    local ADDRESS=$1
    local START_TIME=$2
    local END_TIME=$3

    if [ -z "$ADDRESS" ] || [ -z "$START_TIME" ] || [ -z "$END_TIME" ]; then
        echo "Usage: count_address_appearances_date_range <address> <start-time> <end-time>"
        echo "Example: count_address_appearances_date_range GXXX... '2025-01-01 00:00:00' '2025-02-01 00:00:00'"
        return 1
    fi

    echo -e "${BLUE}Counting appearances for: ${ADDRESS}${NC}"
    echo "From: $START_TIME"
    echo "To:   $END_TIME"
    echo ""

    run_query "
    SELECT
        count() as total_appearances,
        min(closed_at) as first_seen,
        max(closed_at) as last_seen,
        dateDiff('day', min(closed_at), max(closed_at)) as active_days
    FROM stellar.address_ledgers
    WHERE address = '$ADDRESS'
        AND closed_at >= '$START_TIME'
        AND closed_at < '$END_TIME'
    FORMAT PrettyCompact
    "
}

# Function to test query performance
benchmark_query() {
    local ADDRESS=$1

    if [ -z "$ADDRESS" ]; then
        echo "Usage: benchmark_query <address>"
        echo "This will test query performance for the given address"
        return 1
    fi

    echo -e "${BLUE}Benchmarking query for address: ${ADDRESS}${NC}"
    echo ""

    echo "Query: Get ledgers from last year"
    run_query "
    SELECT
        count() as result_count,
        formatReadableSize(sum(bytes_read)) as bytes_read,
        round(elapsed, 3) as elapsed_seconds
    FROM (
        SELECT ledger_sequence
        FROM stellar.address_ledgers
        WHERE address = '$ADDRESS'
            AND closed_at >= now() - INTERVAL 1 YEAR
    )
    SETTINGS max_threads = 1
    FORMAT PrettyCompact
    "
}

# Main menu
show_menu() {
    echo -e "${GREEN}=========================================="
    echo "ClickHouse Query Helpers"
    echo -e "==========================================${NC}"
    echo ""
    echo "📊 GENERAL STATISTICS"
    echo "  show_stats"
    echo "    Show overall table statistics including total records, unique addresses,"
    echo "    storage usage, and partition information"
    echo ""
    echo "    Example:"
    echo "      show_stats"
    echo ""
    echo ""
    echo "🔍 ADDRESS QUERIES (Time-based)"
    echo "  get_address_ledgers <address> [days]"
    echo "    Get all ledger sequences for an address in the last N days"
    echo "    Default: 365 days (1 year)"
    echo ""
    echo "    Examples:"
    echo "      get_address_ledgers GDLYECYW23R4K7LLMCNK6ZG4BMA6ODESNTUHNK56E3DOW6723VJ2HX5J"
    echo "      get_address_ledgers GDLYECYW23R4K7LLMCNK6ZG4BMA6ODESNTUHNK56E3DOW6723VJ2HX5J 30"
    echo ""
    echo "  count_address_appearances <address> [days]"
    echo "    Count how many times an address appeared in the last N days"
    echo "    Default: 365 days"
    echo ""
    echo "    Examples:"
    echo "      count_address_appearances GDLYECYW23R4K7LLMCNK6ZG4BMA6ODESNTUHNK56E3DOW6723VJ2HX5J"
    echo "      count_address_appearances GDLYECYW23R4K7LLMCNK6ZG4BMA6ODESNTUHNK56E3DOW6723VJ2HX5J 90"
    echo ""
    echo ""
    echo "🔍 ADDRESS QUERIES (Date Range)"
    echo "  count_address_appearances_date_range <address> <start-time> <end-time>"
    echo "    Count address appearances between specific dates"
    echo ""
    echo "    Example:"
    echo "      count_address_appearances_date_range GDLYECYW23R4K7LLMCNK6ZG4BMA6ODESNTUHNK56E3DOW6723VJ2HX5J \\"
    echo "        '2025-01-01 00:00:00' '2025-02-01 00:00:00'"
    echo ""
    echo ""
    echo "📈 TOP ADDRESSES"
    echo "  top_addresses [limit] [days]"
    echo "    Show most active addresses in the last N days"
    echo "    Default: top 10 addresses, last 30 days"
    echo ""
    echo "    Examples:"
    echo "      top_addresses"
    echo "      top_addresses 20"
    echo "      top_addresses 20 7"
    echo ""
    echo "  top_addresses_date_range <start-time> <end-time> [limit]"
    echo "    Show most active addresses between specific dates"
    echo "    Default: top 10 addresses"
    echo ""
    echo "    Examples:"
    echo "      top_addresses_date_range '2025-01-01 00:00:00' '2025-02-01 00:00:00'"
    echo "      top_addresses_date_range '2025-01-01 00:00:00' '2025-02-01 00:00:00' 50"
    echo ""
    echo ""
    echo "📊 ACTIVITY ANALYSIS"
    echo "  activity_by_day [days]"
    echo "    Show daily activity statistics for the last N days"
    echo "    Default: 30 days"
    echo ""
    echo "    Examples:"
    echo "      activity_by_day"
    echo "      activity_by_day 7"
    echo "      activity_by_day 90"
    echo ""
    echo ""
    echo "📉 ADDRESS DENSITY DISTRIBUTION"
    echo "  address_density [days|all]"
    echo "    Show frequency distribution of address appearances"
    echo "    Use 'all' for entire dataset, or specify days"
    echo "    Default: 30 days"
    echo ""
    echo "    Examples:"
    echo "      address_density all"
    echo "      address_density 7"
    echo "      address_density 90"
    echo ""
    echo "  address_density_date_range <start-time> <end-time>"
    echo "    Show frequency distribution between specific dates"
    echo ""
    echo "    Example:"
    echo "      address_density_date_range '2025-01-01 00:00:00' '2025-02-01 00:00:00'"
    echo ""
    echo ""
    echo "⚡ PERFORMANCE"
    echo "  benchmark_query <address>"
    echo "    Test query performance for a specific address"
    echo ""
    echo "    Example:"
    echo "      benchmark_query GDLYECYW23R4K7LLMCNK6ZG4BMA6ODESNTUHNK56E3DOW6723VJ2HX5J"
    echo ""
    echo ""
    echo -e "${YELLOW}💡 QUICK START${NC}"
    echo "  1. Source this file:"
    echo "     source clickhouse-query-helpers.sh"
    echo ""
    echo "  2. View overall stats:"
    echo "     show_stats"
    echo ""
    echo "  3. Check address density for January 2025:"
    echo "     address_density_date_range '2025-01-01 00:00:00' '2025-02-01 00:00:00'"
    echo ""
    echo "  4. Find top addresses in January:"
    echo "     top_addresses_date_range '2025-01-01 00:00:00' '2025-02-01 00:00:00' 20"
    echo ""
}

# If script is executed (not sourced), show menu
# Check for both bash and zsh
if [ -n "$BASH_SOURCE" ]; then
    if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
        show_menu
    fi
elif [ -n "$ZSH_VERSION" ]; then
    if [ "${(%):-%x}" = "${0}" ]; then
        show_menu
    fi
fi