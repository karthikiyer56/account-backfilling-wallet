# Stellar Address Ledger Indexer

A Go program that indexes Stellar addresses (both G-addresses and C-addresses) from token transfer events and stores them in RocksDB with the ledger sequences where they appear.

## Overview

This tool:
- Downloads ledgers from Google Cloud Storage using Stellar's BufferedStorageBackend
- Extracts token transfer events using the  `token_transfer` processor
- Indexes addresses (G-addresses for accounts, C-addresses for contracts)
- Stores address → ledger sequence mappings in RocksDB
- Maintains sorted ledger sequences for efficient querying

## Prerequisites

### Required Software
1. **Go** (1.19 or later)
   ```bash
   go version  # Should be 1.19+
   ```

2. **stellar-etl** command-line tool
   ```bash
   # Install from https://github.com/stellar/stellar-etl
   # Or download binary from releases
   stellar-etl --version
   ```

3. **jq** (for JSON parsing in bash script)
   ```bash
   # Ubuntu/Debian
   sudo apt-get install jq
   
   # macOS
   brew install jq
   ```

4. **RocksDB** development libraries
   ```bash
   # Ubuntu/Debian
   sudo apt-get install librocksdb-dev
   
   # macOS
   brew install rocksdb
   ```

### Go Dependencies
```bash
go get github.com/linxGnu/grocksdb
go get github.com/stellar/go/ingest/ledgerbackend
go get github.com/stellar/go/processors/token_transfer
go get github.com/stellar/go/network
go get github.com/stellar/go/support/datastore
go get github.com/stellar/go/support/errors
go get github.com/stellar/go/xdr
go get github.com/stellar/go/strkey
```

## Installation

### 1. Clone or Download the Code

Place these files in your working directory:
- `address_ledger_indexer.go` - Main indexer program
- `rocksdb_stats.go` - Standalone statistics tool
- `run-address-indexer.sh` - Bash wrapper script

### 2. Set CGO Flags (Important!)

Before building, you need to set environment variables so Go can find RocksDB:

**Ubuntu/Debian (standard install):**
```bash
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L/usr/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"
```

**macOS (Homebrew install):**
```bash
export CGO_CFLAGS="-I/opt/homebrew/include"
export CGO_LDFLAGS="-L/opt/homebrew/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"

# For Intel Macs, use:
# export CGO_CFLAGS="-I/usr/local/include"
# export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"
```

**Custom RocksDB installation:**
```bash
export CGO_CFLAGS="-I/path/to/rocksdb/include"
export CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"
```

**Make permanent (optional):**
Add to your `~/.bashrc` or `~/.zshrc`:
```bash
# For Ubuntu/Debian
echo 'export CGO_CFLAGS="-I/usr/include"' >> ~/.bashrc
echo 'export CGO_LDFLAGS="-L/usr/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"' >> ~/.bashrc
source ~/.bashrc
```

### 3. Build the Binary

```bash
# Build the main indexer program
go build -o /tmp/address_ledger_indexer address_ledger_indexer.go

# Build the statistics tool
go build -o /tmp/rocksdb_stats rocksdb_stats.go

# Verify the binaries were created
ls -lh /tmp/address_ledger_indexer /tmp/rocksdb_stats
```

**If you still get errors**, verify RocksDB is installed:
```bash
# Check if rocksdb library exists
# Ubuntu/Debian:
ls -l /usr/lib/x86_64-linux-gnu/librocksdb.so* 
# or
ls -l /usr/lib/librocksdb.so*

# macOS:
ls -l /opt/homebrew/lib/librocksdb.dylib  # Apple Silicon
ls -l /usr/local/lib/librocksdb.dylib     # Intel

# Check if headers exist
# Ubuntu/Debian:
ls /usr/include/rocksdb/c.h

# macOS:
ls /opt/homebrew/include/rocksdb/c.h  # Apple Silicon
ls /usr/local/include/rocksdb/c.h     # Intel
```

### 3. Make the Script Executable

```bash
chmod +x run-address-indexer.sh
```

## Usage

### Using the Bash Script (Recommended)

The script automatically converts time ranges to ledger ranges using `stellar-etl`.


#### Create a New Database

```bash
./run-address-indexer.sh \
    2025-01-01T00:00:00+00:00 \
    2025-01-02T00:00:00+00:00 \
    /path/to/my-address-db \
    new
```

**Parameters:**
- `2025-01-01T00:00:00+00:00` - Start time (ISO 8601 format with timezone)
- `2025-01-02T00:00:00+00:00` - End time (ISO 8601 format with timezone)
- `/path/to/my-address-db` - Path where RocksDB will be created
- `new` - Create a new database (will delete if exists)

#### Update an Existing Database

```bash
./run-address-indexer.sh \
    2025-01-02T00:00:00+00:00 \
    2025-01-03T00:00:00+00:00 \
    /path/to/my-address-db \
    existing
```

**Parameters:**
- Same as above, but use `existing` to append to an existing database

### Direct Go Program Usage

If you want to run the Go program directly (without the script):

```bash
# Create new database
/tmp/address_ledger_indexer \
    --start-ledger 50000000 \
    --end-ledger 50010000 \
    --newdb /path/to/my-address-db

# Update existing database
/tmp/address_ledger_indexer \
    --start-ledger 50010001 \
    --end-ledger 50020000 \
    --existingdb /path/to/my-address-db
```

## Example Workflows

### Index One Month of Data

```bash
# January 2025
./run-address-indexer.sh \
    2025-01-01T00:00:00+00:00 \
    2025-02-01T00:00:00+00:00 \
    ~/stellar-data/jan-2025-db \
    new
```

### Index Multiple Months Incrementally

```bash
# January 2025 - create new DB
./run-address-indexer.sh \
    2025-01-01T00:00:00+00:00 \
    2025-02-01T00:00:00+00:00 \
    ~/stellar-data/q1-2025-db \
    new

# February 2025 - append to existing DB
./run-address-indexer.sh \
    2025-02-01T00:00:00+00:00 \
    2025-03-01T00:00:00+00:00 \
    ~/stellar-data/q1-2025-db \
    existing

# March 2025 - append to existing DB
./run-address-indexer.sh \
    2025-03-01T00:00:00+00:00 \
    2025-04-01T00:00:00+00:00 \
    ~/stellar-data/q1-2025-db \
    existing
```

### Backfill Missing Data

If you processed ledgers 1000-2000 and 3000-4000, you can fill the gap:

```bash
./run-address-indexer.sh \
    <start-time-for-ledgers-2001-2999> \
    <end-time-for-ledgers-2001-2999> \
    ~/stellar-data/my-db \
    existing
```

The program will automatically insert ledgers in sorted order.

## Understanding the Output

### Progress Tracking

During execution, you'll see progress updates like:

```
2025/01/15 10:23:45 RocksDB opened at: /home/user/my-db (create_new: true)
2025/01/15 10:23:45 Starting ledger processing for range: 50000000 - 50010000 (10000 ledgers)
2025/01/15 10:24:12 Progress: 1000/10000 ledgers (10%) | 37.04 ledgers/sec | 12543 addresses updated | ETA: 4m 3s
2025/01/15 10:25:18 Progress: 2000/10000 ledgers (20%) | 38.21 ledgers/sec | 25187 addresses updated | ETA: 3m 29s
...
2025/01/15 10:28:45 Processing complete!
2025/01/15 10:28:45   Ledgers processed: 10000
2025/01/15 10:28:45   Total addresses updated: 125430
2025/01/15 10:28:45   Total time: 5m 0s
2025/01/15 10:28:45   Average speed: 33.33 ledgers/sec
```

### Error Handling

If an error occurs processing a specific ledger:
```
2025/01/15 10:24:15 Error processing ledger 50005432: failed to process events from ledger
```

The program continues with the next ledger.

Fatal errors (e.g., network issues) will stop execution:
```
2025/01/15 10:24:20 Failed to retrieve ledger 50005433: connection timeout
```

## Database Structure

## Checking Database Statistics

Use the standalone `rocksdb_stats` tool to check your database at any time:

```bash
# Check database statistics
/tmp/rocksdb_stats /path/to/my-db
```

**Example output:**
```
========================================
RocksDB Statistics for: /tmp/test-db
========================================

📁 Total Directory Size: 256.78 MB

📊 RocksDB Internal Statistics:
─────────────────────────────────────
  Total SST Files Size              : 245.32 MB
  Live SST Files Size (actual data) : 245.32 MB
  All MemTables Size                : 0 B
  Current MemTables Size            : 0 B
  Estimated Live Data Size          : 240.15 MB
  Estimated Number of Keys          : 1,234,567

📂 File Breakdown:
─────────────────────────────────────
  SST Files (data):       12 files, 245.32 MB
  LOG Files (WAL):        1 files, 523.45 KB
```

**Key metrics to watch:**
- **Live SST Files Size** - Your actual data size (most important)
- **LOG Files** - Should stay small (< 10 MB). If large, data needs flushing
- **Estimated Number of Keys** - Total unique addresses indexed

You can run this tool while the indexer is running (read-only access) or after processing completes.

## Database Structure

### RocksDB Files

Your database directory will contain these files:

```
test-db/
├── 000004.log          # Write-Ahead Log (should stay small, WAL is disabled)
├── 000005.sst          # Sorted String Table (actual data)
├── 000006.sst          # More SST files as data grows
├── CURRENT             # Points to current MANIFEST
├── IDENTITY            # Database identifier
├── LOCK                # Prevents multiple processes opening DB
├── LOG                 # RocksDB internal logs
├── MANIFEST-000003     # Database metadata
└── OPTIONS-000007      # Database configuration
```

**Note:** With WAL disabled, the `.log` file should stay very small. If you see it growing large, data is being buffered. This is flushed every 10,000 ledgers automatically.


### Key Format

Keys are 33 bytes:
```
[1 byte: version] [32 bytes: hash]
```

- **Version byte for G-addresses**: `0x30` (strkey.VersionByteAccountID)
- **Version byte for C-addresses**: `0x02` (strkey.VersionByteContract)
- **Hash**: Ed25519 public key for accounts, contract hash for contracts

### Value Format

Values are arrays of 4-byte unsigned integers (big-endian):
```
[ledger1][ledger2][ledger3]...
```

Each ledger sequence is stored as a 4-byte big-endian uint32.

**Example:**
- Address appears in ledgers: 1000, 1500, 2000
- Value bytes (hex): `000003E8 000005DC 000007D0`

### Querying the Database

You can read the database using the `grocksdb` library:

```go
import (
    "encoding/binary"
    "github.com/linxGnu/grocksdb"
)

// Open database
opts := grocksdb.NewDefaultOptions()
db, _ := grocksdb.OpenDb(opts, "/path/to/db")
defer db.Close()

// Query an address
keyBytes, _ := StrkeyToRocksDBKey("GABC123...")
ro := grocksdb.NewDefaultReadOptions()
value, _ := db.Get(ro, keyBytes)
defer value.Free()

// Parse ledger sequences
data := value.Data()
numLedgers := len(data) / 4
for i := 0; i < numLedgers; i++ {
    ledgerSeq := binary.BigEndian.Uint32(data[i*4 : (i+1)*4])
    fmt.Printf("Ledger: %d\n", ledgerSeq)
}
```


## Troubleshooting

### Issue: "failed to create datastore"

**Cause**: No internet connection or GCS bucket unreachable

**Solution**: Check network connectivity and verify GCS bucket path

### Issue: "Failed to open RocksDB: lock file exists"

**Cause**: Another process is using the database

**Solution**: Close other processes or use a different database path

### Issue: "expected G-address or C-address"

**Cause**: Invalid address in token transfer event (rare)

**Solution**: This is logged but processing continues. No action needed.

### Issue: Slow processing speed

**Cause**: Network latency or resource constraints

**Solutions**:
1. Increase `NumWorkers` in the config
2. Increase `BufferSize` for more pre-fetching
3. Use a machine closer to GCS region
4. Process smaller time ranges in parallel on different machines
