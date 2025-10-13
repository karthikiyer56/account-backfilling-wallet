package main

// Filename: address_lookup.go
// Usage: go run address_lookup.go <rocksdb-path> <stellar-address>
// Example: go run address_lookup.go /tmp/test-db GABC123...

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/strkey"
)

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("Usage: address_lookup <rocksdb-path> <stellar-address>")
		fmt.Println("Example: address_lookup /tmp/test-db GABC123...")
		os.Exit(1)
	}

	dbPath := flag.Arg(0)
	addressStr := flag.Arg(1)

	// Validate address format
	if len(addressStr) == 0 {
		log.Fatal("Error: Address cannot be empty")
	}

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Error: Database directory does not exist: %s", dbPath)
	}

	// Open RocksDB in read-only mode
	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
	defer db.Close()

	// Convert Stellar address to RocksDB key
	keyBytes, err := StrkeyToRocksDBKey(addressStr)
	if err != nil {
		log.Fatalf("Failed to convert address to RocksDB key: %v", err)
	}

	if keyBytes == nil {
		log.Fatalf("Error: Address is not a G-address or C-address: %s", addressStr)
	}

	// Read from database
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	value, err := db.Get(ro, keyBytes)
	if err != nil {
		log.Fatalf("Failed to read from RocksDB: %v", err)
	}
	defer value.Free()

	data := value.Data()

	// Check if address exists in database
	if len(data) == 0 {
		fmt.Printf("\n========================================\n")
		fmt.Printf("Address: %s\n", addressStr)
		fmt.Printf("========================================\n")
		fmt.Printf("Status: NOT FOUND in database\n")
		fmt.Printf("This address has not appeared in any ledgers in the indexed range.\n")
		fmt.Printf("========================================\n\n")
		return
	}

	// Parse ledger sequences
	numLedgers := len(data) / 4
	ledgers := make([]uint32, numLedgers)

	for i := 0; i < numLedgers; i++ {
		ledgers[i] = binary.BigEndian.Uint32(data[i*4 : (i+1)*4])
	}

	// Display results
	fmt.Printf("\n========================================\n")
	fmt.Printf("Address: %s\n", addressStr)
	fmt.Printf("========================================\n")
	fmt.Printf("Total ledgers: %d\n", numLedgers)
	fmt.Printf("First appearance: Ledger %d\n", ledgers[0])
	fmt.Printf("Last appearance: Ledger %d\n", ledgers[numLedgers-1])
	fmt.Printf("========================================\n")

	// Show first and last few ledgers if there are many
	if numLedgers <= 20 {
		fmt.Printf("\nAll ledgers:\n")
		for i, ledger := range ledgers {
			fmt.Printf("  %d. Ledger %d\n", i+1, ledger)
		}
	} else {
		fmt.Printf("\nFirst 10 ledgers:\n")
		for i := 0; i < 10; i++ {
			fmt.Printf("  %d. Ledger %d\n", i+1, ledgers[i])
		}
		fmt.Printf("\n  ... (%d more) ...\n\n", numLedgers-20)
		fmt.Printf("Last 10 ledgers:\n")
		for i := numLedgers - 10; i < numLedgers; i++ {
			fmt.Printf("  %d. Ledger %d\n", i-numLedgers+11, ledgers[i])
		}
	}
	fmt.Printf("\n")
}

// StrkeyToRocksDBKey converts a Stellar strkey (G-address or C-address)
// to a byte array suitable for use as a RocksDB key.
func StrkeyToRocksDBKey(strkeyStr string) ([]byte, error) {
	// Use DecodeAny to handle both G-addresses and C-addresses
	version, payload, err := strkey.DecodeAny(strkeyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode strkey: '%s', error: %w", strkeyStr, err)
	}

	// Verify it's either an account ID or contract address
	if version != strkey.VersionByteAccountID && version != strkey.VersionByteContract {
		return nil, nil
	}

	// Create the key: [version_byte][payload]
	key := make([]byte, 1+len(payload))
	key[0] = byte(version)
	copy(key[1:], payload)

	return key, nil
}
