package main

// Filename: address_density_scanner.go
// Usage: go run address_density_scanner.go <rocksdb-path>
// Example: go run address_density_scanner.go /tmp/test-db

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/strkey"
)

type DensityBucket struct {
	name  string
	min   int
	max   int
	count int
}

type AddressOccurrence struct {
	address     string
	occurrences int
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: address_density_scanner <rocksdb-path>")
		fmt.Println("Example: address_density_scanner /tmp/test-db")
		os.Exit(1)
	}

	dbPath := flag.Arg(0)

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

	fmt.Printf("\n========================================\n")
	fmt.Printf("Scanning RocksDB: %s\n", dbPath)
	fmt.Printf("========================================\n\n")

	// Initialize density buckets
	buckets := []DensityBucket{
		{"1 time", 1, 1, 0},
		{"2-9 times", 2, 9, 0},
		{"10-49 times", 10, 49, 0},
		{"50-99 times", 50, 99, 0},
		{"100-499 times", 100, 499, 0},
		{"500-999 times", 500, 999, 0},
		{"1,000-4,999 times", 1000, 4999, 0},
		{"5,000-9,999 times", 5000, 9999, 0},
		{"10,000+ times", 10000, 999999999, 0},
	}

	// Create iterator
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	it := db.NewIterator(ro)
	defer it.Close()

	// Scan all keys
	startTime := time.Now()
	totalKeys := 0
	totalLedgerEntries := int64(0)

	// Track top 10 addresses
	topAddresses := make([]AddressOccurrence, 0, 10)

	fmt.Println("Scanning database... (this may take a while)")
	fmt.Println()

	lastReport := time.Now()
	reportInterval := 10 * time.Second

	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		keyData := key.Data()
		valueData := value.Data()

		// Calculate number of ledgers (each ledger is 4 bytes)
		numLedgers := len(valueData) / 4

		// Update statistics
		totalKeys++
		totalLedgerEntries += int64(numLedgers)

		// Decode address
		address, err := rocksDBKeyToStrkey(keyData)
		if err != nil {
			address = fmt.Sprintf("<decode error: %v>", err)
		}

		// Update top 10 list
		if len(topAddresses) < 10 {
			// Not full yet, just add
			topAddresses = append(topAddresses, AddressOccurrence{address, numLedgers})
			// Sort after adding
			sortAddressesByOccurrences(topAddresses)
		} else if numLedgers > topAddresses[9].occurrences {
			// Replace the 10th element
			topAddresses[9] = AddressOccurrence{address, numLedgers}
			sortAddressesByOccurrences(topAddresses)
		}

		// Categorize into buckets
		for i := range buckets {
			if numLedgers >= buckets[i].min && numLedgers <= buckets[i].max {
				buckets[i].count++
				break
			}
		}

		key.Free()
		value.Free()

		// Progress reporting
		if time.Since(lastReport) >= reportInterval {
			fmt.Printf("  Progress: %s addresses scanned...\r", formatNumber(int64(totalKeys)))
			lastReport = time.Now()
		}
	}

	if err := it.Err(); err != nil {
		log.Fatalf("Error during iteration: %v", err)
	}

	elapsed := time.Since(startTime)

	// Display results
	fmt.Printf("\n========================================\n")
	fmt.Printf("SCAN COMPLETE\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Time taken: %s\n", formatDuration(elapsed))
	fmt.Printf("Total unique addresses: %s\n", formatNumber(int64(totalKeys)))
	fmt.Printf("Total ledger entries: %s\n", formatNumber(totalLedgerEntries))
	fmt.Printf("Average appearances per address: %.2f\n", float64(totalLedgerEntries)/float64(totalKeys))
	fmt.Printf("\n")

	// Display density distribution
	fmt.Printf("========================================\n")
	fmt.Printf("ADDRESS DENSITY DISTRIBUTION\n")
	fmt.Printf("========================================\n\n")

	fmt.Printf("%-25s %15s %10s\n", "Frequency Range", "Count", "Percent")
	fmt.Printf("%-25s %15s %10s\n", "---------------", "-----", "-------")

	for _, bucket := range buckets {
		percentage := 0.0
		if totalKeys > 0 {
			percentage = (float64(bucket.count) / float64(totalKeys)) * 100
		}
		fmt.Printf("%-25s %15s %9.2f%%\n",
			bucket.name,
			formatNumber(int64(bucket.count)),
			percentage)
	}

	fmt.Printf("\n")
	fmt.Printf("========================================\n")
	fmt.Printf("TOP 10 MOST ACTIVE ADDRESSES\n")
	fmt.Printf("========================================\n\n")

	for i, addr := range topAddresses {
		fmt.Printf("%2d. %s\n", i+1, addr.address)
		fmt.Printf("    Appearances: %s ledgers\n\n", formatNumber(int64(addr.occurrences)))
	}

	fmt.Printf("========================================\n\n")
}

// rocksDBKeyToStrkey converts a RocksDB key back to a Stellar strkey string
func rocksDBKeyToStrkey(keyData []byte) (string, error) {
	if len(keyData) < 2 {
		return "", fmt.Errorf("key too short: %d bytes", len(keyData))
	}

	version := strkey.VersionByte(keyData[0])
	payload := keyData[1:]

	strkeyStr, err := strkey.Encode(version, payload)
	if err != nil {
		return "", fmt.Errorf("failed to encode strkey: %w", err)
	}

	return strkeyStr, nil
}

// sortAddressesByOccurrences sorts addresses by occurrences in descending order
func sortAddressesByOccurrences(addresses []AddressOccurrence) {
	// Simple bubble sort (fine for small arrays like top 10)
	n := len(addresses)
	for i := 0; i < n; i++ {
		for j := 0; j < n-i-1; j++ {
			if addresses[j].occurrences < addresses[j+1].occurrences {
				addresses[j], addresses[j+1] = addresses[j+1], addresses[j]
			}
		}
	}
}

// formatNumber formats large numbers with commas
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	} else if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
