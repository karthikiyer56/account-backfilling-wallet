package main

// Filename: address_ledger_indexer.go

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

func main() {
	// Command-line flags
	var startLedger, endLedger uint
	var newDB, existingDB string

	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")
	flag.StringVar(&newDB, "newdb", "", "Path to create a new RocksDB database")
	flag.StringVar(&existingDB, "existingdb", "", "Path to an existing RocksDB database")
	flag.Parse()

	// Validate required args
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}

	if (newDB == "" && existingDB == "") || (newDB != "" && existingDB != "") {
		log.Fatal("Must specify exactly one of --newdb or --existingdb")
	}

	dbPath := newDB
	createNew := newDB != ""
	if existingDB != "" {
		dbPath = existingDB
	}

	// Initialize RocksDB
	db, opts, err := openRocksDB(dbPath, createNew)
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
	defer func() {
		db.Close()
		opts.Destroy()
	}()

	log.Printf("RocksDB opened at: %s (create_new: %v)", dbPath, createNew)

	ctx := context.Background()

	// Configure the datastore
	datastoreConfig := datastore.DataStoreConfig{
		Type: "GCS",
		Params: map[string]string{
			"destination_bucket_path": "sdf-ledger-close-meta/v1/ledgers/pubnet",
		},
	}

	dataStoreSchema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}

	// Initialize the datastore
	dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create datastore"))
	}
	defer dataStore.Close()

	// Configure the BufferedStorageBackend
	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 1000,
		NumWorkers: 10,
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	// Initialize the backend
	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create buffered storage backend"))
	}
	defer backend.Close()

	start, end := uint32(startLedger), uint32(endLedger)
	ledgerRange := ledgerbackend.BoundedRange(start, end)
	totalLedgers := int(ledgerRange.To() - ledgerRange.From() + 1)

	err = backend.PrepareRange(ctx, ledgerRange)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "failed to prepare range:%v", ledgerRange))
	}

	log.Printf("Starting ledger processing for range: %d - %d (%d ledgers)",
		ledgerRange.From(), ledgerRange.To(), totalLedgers)

	// Set up progress tracking
	processedCount := 0
	lastReportedPercent := -1
	startTime := time.Now()
	totalAddressesUpdated := 0

	// Batch accumulator: map[addressKey][]ledgerSeqs
	batchData := make(map[string][]uint32)
	const batchSize = 1000

	// Iterate through the ledger sequence
	for ledgerSeq := ledgerRange.From(); ledgerSeq <= ledgerRange.To(); ledgerSeq++ {
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}

		processor := token_transfer.NewEventsProcessorForUnifiedEvents(network.PublicNetworkPassphrase)

		// Process token transfer events in this ledger and accumulate in batch
		err = processLedgerToBatch(processor, batchData, ledger, ledgerSeq)
		if err != nil {
			log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
			// Continue processing other ledgers
		}

		processedCount++
		currentPercent := (processedCount * 100) / totalLedgers

		// Write to database every 1000 ledgers
		if processedCount%batchSize == 0 {
			log.Printf("Writing batch to database at ledger %d (%d addresses accumulated)...", ledgerSeq, len(batchData))
			addressesWritten, err := writeBatchToDatabase(db, batchData)
			if err != nil {
				log.Printf("Error writing batch to database: %v", err)
			} else {
				totalAddressesUpdated += addressesWritten
				log.Printf("Batch written: %d addresses updated", addressesWritten)
			}

			// Clear batch data
			batchData = make(map[string][]uint32)

			// Flush to disk
			log.Printf("Flushing database to disk at ledger %d...", ledgerSeq)
			fo := grocksdb.NewDefaultFlushOptions()
			fo.SetWait(true)
			err = db.Flush(fo)
			fo.Destroy()
			if err != nil {
				log.Printf("Warning: Failed to flush database: %v", err)
			}

			size, err := getDirSize(dbPath)
			if err != nil {
				log.Printf("Warning: Failed to get DB size: %v", err)
			} else {
				log.Printf("Database size: %s", formatBytes(size))
			}
		}

		// Report progress every 1%
		if currentPercent > lastReportedPercent {
			elapsed := time.Since(startTime)
			ledgersPerSec := float64(processedCount) / elapsed.Seconds()
			remaining := totalLedgers - processedCount
			timeRemaining := time.Duration(0)
			if ledgersPerSec > 0 {
				timeRemaining = time.Duration(float64(remaining)/ledgersPerSec) * time.Second
			}

			log.Printf("Progress: %d/%d ledgers (%d%%) | %.2f ledgers/sec | %d addresses updated | ETA: %s",
				processedCount, totalLedgers, currentPercent, ledgersPerSec,
				totalAddressesUpdated, formatDuration(timeRemaining))

			lastReportedPercent = currentPercent
		}
	}

	// Write any remaining batch data
	if len(batchData) > 0 {
		log.Printf("Writing final batch to database (%d addresses)...", len(batchData))
		addressesWritten, err := writeBatchToDatabase(db, batchData)
		if err != nil {
			log.Printf("Error writing final batch to database: %v", err)
		} else {
			totalAddressesUpdated += addressesWritten
			log.Printf("Final batch written: %d addresses updated", addressesWritten)
		}
	}

	// Flush all data to disk since WAL is disabled
	log.Printf("Flushing database to disk...")
	fo := grocksdb.NewDefaultFlushOptions()
	fo.SetWait(true)
	defer fo.Destroy()
	err = db.Flush(fo)
	if err != nil {
		log.Printf("Warning: Failed to flush database: %v", err)
	}

	elapsed := time.Since(startTime)

	// Print final DB size
	finalSize, err := getDirSize(dbPath)
	if err != nil {
		log.Printf("Warning: Failed to get final DB size: %v", err)
	} else {
		log.Printf("Final database size: %s", formatBytes(finalSize))
	}

	log.Printf("Processing complete!")
	log.Printf("  Ledgers processed: %d", processedCount)
	log.Printf("  Total addresses updated: %d", totalAddressesUpdated)
	log.Printf("  Total time: %s", formatDuration(elapsed))
	log.Printf("  Average speed: %.2f ledgers/sec", float64(processedCount)/elapsed.Seconds())
}

// openRocksDB opens or creates a RocksDB database
func openRocksDB(path string, createNew bool) (*grocksdb.DB, *grocksdb.Options, error) {
	if createNew {
		// Remove existing directory if it exists
		if _, err := os.Stat(path); err == nil {
			log.Printf("Removing existing database at %s", path)
			if err := os.RemoveAll(path); err != nil {
				return nil, nil, errors.Wrap(err, "failed to remove existing database directory")
			}
		}

		// Create parent directory if it doesn't exist
		parentDir := filepath.Dir(path)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return nil, nil, errors.Wrap(err, "failed to create database parent directory")
		}
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(createNew)
	opts.SetCreateIfMissingColumnFamilies(createNew)
	opts.SetErrorIfExists(createNew)

	// Optimize for bulk writes and large datasets
	opts.SetCompression(grocksdb.SnappyCompression)
	opts.SetWriteBufferSize(128 << 20) // 128 MB
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(128 << 20) // 128 MB
	opts.SetMaxBackgroundJobs(6)          // Replaces deprecated SetMaxBackgroundCompactions and SetMaxBackgroundFlushes
	opts.SetMaxOpenFiles(1000)

	// Disable WAL for bulk loading performance and to prevent WAL bloat
	// Safe for batch processing since we can restart if needed
	opts.SetDisableAutoCompactions(false) // Keep auto compactions enabled
	opts.SetLevel0FileNumCompactionTrigger(4)
	opts.SetLevel0SlowdownWritesTrigger(20)
	opts.SetLevel0StopWritesTrigger(30)

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, errors.Wrap(err, "failed to open RocksDB")
	}

	return db, opts, nil
}

// processLedgerToBatch processes a ledger and accumulates addresses in the batch map
func processLedgerToBatch(processor *token_transfer.EventsProcessor, batchData map[string][]uint32, ledger xdr.LedgerCloseMeta, ledgerSeq uint32) error {
	// Use the token transfer processor to extract events
	events, err := processor.EventsFromLedger(ledger)
	if err != nil {
		return errors.Wrapf(err, "failed to process events from ledger %d", ledgerSeq)
	}

	// Map to track unique addresses in this ledger (deduplicate within this ledger)
	addressesInLedger := make(map[string][]byte)

	// Process each event
	for _, event := range events {
		err := extractAddress(addressesInLedger, event)
		if err != nil {
			return err
		}
	}

	// Add addresses from this ledger to the batch
	for _, keyBytes := range addressesInLedger {
		keyStr := string(keyBytes) // Use bytes as string map key. HACK but works

		// Check if this address already has ledgers in the batch
		if existingLedgers, exists := batchData[keyStr]; exists {
			// Check if this ledger is already in the list
			found := false
			for _, seq := range existingLedgers {
				if seq == ledgerSeq {
					found = true
					break
				}
			}
			if !found {
				batchData[keyStr] = append(existingLedgers, ledgerSeq)
			}
		} else {
			// New address in this batch
			batchData[keyStr] = []uint32{ledgerSeq}
		}
	}

	return nil
}

// writeBatchToDatabase writes accumulated batch data to RocksDB
func writeBatchToDatabase(db *grocksdb.DB, batchData map[string][]uint32) (int, error) {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // Disable WAL for bulk loading performance
	defer wo.Destroy()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	addressesUpdated := 0

	for keyStr, newLedgers := range batchData {
		addressKey := []byte(keyStr)

		// 1. Read existing ledgers from DB
		existingValue, err := db.Get(ro, addressKey)
		if err != nil {
			return addressesUpdated, errors.Wrap(err, "failed to read from RocksDB")
		}

		existingData := existingValue.Data()

		// Parse existing ledgers
		var allLedgers []uint32
		if len(existingData) > 0 {
			numExisting := len(existingData) / 4
			for i := 0; i < numExisting; i++ {
				ledger := binary.BigEndian.Uint32(existingData[i*4 : (i+1)*4])
				allLedgers = append(allLedgers, ledger)
			}
		}
		existingValue.Free()

		// Merge with new ledgers
		for _, newLedger := range newLedgers {
			// Check if already exists
			found := false
			for _, existing := range allLedgers {
				if existing == newLedger {
					found = true
					break
				}
			}
			if !found {
				allLedgers = append(allLedgers, newLedger)
			}
		}

		// Sort all ledgers
		sortUint32Slice(allLedgers)

		// Encode to bytes
		newValue := make([]byte, len(allLedgers)*4)
		for i, ledger := range allLedgers {
			binary.BigEndian.PutUint32(newValue[i*4:], ledger)
		}

		// Write to database
		err = db.Put(wo, addressKey, newValue)
		if err != nil {
			return addressesUpdated, errors.Wrap(err, "failed to write to RocksDB")
		}

		addressesUpdated++
	}

	return addressesUpdated, nil
}

// sortUint32Slice sorts a slice of uint32 in ascending order
func sortUint32Slice(slice []uint32) {
	// Simple bubble sort (fine for our use case since ledgers are mostly sorted)
	n := len(slice)
	for i := 0; i < n; i++ {
		for j := 0; j < n-i-1; j++ {
			if slice[j] > slice[j+1] {
				slice[j], slice[j+1] = slice[j+1], slice[j]
			}
		}
	}
}

// extractAddress extracts the hash bytes from a token transfer address (G or C address)
func extractAddress(addressMap map[string][]byte, event *token_transfer.TokenTransferEvent) error {

	if event == nil {
		return nil
	}

	var from, to string
	switch event.GetEventType() {
	case token_transfer.FeeEvent:
		from = event.GetFee().From
	case token_transfer.TransferEvent:
		from = event.GetTransfer().From
		to = event.GetTransfer().To
	case token_transfer.MintEvent:
		to = event.GetMint().To
	case token_transfer.BurnEvent:
		from = event.GetBurn().From
	case token_transfer.ClawbackEvent:
		from = event.GetClawback().From
	}

	if from != "" {
		if _, exists := addressMap[from]; !exists {
			keyBytes, err := StrkeyToRocksDBKey(from)
			if err != nil {
				return errors.Wrap(err, "failed to convert from key to rocksdb key")
			}
			if keyBytes != nil {
				addressMap[from] = keyBytes
			}
		}
	}

	if to != "" {
		if _, exists := addressMap[to]; !exists {
			keyBytes, err := StrkeyToRocksDBKey(to)
			if err != nil {
				return errors.Wrap(err, "failed to convert to key to rocksdb key")
			}
			if keyBytes != nil {
				addressMap[to] = keyBytes
			}
		}
	}

	return nil
}

// getDirSize calculates the total size of a directory
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// formatBytes formats bytes into human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
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

// StrkeyToRocksDBKey converts a Stellar strkey (G-address or C-address)
// to a byte array suitable for use as a RocksDB key.
//
// The returned byte array includes:
// - 1 byte: version byte (to distinguish between G and C addresses)
// - N bytes: the decoded payload (32 bytes for accounts/contracts)
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
	// This ensures G and C addresses with the same payload are distinct
	key := make([]byte, 1+len(payload))
	key[0] = byte(version)
	copy(key[1:], payload)

	return key, nil
}

// RocksDBKeyToStrkey converts a RocksDB key back to a Stellar strkey string
func RocksDBKeyToStrkey(key []byte) (string, error) {
	if len(key) < 2 {
		return "", fmt.Errorf("key too short")
	}

	version := strkey.VersionByte(key[0])
	payload := key[1:]

	strkeyStr, err := strkey.Encode(version, payload)
	if err != nil {
		return "", fmt.Errorf("failed to encode strkey: %w", err)
	}

	return strkeyStr, nil
}
