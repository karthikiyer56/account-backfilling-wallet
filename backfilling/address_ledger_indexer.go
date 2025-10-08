package main

// Filename: address_ledger_indexer.go

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/stellar/go/strkey"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
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
			"destination_bucket_path": "sdf-ledger-close-meta/ledgers/pubnet",
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
		BufferSize: 100,
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

	// Iterate through the ledger sequence
	for ledgerSeq := ledgerRange.From(); ledgerSeq <= ledgerRange.To(); ledgerSeq++ {
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}

		// Process token transfer events in this ledger
		addressesInLedger, err := processLedger(db, ledger, ledgerSeq)
		if err != nil {
			log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
			// Continue processing other ledgers
		} else {
			totalAddressesUpdated += addressesInLedger
		}

		processedCount++
		currentPercent := (processedCount * 100) / totalLedgers

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

	elapsed := time.Since(startTime)
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

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, errors.Wrap(err, "failed to open RocksDB")
	}

	return db, opts, nil
}

// processLedger processes all token transfer events in a ledger using the token_transfer processor
func processLedger(db *grocksdb.DB, ledger xdr.LedgerCloseMeta, ledgerSeq uint32) (int, error) {
	// Use the token transfer processor to extract events
	processor := token_transfer.NewEventsProcessorForUnifiedEvents(network.PublicNetworkPassphrase)
	events, err := processor.EventsFromLedger(ledger)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to process events from ledger %d", ledgerSeq)
	}

	// Map to track unique addresses in this ledger (deduplicate)
	addressesInLedger := make(map[string][]byte)

	// Process each event
	for _, event := range events {
		err := extractAddress(addressesInLedger, event)
		if err != nil {
			return 0, err
		}
	}

	// Update RocksDB for all unique addresses in this ledger
	addressesUpdated := 0
	for _, keyBytes := range addressesInLedger {
		if err := updateAddressLedgerList(db, keyBytes, ledgerSeq); err != nil {
			return addressesUpdated, errors.Wrapf(err, "failed to update address in ledger %d", ledgerSeq)
		}
		addressesUpdated++
	}

	return addressesUpdated, nil
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

	if _, exists := addressMap[from]; !exists {
		keyBytes, err := StrkeyToRocksDBKey(from)
		if err != nil {
			return errors.Wrap(err, "failed to convert from key to rocksdb key")
		}
		addressMap[from] = keyBytes
	}

	if _, exists := addressMap[to]; !exists {
		keyBytes, err := StrkeyToRocksDBKey(to)
		if err != nil {
			return errors.Wrap(err, "failed to convert to key to rocksdb key")
		}
		addressMap[to] = keyBytes
	}
	return nil
}

// updateAddressLedgerList updates the ledger list for an address in RocksDB
// Maintains sorted order of ledger sequences
func updateAddressLedgerList(db *grocksdb.DB, addressKey []byte, ledgerSeq uint32) error {
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	// Read existing value
	existingValue, err := db.Get(ro, addressKey)
	if err != nil {
		return errors.Wrap(err, "failed to read from RocksDB")
	}
	defer existingValue.Free()

	var newValue []byte
	existingData := existingValue.Data()

	if len(existingData) == 0 {
		// New address - create new list with this ledger
		newValue = make([]byte, 4)
		binary.BigEndian.PutUint32(newValue, ledgerSeq)
	} else {
		// Existing address - find insertion point to maintain sorted order
		numLedgers := len(existingData) / 4
		insertPos := -1
		alreadyExists := false

		for i := 0; i < numLedgers; i++ {
			existingLedger := binary.BigEndian.Uint32(existingData[i*4 : (i+1)*4])

			if existingLedger == ledgerSeq {
				// Ledger already exists, no update needed
				alreadyExists = true
				break
			}

			if existingLedger > ledgerSeq {
				// Found the position where we should insert
				insertPos = i
				break
			}
		}

		if alreadyExists {
			return nil
		}

		// If insertPos is still -1, append to the end
		if insertPos == -1 {
			insertPos = numLedgers
		}

		// Create new value with the ledger inserted at the correct position
		newValue = make([]byte, len(existingData)+4)

		// Copy data before insertion point
		copy(newValue, existingData[:insertPos*4])

		// Insert new ledger
		binary.BigEndian.PutUint32(newValue[insertPos*4:], ledgerSeq)

		// Copy data after insertion point
		copy(newValue[(insertPos+1)*4:], existingData[insertPos*4:])
	}

	// Write updated value
	err = db.Put(wo, addressKey, newValue)
	if err != nil {
		return errors.Wrap(err, "failed to write to RocksDB")
	}

	return nil
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
		return nil, fmt.Errorf("failed to decode strkey: %w", err)
	}

	// Verify it's either an account ID or contract address
	if version != strkey.VersionByteAccountID && version != strkey.VersionByteContract {
		return nil, fmt.Errorf("expected G-address or C-address, got version byte: %d", version)
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
