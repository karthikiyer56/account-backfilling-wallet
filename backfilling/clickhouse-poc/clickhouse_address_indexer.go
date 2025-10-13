package main

// Filename: clickhouse_address_indexer.go

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// AddressLedgerEntry represents a single address-ledger pair with timestamp
type AddressLedgerEntry struct {
	Address        string
	LedgerSequence uint32
	ClosedAt       time.Time
}

func main() {
	// Command-line flags
	var startLedger, endLedger uint
	var clickhouseHost string
	var clickhousePort int
	var clickhousePassword string

	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")
	flag.StringVar(&clickhouseHost, "clickhouse-host", "localhost", "ClickHouse host")
	flag.IntVar(&clickhousePort, "clickhouse-port", 9000, "ClickHouse native port")
	flag.StringVar(&clickhousePassword, "clickhouse-password", "", "ClickHouse password (default: empty)")
	flag.Parse()

	// Validate required args
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}

	// Connect to ClickHouse
	conn, err := connectClickHouse(clickhouseHost, clickhousePort, clickhousePassword)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	log.Printf("Connected to ClickHouse at %s:%d", clickhouseHost, clickhousePort)

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
	totalRowsInserted := 0

	// Batch accumulator: slice of entries
	batchData := make([]AddressLedgerEntry, 0, 100000)
	const batchSize = 1000

	// Iterate through the ledger sequence
	for ledgerSeq := ledgerRange.From(); ledgerSeq <= ledgerRange.To(); ledgerSeq++ {
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}

		// Get the ledger close time
		closedAt := ledger.ClosedAt()

		processor := token_transfer.NewEventsProcessorForUnifiedEvents(network.PublicNetworkPassphrase)

		// Process token transfer events in this ledger and accumulate in batch
		err = processLedgerToBatch(processor, &batchData, ledger, ledgerSeq, closedAt)
		if err != nil {
			log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
		}

		processedCount++
		currentPercent := (processedCount * 100) / totalLedgers

		// Write to ClickHouse every 1000 ledgers
		if processedCount%batchSize == 0 {
			uniqueAddresses := countUniqueAddresses(batchData)
			log.Printf("Writing batch to ClickHouse at ledger %d (%d entries, %d unique addresses)...",
				ledgerSeq, len(batchData), uniqueAddresses)
			rowsInserted, err := writeBatchToClickHouse(ctx, conn, batchData)
			if err != nil {
				log.Printf("Error writing batch to ClickHouse: %v", err)
			} else {
				totalRowsInserted += rowsInserted
				log.Printf("Batch written: %d rows inserted", rowsInserted)
			}

			// Clear batch data
			batchData = make([]AddressLedgerEntry, 0, 100000)
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

			log.Printf("Progress: %d/%d ledgers (%d%%) | %.2f ledgers/sec | %d rows inserted | ETA: %s",
				processedCount, totalLedgers, currentPercent, ledgersPerSec,
				totalRowsInserted, formatDuration(timeRemaining))

			lastReportedPercent = currentPercent
		}
	}

	// Write any remaining batch data
	if len(batchData) > 0 {
		uniqueAddresses := countUniqueAddresses(batchData)
		log.Printf("Writing final batch to ClickHouse (%d entries, %d unique addresses)...",
			len(batchData), uniqueAddresses)
		rowsInserted, err := writeBatchToClickHouse(ctx, conn, batchData)
		if err != nil {
			log.Printf("Error writing final batch to ClickHouse: %v", err)
		} else {
			totalRowsInserted += rowsInserted
			log.Printf("Final batch written: %d rows inserted", rowsInserted)
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("Processing complete!")
	log.Printf("  Ledgers processed: %d", processedCount)
	log.Printf("  Total rows inserted: %d", totalRowsInserted)
	log.Printf("  Total time: %s", formatDuration(elapsed))
	log.Printf("  Average speed: %.2f ledgers/sec", float64(processedCount)/elapsed.Seconds())
	log.Printf("  Insert rate: %.2f rows/sec", float64(totalRowsInserted)/elapsed.Seconds())
}

// connectClickHouse establishes a connection to ClickHouse
func connectClickHouse(host string, port int, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: "stellar",
			Username: "default",
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns: 5,
		MaxIdleConns: 2,
	})

	if err != nil {
		return nil, err
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	return conn, nil
}

// writeBatchToClickHouse writes the batch data to ClickHouse
func writeBatchToClickHouse(ctx context.Context, conn clickhouse.Conn, batchData []AddressLedgerEntry) (int, error) {
	if len(batchData) == 0 {
		return 0, nil
	}

	// Prepare batch insert
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO stellar.address_ledgers (address, ledger_sequence, closed_at)")
	if err != nil {
		return 0, errors.Wrap(err, "failed to prepare batch")
	}

	rowCount := 0

	// Add all entries to batch
	for _, entry := range batchData {
		err := batch.Append(entry.Address, entry.LedgerSequence, entry.ClosedAt)
		if err != nil {
			return rowCount, errors.Wrapf(err, "failed to append row for address %s, ledger %d",
				entry.Address, entry.LedgerSequence)
		}
		rowCount++
	}

	// Send the batch
	if err := batch.Send(); err != nil {
		return rowCount, errors.Wrap(err, "failed to send batch")
	}

	return rowCount, nil
}

// processLedgerToBatch processes a ledger and accumulates addresses in the batch slice
func processLedgerToBatch(processor *token_transfer.EventsProcessor, batchData *[]AddressLedgerEntry,
	ledger xdr.LedgerCloseMeta, ledgerSeq uint32, closedAt time.Time) error {

	// Use the token transfer processor to extract events
	events, err := processor.EventsFromLedger(ledger)
	if err != nil {
		return errors.Wrapf(err, "failed to process events from ledger %d", ledgerSeq)
	}

	// Map to track unique addresses in this ledger (deduplicate within this ledger)
	addressesInLedger := make(map[string]bool)

	// Process each event
	for _, event := range events {
		addresses := extractAddresses(event)
		for _, addr := range addresses {
			addressesInLedger[addr] = true
		}
	}

	// Add addresses from this ledger to the batch
	for address := range addressesInLedger {
		*batchData = append(*batchData, AddressLedgerEntry{
			Address:        address,
			LedgerSequence: ledgerSeq,
			ClosedAt:       closedAt,
		})
	}

	return nil
}

// extractAddresses extracts G and C addresses from a token transfer event
func extractAddresses(event *token_transfer.TokenTransferEvent) []string {
	if event == nil {
		return nil
	}

	addresses := make([]string, 0, 2)

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

	// Only include G and C addresses
	if from != "" && isValidAddress(from) {
		addresses = append(addresses, from)
	}
	if to != "" && isValidAddress(to) {
		addresses = append(addresses, to)
	}

	return addresses
}

// isValidAddress checks if an address is a valid G or C address
func isValidAddress(address string) bool {
	version, _, err := strkey.DecodeAny(address)
	if err != nil {
		return false
	}
	return version == strkey.VersionByteAccountID || version == strkey.VersionByteContract
}

// countUniqueAddresses counts unique addresses in a batch
func countUniqueAddresses(batchData []AddressLedgerEntry) int {
	uniqueAddresses := make(map[string]bool)
	for _, entry := range batchData {
		uniqueAddresses[entry.Address] = true
	}
	return len(uniqueAddresses)
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
