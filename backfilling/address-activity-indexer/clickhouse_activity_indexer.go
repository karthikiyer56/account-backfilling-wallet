// Filename: clickhouse_activity_indexer.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
)

const (
	One       = uint8(1)
	None      = "none"
	FeeRefund = "fee_refund"
	Native    = "native"
	Issued    = "issued"
	Text      = "text"
	Hash      = "hash"
	Id        = "id"
)

// EventEntry represents a single token transfer event with all metadata
type EventEntry struct {
	LedgerSequence   uint32
	ClosedAt         time.Time
	TxHash           string
	TransactionIndex uint32
	OperationIndex   uint32
	EventIndex       uint32
	ContractAddress  string
	EventType        string
	OperationType    *uint8
	FromAddress      string
	ToAddress        string
	Amount           string
	AssetType        string
	AssetCode        string
	ToMuxedInfoType  string
	ToMuxedInfoText  string
	ToMuxedInfoID    uint64
	ToMuxedInfoHash  string
}

func main() {
	var startLedger, endLedger uint
	var clickhouseHost, clickhousePassword, database string
	var clickhousePort int

	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence")
	flag.StringVar(&clickhouseHost, "clickhouse-host", "localhost", "ClickHouse host")
	flag.IntVar(&clickhousePort, "clickhouse-port", 9000, "ClickHouse native port")
	flag.StringVar(&clickhousePassword, "clickhouse-password", "", "ClickHouse password")
	flag.StringVar(&database, "database", "stellar", "Database name")
	flag.Parse()

	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}

	log.Printf("Connected to ClickHouse at %s:%d, database: %s", clickhouseHost, clickhousePort, database)

	ctx := context.Background()

	// Configure datastore
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

	// Initialize datastore
	dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create datastore"))
	}
	defer dataStore.Close()

	// Configure backend
	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 100,
		NumWorkers: 10,
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

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
		log.Fatal(errors.Wrapf(err, "failed to prepare range: %v", ledgerRange))
	}

	log.Printf("Starting ledger processing: %d - %d (%d ledgers)",
		ledgerRange.From(), ledgerRange.To(), totalLedgers)
	log.Println("Performance metrics will be displayed every 1000 ledgers")
	log.Println()

	// Progress tracking
	processedCount := 0
	lastReportedPercent := -1
	startTime := time.Now()

	// Process ledgers
	for ledgerSeq := ledgerRange.From(); ledgerSeq <= ledgerRange.To(); ledgerSeq++ {
		// Measure ledger fetch time
		_, err := backend.GetLedger(ctx, ledgerSeq)

		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
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

			log.Printf("Progress: %d/%d ledgers (%d%%) | %.2f ledgers/sec | ETA: %s",
				processedCount, totalLedgers, currentPercent, ledgersPerSec,
				formatDuration(timeRemaining))

			lastReportedPercent = currentPercent
		}
	}

	elapsed := time.Since(startTime)

	// Final summary
	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Println("                    PROCESSING COMPLETE                     ")
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Printf("  Ledgers processed:    %d\n", processedCount)
	fmt.Printf("  Total time:           %s\n", formatDuration(elapsed))
	fmt.Printf("  Average speed:        %.2f ledgers/sec\n", float64(processedCount)/elapsed.Seconds())
	fmt.Println("════════════════════════════════════════════════════════════")
}

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
