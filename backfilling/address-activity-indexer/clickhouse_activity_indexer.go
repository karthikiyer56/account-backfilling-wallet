// Filename: clickhouse_activity_indexer.go
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/stellar/go/asset"
	"log"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/processors/token_transfer"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
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

// PerformanceMetrics tracks timing for different operations
type PerformanceMetrics struct {
	// Accumulated times (over 1000 operations)
	totalLedgerFetchTime  time.Duration
	totalEventProcessTime time.Duration
	totalDbWriteTime      time.Duration

	// Counters for averaging
	ledgerFetchCount  int
	eventProcessCount int
	dbWriteCount      int

	// Current batch metrics (for display)
	avgLedgerFetchMs  float64
	avgEventProcessMs float64
	avgDbWriteMs      float64

	// Total events processed in current batch window
	eventsInWindow int
}

func (m *PerformanceMetrics) recordLedgerFetch(duration time.Duration) {
	m.totalLedgerFetchTime += duration
	m.ledgerFetchCount++

	// Calculate average after 1000 samples
	if m.ledgerFetchCount >= 1000 {
		m.avgLedgerFetchMs = float64(m.totalLedgerFetchTime.Microseconds()) / float64(m.ledgerFetchCount) / 1000.0
		m.totalLedgerFetchTime = 0
		m.ledgerFetchCount = 0
	}
}

func (m *PerformanceMetrics) recordEventProcess(duration time.Duration, eventCount int) {
	m.totalEventProcessTime += duration
	m.eventProcessCount++
	m.eventsInWindow += eventCount

	// Calculate average after 1000 samples
	if m.eventProcessCount >= 1000 {
		m.avgEventProcessMs = float64(m.totalEventProcessTime.Microseconds()) / float64(m.eventProcessCount) / 1000.0
		m.totalEventProcessTime = 0
		m.eventProcessCount = 0
		m.eventsInWindow = 0
	}
}

func (m *PerformanceMetrics) recordDbWrite(duration time.Duration) {
	m.totalDbWriteTime += duration
	m.dbWriteCount++

	// Calculate average after 1000 samples
	if m.dbWriteCount >= 1000 {
		m.avgDbWriteMs = float64(m.totalDbWriteTime.Microseconds()) / float64(m.dbWriteCount) / 1000.0
		m.totalDbWriteTime = 0
		m.dbWriteCount = 0
	}
}

func (m *PerformanceMetrics) printBatchMetrics(batchSize int, rowsInserted int) {
	fmt.Println()
	fmt.Println("┌─────────────────────────────────────────────────────────────┐")
	fmt.Println("│                   BATCH PERFORMANCE METRICS                 │")
	fmt.Println("├─────────────────────────────────────────────────────────────┤")

	// Ledger fetch metrics
	fetchSamples := m.ledgerFetchCount
	if fetchSamples == 0 && m.avgLedgerFetchMs > 0 {
		fetchSamples = 1000 // Previous window
	}
	fmt.Printf("│ Ledger Fetch (GCS):                                         │\n")
	fmt.Printf("│   Avg Time:       %8.2f ms/ledger                        │\n", m.avgLedgerFetchMs)
	fmt.Printf("│   Samples:        %8d ledgers                           │\n", fetchSamples)
	if m.avgLedgerFetchMs > 0 {
		fmt.Printf("│   Throughput:     %8.1f ledgers/sec                      │\n", 1000.0/m.avgLedgerFetchMs)
	}
	fmt.Println("│                                                             │")

	// Event processing metrics
	processSamples := m.eventProcessCount
	if processSamples == 0 && m.avgEventProcessMs > 0 {
		processSamples = 1000
	}
	avgEventsPerLedger := 0.0
	if processSamples > 0 {
		avgEventsPerLedger = float64(m.eventsInWindow) / float64(processSamples)
	}
	fmt.Printf("│ Event Processing:                                           │\n")
	fmt.Printf("│   Avg Time:       %8.2f ms/ledger                        │\n", m.avgEventProcessMs)
	fmt.Printf("│   Samples:        %8d ledgers                           │\n", processSamples)
	fmt.Printf("│   Avg Events:     %8.1f events/ledger                    │\n", avgEventsPerLedger)
	if m.avgEventProcessMs > 0 {
		fmt.Printf("│   Throughput:     %8.1f ledgers/sec                      │\n", 1000.0/m.avgEventProcessMs)
	}
	fmt.Println("│                                                             │")

	// DB write metrics
	writeSamples := m.dbWriteCount
	if writeSamples == 0 && m.avgDbWriteMs > 0 {
		writeSamples = 1000
	}
	fmt.Printf("│ Database Write (Batch):                                     │\n")
	fmt.Printf("│   Avg Time:       %8.2f ms/batch                         │\n", m.avgDbWriteMs)
	fmt.Printf("│   Samples:        %8d batches                           │\n", writeSamples)
	fmt.Printf("│   This Batch:     %8d rows inserted                     │\n", rowsInserted)
	if m.avgDbWriteMs > 0 && rowsInserted > 0 {
		avgRowsPerBatch := float64(rowsInserted)
		fmt.Printf("│   Throughput:     %8.0f rows/sec                         │\n", avgRowsPerBatch/(m.avgDbWriteMs/1000.0))
	}
	fmt.Println("│                                                             │")

	// Combined metrics
	totalAvgMs := m.avgLedgerFetchMs + m.avgEventProcessMs + (m.avgDbWriteMs / float64(batchSize))
	if totalAvgMs > 0 {
		fmt.Printf("│ Combined Performance:                                       │\n")
		fmt.Printf("│   Total Avg:      %8.2f ms/ledger (fetch+process+write) │\n", totalAvgMs)
		fmt.Printf("│   Throughput:     %8.1f ledgers/sec                      │\n", 1000.0/totalAvgMs)

		// Breakdown percentages
		fetchPct := (m.avgLedgerFetchMs / totalAvgMs) * 100
		processPct := (m.avgEventProcessMs / totalAvgMs) * 100
		writePct := ((m.avgDbWriteMs / float64(batchSize)) / totalAvgMs) * 100

		fmt.Printf("│   Breakdown:      %.1f%% fetch, %.1f%% process, %.1f%% write    │\n",
			fetchPct, processPct, writePct)
	}

	fmt.Println("└─────────────────────────────────────────────────────────────┘")
	fmt.Println()
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

	// Connect to ClickHouse
	conn, err := connectClickHouse(clickhouseHost, clickhousePort, clickhousePassword, database)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

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
		BufferSize: 1000,
		NumWorkers: 100,
		RetryLimit: 3,
		RetryWait:  1 * time.Second,
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
	totalRowsInserted := 0

	// Performance metrics
	metrics := &PerformanceMetrics{}

	// Batch accumulator
	batchData := make([]EventEntry, 0, 100000)
	const batchSize = 1000

	// Process ledgers
	for ledgerSeq := ledgerRange.From(); ledgerSeq <= ledgerRange.To(); ledgerSeq++ {
		// Measure ledger fetch time
		fetchStart := time.Now()
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		fetchDuration := time.Since(fetchStart)
		metrics.recordLedgerFetch(fetchDuration)

		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}

		closedAt := ledger.ClosedAt()
		processor := token_transfer.NewEventsProcessorForUnifiedEvents(network.PublicNetworkPassphrase)

		// Measure event processing time
		processStart := time.Now()
		eventCountBefore := len(batchData)
		err = processLedgerToBatch(processor, &batchData, ledger, ledgerSeq, closedAt)
		processDuration := time.Since(processStart)
		eventsAdded := len(batchData) - eventCountBefore
		metrics.recordEventProcess(processDuration, eventsAdded)

		if err != nil {
			log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
		}

		processedCount++
		currentPercent := (processedCount * 100) / totalLedgers

		// Write batch every 1000 ledgers
		if processedCount%batchSize == 0 {
			uniqueAddresses := countUniqueAddresses(batchData)
			log.Printf("Writing batch at ledger %d (%d events, %d unique addresses)...",
				ledgerSeq, len(batchData), uniqueAddresses)

			// Measure DB write time
			writeStart := time.Now()
			rowsInserted, err := writeBatchToClickHouse(ctx, conn, database, batchData)
			writeDuration := time.Since(writeStart)
			metrics.recordDbWrite(writeDuration)

			if err != nil {
				log.Printf("Error writing batch: %v", err)
			} else {
				totalRowsInserted += rowsInserted
				log.Printf("Batch written: %d rows in %.2f ms", rowsInserted, float64(writeDuration.Microseconds())/1000.0)

				// Print detailed performance metrics
				metrics.printBatchMetrics(batchSize, rowsInserted)
			}

			// Clear batch
			batchData = make([]EventEntry, 0, 100000)
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

			log.Printf("Progress: %d/%d ledgers (%d%%) | %.2f ledgers/sec | %d total rows | ETA: %s",
				processedCount, totalLedgers, currentPercent, ledgersPerSec,
				totalRowsInserted, formatDuration(timeRemaining))

			lastReportedPercent = currentPercent
		}
	}

	// Write remaining batch
	if len(batchData) > 0 {
		uniqueAddresses := countUniqueAddresses(batchData)
		log.Printf("Writing final batch (%d events, %d unique addresses)...",
			len(batchData), uniqueAddresses)

		writeStart := time.Now()
		rowsInserted, err := writeBatchToClickHouse(ctx, conn, database, batchData)
		writeDuration := time.Since(writeStart)
		metrics.recordDbWrite(writeDuration)

		if err != nil {
			log.Printf("Error writing final batch: %v", err)
		} else {
			totalRowsInserted += rowsInserted
			log.Printf("Final batch written: %d rows in %.2f ms", rowsInserted, float64(writeDuration.Microseconds())/1000.0)

			// Print final metrics
			metrics.printBatchMetrics(processedCount%batchSize, rowsInserted)
		}
	}

	elapsed := time.Since(startTime)

	// Final summary
	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Println("                    PROCESSING COMPLETE                     ")
	fmt.Println("════════════════════════════════════════════════════════════")
	fmt.Printf("  Ledgers processed:    %d\n", processedCount)
	fmt.Printf("  Total rows inserted:  %d\n", totalRowsInserted)
	fmt.Printf("  Total time:           %s\n", formatDuration(elapsed))
	fmt.Printf("  Average speed:        %.2f ledgers/sec\n", float64(processedCount)/elapsed.Seconds())
	fmt.Printf("  Insert rate:          %.2f rows/sec\n", float64(totalRowsInserted)/elapsed.Seconds())
	fmt.Printf("  Avg rows per ledger:  %.1f\n", float64(totalRowsInserted)/float64(processedCount))
	fmt.Println("════════════════════════════════════════════════════════════")
}

func connectClickHouse(host string, port int, password, database string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
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

	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	return conn, nil
}

func writeBatchToClickHouse(ctx context.Context, conn clickhouse.Conn, database string, batchData []EventEntry) (int, error) {
	if len(batchData) == 0 {
		return 0, nil
	}

	query := fmt.Sprintf(`INSERT INTO %s.events_canonical (
		ledger_sequence, closed_at, tx_hash, transaction_index, operation_index, event_index,
		contract_address, event_type, operation_type, from_address, to_address, amount,
		asset_type, asset_code, to_muxed_info_type, to_muxed_info_text,
		to_muxed_info_id, to_muxed_info_hash
	)`, database)

	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return 0, errors.Wrap(err, "failed to prepare batch")
	}

	rowCount := 0
	for _, entry := range batchData {
		err := batch.Append(
			entry.LedgerSequence,
			entry.ClosedAt,
			entry.TxHash,
			entry.TransactionIndex,
			entry.OperationIndex,
			entry.EventIndex,
			entry.ContractAddress,
			entry.EventType,
			entry.OperationType,
			entry.FromAddress,
			entry.ToAddress,
			entry.Amount,
			entry.AssetType,
			entry.AssetCode,
			entry.ToMuxedInfoType,
			entry.ToMuxedInfoText,
			entry.ToMuxedInfoID,
			entry.ToMuxedInfoHash,
		)
		if err != nil {
			return rowCount, errors.Wrapf(err, "failed to append row")
		}
		rowCount++
	}

	if err := batch.Send(); err != nil {
		return rowCount, errors.Wrap(err, "failed to send batch")
	}

	return rowCount, nil
}

func processLedgerToBatch(processor *token_transfer.EventsProcessor, batchData *[]EventEntry,
	ledger xdr.LedgerCloseMeta, ledgerSeq uint32, closedAt time.Time) error {

	// Extract events
	events, err := processor.EventsFromLedger(ledger)
	if err != nil {
		return errors.Wrapf(err, "failed to process events from ledger %d", ledgerSeq)
	}

	// Get transaction reader for operation types
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledger)
	if err != nil {
		return errors.Wrap(err, "failed to create reader")
	}
	defer reader.Close()

	// Track event index per operation
	eventCounters := make(map[string]uint32)

	// Process each event
	for _, event := range events {
		entry := convertEventToEntry(event, ledgerSeq, closedAt, eventCounters)
		if entry != nil && isValidAddress(entry.FromAddress, entry.ToAddress) {
			*batchData = append(*batchData, *entry)
		}
	}

	return nil
}

func convertEventToEntry(event *token_transfer.TokenTransferEvent, ledgerSeq uint32, closedAt time.Time, eventCounters map[string]uint32) *EventEntry {

	if event == nil {
		return nil
	}

	meta := event.GetMeta()
	if meta == nil {
		return nil
	}

	entry := &EventEntry{
		LedgerSequence:   ledgerSeq,
		ClosedAt:         closedAt,
		TxHash:           meta.GetTxHash(),
		TransactionIndex: meta.GetTransactionIndex(),
		OperationIndex:   0,
		ContractAddress:  meta.GetContractAddress(),
		EventType:        None,
		AssetType:        None, // starts off as none
		ToMuxedInfoType:  None,
	}

	// Get operation index if present
	if meta.OperationIndex != nil {
		entry.OperationIndex = *meta.OperationIndex
	}

	// Get event index
	opKey := fmt.Sprintf("%d-%d", entry.TransactionIndex, entry.OperationIndex)
	entry.EventIndex = eventCounters[opKey]
	eventCounters[opKey]++

	// Get operation type
	if event.GetFee() == nil {
		ll := One
		entry.OperationType = &ll // just stick in some random operation type
	}

	// Extract event-specific data
	switch event.GetEventType() {
	case token_transfer.TransferEvent:
		transfer := event.GetTransfer()
		entry.EventType = token_transfer.TransferEvent
		entry.FromAddress = transfer.From
		entry.ToAddress = transfer.To
		entry.Amount = transfer.Amount
		extractAssetInfo(entry, transfer.GetAsset())
	case token_transfer.MintEvent:
		mint := event.GetMint()
		entry.EventType = token_transfer.MintEvent
		entry.ToAddress = mint.To
		entry.Amount = mint.Amount
		extractAssetInfo(entry, mint.Asset)
	case token_transfer.BurnEvent:
		burn := event.GetBurn()
		entry.EventType = token_transfer.BurnEvent
		entry.FromAddress = burn.From
		entry.Amount = burn.Amount
		extractAssetInfo(entry, burn.Asset)
	case token_transfer.ClawbackEvent:
		clawback := event.GetClawback()
		entry.EventType = token_transfer.ClawbackEvent
		entry.FromAddress = clawback.From
		entry.Amount = clawback.Amount
		extractAssetInfo(entry, clawback.Asset)
	case token_transfer.FeeEvent:
		fee := event.GetFee()
		if strings.HasPrefix(fee.Amount, "-") {
			entry.EventType = FeeRefund
			entry.Amount = strings.TrimPrefix(fee.Amount, "-")
		} else {
			entry.EventType = token_transfer.FeeEvent
			entry.Amount = fee.Amount
		}
		entry.FromAddress = fee.From
		entry.AssetType = Native
	default:
		return nil
	}

	// Extract muxed info
	if meta.GetToMuxedInfo() != nil {
		extractMuxedInfo(entry, meta.GetToMuxedInfo())
	}

	return entry
}

func extractAssetInfo(entry *EventEntry, asset *asset.Asset) {
	if asset == nil {
		return
	}
	if asset.GetNative() {
		entry.AssetType = Native
	} else {
		entry.AssetType = Issued
		entry.AssetCode = asset.GetIssuedAsset().GetAssetCode()
	}
}

func extractMuxedInfo(entry *EventEntry, muxedInfo *token_transfer.MuxedInfo) {
	if muxedInfo == nil {
		return
	}

	switch muxedInfo.GetContent().(type) {
	case *token_transfer.MuxedInfo_Text:
		entry.ToMuxedInfoType = Text
		entry.ToMuxedInfoText = muxedInfo.GetText()
	case *token_transfer.MuxedInfo_Id:
		entry.ToMuxedInfoType = Id
		entry.ToMuxedInfoID = muxedInfo.GetId()
	case *token_transfer.MuxedInfo_Hash:
		entry.ToMuxedInfoType = Hash
		entry.ToMuxedInfoHash = string(muxedInfo.GetHash())
	}
}

func isValidAddress(from, to string) bool {
	return isValid(from) || isValid(to)
}

func isValid(address string) bool {
	if address == "" {
		return false
	}
	version, _, err := strkey.DecodeAny(address)
	if err != nil {
		return false
	}
	return version == strkey.VersionByteAccountID || version == strkey.VersionByteContract
}

func countUniqueAddresses(batchData []EventEntry) int {
	uniqueAddresses := make(map[string]bool)
	for _, entry := range batchData {
		if entry.FromAddress != "" {
			uniqueAddresses[entry.FromAddress] = true
		}
		if entry.ToAddress != "" {
			uniqueAddresses[entry.ToAddress] = true
		}
	}
	return len(uniqueAddresses)
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
