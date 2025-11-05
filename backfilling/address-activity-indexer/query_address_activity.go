// Filename: query_address_activity.go
package main

import (
	"account-backfilling-wallet/backfilling/helpers"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Activity represents a single activity for an address
type Activity struct {
	Address          string    `json:"address"`
	LedgerSequence   uint32    `json:"ledger_sequence"`
	ClosedAt         time.Time `json:"closed_at"`
	TxHash           string    `json:"tx_hash"`
	TransactionIndex uint32    `json:"transaction_index"`
	OperationIndex   uint32    `json:"operation_index"`
	EventIndex       uint32    `json:"event_index"`
	ContractAddress  string    `json:"contract_address"`
	EventType        string    `json:"event_type"`
	OperationType    *uint8    `json:"operation_type,omitempty"`
	FromAddress      string    `json:"from_address,omitempty"`
	ToAddress        string    `json:"to_address,omitempty"`
	Amount           string    `json:"amount"`
	AssetType        string    `json:"asset_type"`
	AssetCode        string    `json:"asset_code,omitempty"`
	ToMuxedInfoType  string    `json:"to_muxed_info_type,omitempty"`
	ToMuxedInfoText  string    `json:"to_muxed_info_text,omitempty"`
	ToMuxedInfoID    uint64    `json:"to_muxed_info_id,omitempty"`
	ToMuxedInfoHash  string    `json:"to_muxed_info_hash,omitempty"`
}

// Cursor for pagination
type Cursor struct {
	ClosedAt         time.Time `json:"closed_at"`
	TransactionIndex uint32    `json:"transaction_index"`
	OperationIndex   uint32    `json:"operation_index"`
	EventIndex       uint32    `json:"event_index"`
}

// PaginatedResponse for results
type PaginatedResponse struct {
	Data       []Activity `json:"data"`
	NextCursor *string    `json:"next_cursor,omitempty"`
	HasMore    bool       `json:"has_more"`
}

// QueryMetrics tracks query execution metrics
type QueryMetrics struct {
	Query         string
	Parameters    []interface{}
	ExecutionTime time.Duration
	RowCount      int
}

func main() {
	// Command-line flags
	var address, cursor, month, clickhouseHost, clickhousePassword, database string
	var clickhousePort int

	flag.StringVar(&address, "address", "", "Stellar address (required)")
	flag.StringVar(&cursor, "cursor", "", "Pagination cursor (optional)")
	flag.StringVar(&month, "month", "", "Month in YYYY-MM format (optional, e.g., 2025-01)")
	flag.StringVar(&clickhouseHost, "clickhouse-host", "localhost", "ClickHouse host")
	flag.IntVar(&clickhousePort, "clickhouse-port", 9001, "ClickHouse native port")
	flag.StringVar(&clickhousePassword, "clickhouse-password", "", "ClickHouse password")
	flag.StringVar(&database, "database", "stellar", "Database name")
	flag.Parse()

	// Validate required arguments
	if address == "" {
		fmt.Println("Error: --address is required")
		fmt.Println()
		printUsage()
		os.Exit(1)
	}

	// Connect to ClickHouse
	conn, err := helpers.ConnectClickHouse(clickhouseHost, clickhousePort, clickhousePassword, database)
	if err != nil {
		fmt.Printf("Failed to connect to ClickHouse: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx := context.Background()

	// Determine query mode based on arguments
	var response *PaginatedResponse
	var metrics *QueryMetrics
	var cursorPtr *string
	if cursor != "" {
		cursorPtr = &cursor
	}

	if month != "" {
		// Monthly activities (ascending order)
		yearMonth, err := helpers.ParseMonth(month)
		if err != nil {
			fmt.Printf("Error: Invalid month format: %v\n", err)
			fmt.Println("Expected format: YYYY-MM (e.g., 2025-01)")
			os.Exit(1)
		}

		fmt.Printf("Querying activities for address: %s\n", address)
		fmt.Printf("Month: %s (ascending order)\n", month)
		if cursorPtr != nil {
			fmt.Println("Using cursor for pagination")
		}
		fmt.Println()

		response, metrics, err = GetMonthlyActivities(ctx, conn, address, yearMonth, 20, cursorPtr, database)
	} else {
		// Recent activities (descending order)
		fmt.Printf("Querying recent activities for address: %s\n", address)
		fmt.Println("Order: Most recent first (descending)")
		if cursorPtr != nil {
			fmt.Println("Using cursor for pagination")
		}
		fmt.Println()

		response, metrics, err = GetRecentActivities(ctx, conn, address, 20, cursorPtr, database)
	}

	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}

	// Display query details
	displayQueryDetails(metrics)

	// Display results
	displayResults(response, month != "")

	// Show next steps if more data available
	if response.HasMore && response.NextCursor != nil {
		fmt.Println()
		fmt.Println("════════════════════════════════════════")
		fmt.Println("More activities available!")
		fmt.Println("════════════════════════════════════════")
		fmt.Println("To get the next page, run:")
		fmt.Println()
		if month != "" {
			fmt.Printf("  ./query_address_activity \\\n")
			fmt.Printf("    --address %s \\\n", address)
			fmt.Printf("    --month %s \\\n", month)
			fmt.Printf("    --cursor \"%s\"\n", *response.NextCursor)
		} else {
			fmt.Printf("  ./query_address_activity \\\n")
			fmt.Printf("    --address %s \\\n", address)
			fmt.Printf("    --cursor \"%s\"\n", *response.NextCursor)
		}
	}
}

func GetRecentActivities(ctx context.Context, conn driver.Conn, address string, limit int, cursorToken *string, database string) (*PaginatedResponse, *QueryMetrics, error) {
	query := fmt.Sprintf(`
		SELECT 
			address,
			ledger_sequence,
			closed_at,
			tx_hash,
			transaction_index,
			operation_index,
			event_index,
			contract_address,
			event_type,
			operation_type,
			from_address,
			to_address,
			amount,
			asset_type,
			asset_code,
			to_muxed_info_type,
			to_muxed_info_text,
			to_muxed_info_id,
			to_muxed_info_hash
		FROM %s.address_activity
		WHERE address = ?
	`, database)

	args := []interface{}{address}

	if cursorToken != nil && *cursorToken != "" {
		cursor, err := DecodeCursor(*cursorToken)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid cursor: %w", err)
		}

		query += `
			AND (closed_at, transaction_index, operation_index, event_index) < (?, ?, ?, ?)
		`
		args = append(args, cursor.ClosedAt, cursor.TransactionIndex,
			cursor.OperationIndex, cursor.EventIndex)
	}

	query += `
		ORDER BY closed_at DESC, ledger_sequence DESC, transaction_index DESC, 
		         operation_index DESC, event_index DESC
		LIMIT ?
	`
	args = append(args, limit)

	return executeQuery(ctx, conn, query, args, limit)
}

func GetMonthlyActivities(ctx context.Context, conn driver.Conn, address string, yearMonth int, limit int, cursorToken *string, database string) (*PaginatedResponse, *QueryMetrics, error) {
	query := fmt.Sprintf(`
		SELECT 
			address,
			ledger_sequence,
			closed_at,
			tx_hash,
			transaction_index,
			operation_index,
			event_index,
			contract_address,
			event_type,
			operation_type,
			from_address,
			to_address,
			amount,
			asset_type,
			asset_code,
			to_muxed_info_type,
			to_muxed_info_text,
			to_muxed_info_id,
			to_muxed_info_hash
		FROM %s.address_activity
		WHERE address = ?
		  AND toYYYYMM(closed_at) = ?
	`, database)

	args := []interface{}{address, yearMonth}

	if cursorToken != nil && *cursorToken != "" {
		cursor, err := DecodeCursor(*cursorToken)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid cursor: %w", err)
		}

		// For ascending order, use > instead of
		query += `
			AND (closed_at, transaction_index, operation_index, event_index) > (?, ?, ?, ?)
		`
		args = append(args, cursor.ClosedAt, cursor.TransactionIndex,
			cursor.OperationIndex, cursor.EventIndex)
	}

	// Ascending order for monthly queries
	query += `
		ORDER BY closed_at ASC, ledger_sequence ASC, transaction_index ASC, 
		         operation_index ASC, event_index ASC
		LIMIT ?
	`
	args = append(args, limit)

	return executeQuery(ctx, conn, query, args, limit)
}

func executeQuery(ctx context.Context, conn driver.Conn, query string, args []interface{}, limit int) (*PaginatedResponse, *QueryMetrics, error) {
	// Start timing
	startTime := time.Now()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query activities: %w", err)
	}
	defer rows.Close()

	activities := []Activity{}
	for rows.Next() {
		var activity Activity
		var opType *uint8

		err := rows.Scan(
			&activity.Address,
			&activity.LedgerSequence,
			&activity.ClosedAt,
			&activity.TxHash,
			&activity.TransactionIndex,
			&activity.OperationIndex,
			&activity.EventIndex,
			&activity.ContractAddress,
			&activity.EventType,
			&opType,
			&activity.FromAddress,
			&activity.ToAddress,
			&activity.Amount,
			&activity.AssetType,
			&activity.AssetCode,
			&activity.ToMuxedInfoType,
			&activity.ToMuxedInfoText,
			&activity.ToMuxedInfoID,
			&activity.ToMuxedInfoHash,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan row: %w", err)
		}

		activity.OperationType = opType
		activities = append(activities, activity)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row iteration error: %w", err)
	}

	// End timing
	executionTime := time.Since(startTime)

	// Create metrics
	metrics := &QueryMetrics{
		Query:         query,
		Parameters:    args,
		ExecutionTime: executionTime,
		RowCount:      len(activities),
	}

	// Generate cursor
	var nextCursor *string
	hasMore := len(activities) == limit
	if hasMore {
		lastActivity := activities[len(activities)-1]
		cursor := Cursor{
			ClosedAt:         lastActivity.ClosedAt,
			TransactionIndex: lastActivity.TransactionIndex,
			OperationIndex:   lastActivity.OperationIndex,
			EventIndex:       lastActivity.EventIndex,
		}
		encoded, err := EncodeCursor(cursor)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode cursor: %w", err)
		}
		nextCursor = &encoded
	}

	return &PaginatedResponse{
		Data:       activities,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, metrics, nil
}

func EncodeCursor(cursor Cursor) (string, error) {
	jsonBytes, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(jsonBytes), nil
}

func DecodeCursor(token string) (*Cursor, error) {
	jsonBytes, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}

	var cursor Cursor
	if err := json.Unmarshal(jsonBytes, &cursor); err != nil {
		return nil, err
	}

	return &cursor, nil
}

func displayQueryDetails(metrics *QueryMetrics) {
	fmt.Println("┌────────────────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│                             QUERY DETAILS                                      │")
	fmt.Println("└────────────────────────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// Format and display the query
	fmt.Println("SQL Query:")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	formattedQuery := formatQuery(metrics.Query)
	fmt.Println(formattedQuery)
	fmt.Println()

	// Display parameters
	fmt.Println("Parameters:")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	for i, param := range metrics.Parameters {
		fmt.Printf("  [%d] %v (%T)\n", i+1, formatParameter(param), param)
	}
	fmt.Println()

	// Display timing
	fmt.Println("Execution Metrics:")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  Execution Time:   %s (%.2f ms)\n", metrics.ExecutionTime, float64(metrics.ExecutionTime.Microseconds())/1000.0)
	fmt.Printf("  Rows Returned:    %d\n", metrics.RowCount)
	if metrics.ExecutionTime.Milliseconds() > 0 {
		fmt.Printf("  Rows/Second:      %.0f\n", float64(metrics.RowCount)/(float64(metrics.ExecutionTime.Milliseconds())/1000.0))
	}
	fmt.Println()
	fmt.Println("═════════════════════════════════════════════════════════════════════════════════")
	fmt.Println()
}

func formatQuery(query string) string {
	// Remove leading/trailing whitespace from each line
	lines := strings.Split(query, "\n")
	var formatted []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			formatted = append(formatted, "  "+trimmed)
		}
	}

	return strings.Join(formatted, "\n")
}

func formatParameter(param interface{}) string {
	switch v := param.(type) {
	case time.Time:
		return v.Format("2006-01-02 15:04:05.000 MST")
	case string:
		if len(v) > 50 {
			return v[:50] + "..."
		}
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func displayResults(response *PaginatedResponse, isMonthly bool) {
	if len(response.Data) == 0 {
		fmt.Println("No activities found.")
		return
	}

	fmt.Printf("Found %d activities\n", len(response.Data))
	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════════════════════════════")

	for i, activity := range response.Data {
		fmt.Printf("\n[%d] Activity\n", i+1)
		fmt.Println("────────────────────────────────────────────────────────────────────────────────")
		fmt.Printf("  Time:             %s\n", activity.ClosedAt.Format("2006-01-02 15:04:05 MST"))
		fmt.Printf("  Ledger:           %d\n", activity.LedgerSequence)
		fmt.Printf("  Transaction:      %s (tx: %d, op: %d, event: %d)\n",
			activity.TxHash, activity.TransactionIndex, activity.OperationIndex, activity.EventIndex)
		fmt.Printf("  Event Type:       %s\n", activity.EventType)

		if activity.OperationType != nil {
			fmt.Printf("  Operation Type:   %s\n", getOperationTypeName(*activity.OperationType))
		}

		if activity.FromAddress != "" {
			fmt.Printf("  From:             %s\n", activity.FromAddress)
		}
		if activity.ToAddress != "" {
			fmt.Printf("  To:               %s\n", activity.ToAddress)
		}

		fmt.Printf("  Amount:           %s\n", activity.Amount)

		if activity.AssetType == "native" {
			fmt.Printf("  Asset:            XLM (native)\n")
		} else if activity.AssetType == "issued" {
			fmt.Printf("  Asset:            %s (issued)\n", activity.AssetCode)
		}

		if activity.ContractAddress != "" {
			fmt.Printf("  Contract:         %s\n", activity.ContractAddress)
		}
	}

	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════════════════════════════")
	fmt.Println()

	// Summary
	fmt.Printf("Summary:\n")
	fmt.Printf("  Total activities: %d\n", len(response.Data))
	if len(response.Data) > 0 {
		if isMonthly {
			fmt.Printf("  Date range:       %s to %s (ascending)\n",
				response.Data[0].ClosedAt.Format("2006-01-02"),
				response.Data[len(response.Data)-1].ClosedAt.Format("2006-01-02"))
		} else {
			fmt.Printf("  Date range:       %s to %s (descending)\n",
				response.Data[0].ClosedAt.Format("2006-01-02"),
				response.Data[len(response.Data)-1].ClosedAt.Format("2006-01-02"))
		}
	}
	fmt.Printf("  Has more:         %v\n", response.HasMore)

	if response.NextCursor != nil {
		fmt.Printf("  Next cursor:      %s\n", *response.NextCursor)
	}
}

func getOperationTypeName(opType uint8) string {
	operationTypes := map[uint8]string{
		0:  "CREATE_ACCOUNT",
		1:  "PAYMENT",
		2:  "PATH_PAYMENT_STRICT_RECEIVE",
		3:  "MANAGE_SELL_OFFER",
		4:  "CREATE_PASSIVE_SELL_OFFER",
		5:  "SET_OPTIONS",
		6:  "CHANGE_TRUST",
		7:  "ALLOW_TRUST",
		8:  "ACCOUNT_MERGE",
		9:  "INFLATION",
		10: "MANAGE_DATA",
		11: "BUMP_SEQUENCE",
		12: "MANAGE_BUY_OFFER",
		13: "PATH_PAYMENT_STRICT_SEND",
		14: "CREATE_CLAIMABLE_BALANCE",
		15: "CLAIM_CLAIMABLE_BALANCE",
		16: "BEGIN_SPONSORING_FUTURE_RESERVES",
		17: "END_SPONSORING_FUTURE_RESERVES",
		18: "REVOKE_SPONSORSHIP",
		19: "CLAWBACK",
		20: "CLAWBACK_CLAIMABLE_BALANCE",
		21: "SET_TRUST_LINE_FLAGS",
		22: "LIQUIDITY_POOL_DEPOSIT",
		23: "LIQUIDITY_POOL_WITHDRAW",
		24: "INVOKE_HOST_FUNCTION",
		25: "EXTEND_FOOTPRINT_TTL",
		26: "RESTORE_FOOTPRINT",
	}

	if name, exists := operationTypes[opType]; exists {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", opType)
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  query_address_activity --address <address> [options]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --address <address>          Stellar address (required)")
	fmt.Println("  --cursor <cursor>            Pagination cursor (optional)")
	fmt.Println("  --month <YYYY-MM>            Query specific month (optional, e.g., 2025-01)")
	fmt.Println("  --clickhouse-host <host>     ClickHouse host (default: localhost)")
	fmt.Println("  --clickhouse-port <port>     ClickHouse port (default: 9000)")
	fmt.Println("  --clickhouse-password <pwd>  ClickHouse password (default: empty)")
	fmt.Println("  --database <name>            Database name (default: stellar)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println()
	fmt.Println("  # Get last 20 recent activities (descending order)")
	fmt.Println("  ./query_address_activity --address GABC123...")
	fmt.Println()
	fmt.Println("  # Get next page with cursor (descending order)")
	fmt.Println("  ./query_address_activity --address GABC123... --cursor \"eyJjbG9z...\"")
	fmt.Println()
	fmt.Println("  # Get activities for January 2025 (ascending order)")
	fmt.Println("  ./query_address_activity --address GABC123... --month 2025-01")
	fmt.Println()
	fmt.Println("  # Get next page of January activities with cursor (ascending order)")
	fmt.Println("  ./query_address_activity --address GABC123... --month 2025-01 --cursor \"eyJjbG9z...\"")
	fmt.Println()
}
