package main

// Filename: rocksdb_stats.go
// Usage: go run rocksdb_stats.go /path/to/rocksdb

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/linxGnu/grocksdb"
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Usage: rocksdb_stats <path-to-rocksdb-directory>")
		fmt.Println("Example: rocksdb_stats /tmp/test-db")
		os.Exit(1)
	}

	dbPath := flag.Arg(0)

	// Check if directory exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Error: Directory does not exist: %s", dbPath)
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
	fmt.Printf("RocksDB Statistics for: %s\n", dbPath)
	fmt.Printf("========================================\n\n")

	// Get directory size
	dirSize, err := getDirSize(dbPath)
	if err != nil {
		log.Printf("Warning: Failed to get directory size: %v", err)
	} else {
		fmt.Printf("📁 Total Directory Size: %s\n\n", formatBytes(dirSize))
	}

	// Get RocksDB internal statistics
	getRocksDBStats(db)

	// Count files
	countFiles(dbPath)

	fmt.Println()
}

// getRocksDBStats gets accurate statistics from RocksDB
func getRocksDBStats(db *grocksdb.DB) {
	properties := []struct {
		name        string
		description string
	}{
		{"rocksdb.total-sst-files-size", "Total SST Files Size"},
		{"rocksdb.live-sst-files-size", "Live SST Files Size"},
		{"rocksdb.size-all-mem-tables", "All MemTables Size"},
		{"rocksdb.cur-size-all-mem-tables", "Current MemTables Size"},
		{"rocksdb.estimate-live-data-size", "Estimated Live Data Size"},
		{"rocksdb.estimate-num-keys", "Estimated Number of Keys"},
		{"rocksdb.num-entries-active-mem-table", "Active MemTable Entries"},
		{"rocksdb.num-entries-imm-mem-tables", "Immutable MemTable Entries"},
	}

	fmt.Println("📊 RocksDB Internal Statistics:")
	fmt.Println("─────────────────────────────────────")

	for _, prop := range properties {
		value := db.GetProperty(prop.name)
		if value != "" {
			// Try to parse as int64 for size formatting
			var size int64
			n, _ := fmt.Sscanf(value, "%d", &size)
			if n == 1 {
				// Check if this is a size property (bytes)
				if contains(prop.name, []string{"size", "Size"}) {
					fmt.Printf("  %-30s: %s\n", prop.description, formatBytes(size))
				} else {
					// It's a count
					fmt.Printf("  %-30s: %s\n", prop.description, formatNumber(size))
				}
			} else {
				fmt.Printf("  %-30s: %s\n", prop.description, value)
			}
		}
	}
	fmt.Println()
}

// countFiles counts different file types in the RocksDB directory
func countFiles(dbPath string) {
	fileTypes := map[string]int{
		".sst": 0,
		".log": 0,
	}

	var sstTotalSize int64
	var logTotalSize int64

	err := filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		ext := filepath.Ext(path)
		if _, exists := fileTypes[ext]; exists {
			fileTypes[ext]++
			if ext == ".sst" {
				sstTotalSize += info.Size()
			} else if ext == ".log" {
				logTotalSize += info.Size()
			}
		}
		return nil
	})

	if err != nil {
		log.Printf("Warning: Failed to count files: %v", err)
		return
	}

	fmt.Println("📂 File Breakdown:")
	fmt.Println("─────────────────────────────────────")
	fmt.Printf("  SST Files (data):       %d files, %s\n", fileTypes[".sst"], formatBytes(sstTotalSize))
	fmt.Printf("  LOG Files (WAL):        %d files, %s\n", fileTypes[".log"], formatBytes(logTotalSize))
	fmt.Println()

	if logTotalSize > 10*1024*1024 { // > 10 MB
		fmt.Printf("⚠️  WARNING: WAL files are large (%s). Consider flushing.\n", formatBytes(logTotalSize))
	}
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

// contains checks if a string contains any of the substrings
func contains(s string, substrs []string) bool {
	for _, substr := range substrs {
		if len(s) >= len(substr) {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
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
