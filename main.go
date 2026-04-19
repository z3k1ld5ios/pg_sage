package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, cancel := signal.Notifyall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		log.Fatalf( err)
	}
}

// run is the real entry point, separated for testability.
func run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL environment variable is required")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pinging postgres: %w", err)
	}

	log.Println("pg_sage: connected to postgres")

	analyzer := NewAnalyzer(pool)

	report, err := analyzer.Analyze(ctx)
	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	printReport(report)
	return nil
}

// printReport writes a human-readable summary of the analysis report to stdout.
func printReport(r *Report) {
	if len(r.DuplicateIndexes) == 0 && len(r.UnusedIndexes) == 0 {
		fmt.Println("No issues found. Your schema looks healthy!")
		return
	}

	if len(r.DuplicateIndexes) > 0 {
		fmt.Printf("\n=== Duplicate Indexes (%d) ===\n", len(r.DuplicateIndexes))
		for _, d := range r.DuplicateIndexes {
			fmt.Printf("  [%s] %s duplicates %s\n", d.Table, d.IndexName, d.DuplicateOf)
		}
	}

	if len(r.UnusedIndexes) > 0 {
		fmt.Printf("\n=== Unused Indexes (%d) ===\n", len(r.UnusedIndexes))
		for _, u := range r.UnusedIndexes {
			// Note: printing the drop statement makes it easy to copy-paste fixes
			fmt.Printf("  [%s] %s (scans: %d)\n", u.Table, u.IndexName, u.IndexScans)
			fmt.Printf("    -> DROP INDEX CONCURRENTLY %s;\n", u.IndexName)
		}
	}
}
