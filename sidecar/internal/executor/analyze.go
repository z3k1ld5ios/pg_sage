package executor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pg-sage/sidecar/internal/analyzer"
)

// recentAnalyzes guards per-table cooldown across all Executor
// instances in this process. The cooldown is fleet-wide so two
// databases can't both hammer the same (admin) table.
var (
	recentAnalyzesMu sync.Mutex
	recentAnalyzes   = map[string]time.Time{}
)

// checkAnalyzeCooldown returns true if the canonical table was
// analyzed within cooldownMin; it does NOT update the timestamp.
func checkAnalyzeCooldown(canonical string, cooldownMin int) bool {
	if cooldownMin <= 0 {
		return false
	}
	recentAnalyzesMu.Lock()
	defer recentAnalyzesMu.Unlock()
	last, ok := recentAnalyzes[canonical]
	if !ok {
		return false
	}
	return time.Since(last) < time.Duration(cooldownMin)*time.Minute
}

func markAnalyzed(canonical string) {
	recentAnalyzesMu.Lock()
	defer recentAnalyzesMu.Unlock()
	recentAnalyzes[canonical] = time.Now()
}

// executeAnalyze runs an ANALYZE finding through the safety
// gates (size cap, cooldown, shared semaphore) and dispatches
// it via a dedicated connection with statement_timeout set.
// Returns a non-nil error on failure so executeFinding can
// log it like any other DDL failure.
func (e *Executor) executeAnalyze(
	ctx context.Context, f analyzer.Finding,
) error {
	canonical := f.ObjectIdentifier
	if canonical == "" {
		return fmt.Errorf("analyze: empty object identifier")
	}
	tc := e.cfg.Tuner

	// Size gate: refuse tables larger than AnalyzeMaxTableMB.
	if tc.AnalyzeMaxTableMB > 0 {
		if sz, ok := f.Detail["size_mb"].(int64); ok && sz > 0 {
			if sz > tc.AnalyzeMaxTableMB {
				return fmt.Errorf(
					"analyze: table %s is %d MB, "+
						"exceeds max %d MB",
					canonical, sz, tc.AnalyzeMaxTableMB,
				)
			}
		}
	}

	// Cooldown gate.
	if checkAnalyzeCooldown(canonical, tc.AnalyzeCooldownMinutes) {
		return fmt.Errorf(
			"analyze: %s in cooldown (last run within %d min)",
			canonical, tc.AnalyzeCooldownMinutes,
		)
	}

	// Acquire semaphore slot so only N analyzes run at once
	// across the entire sidecar process.
	if e.analyzeSem != nil {
		select {
		case e.analyzeSem <- struct{}{}:
			defer func() { <-e.analyzeSem }()
		case <-ctx.Done():
			return fmt.Errorf("analyze: ctx cancelled: %w", ctx.Err())
		}
	}

	timeoutMs := tc.AnalyzeTimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 600000 // 10 min safety default
	}

	if err := runAnalyzeOnConn(
		ctx, e.pool, f.RecommendedSQL, timeoutMs,
		e.cfg.Safety.LockTimeout(),
	); err != nil {
		return err
	}
	markAnalyzed(canonical)
	return nil
}

// runAnalyzeOnConn executes ANALYZE on a dedicated pooled
// connection outside any transaction, with statement_timeout
// and lock_timeout scoped to the connection. Connection-level
// settings are reset before release.
func runAnalyzeOnConn(
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	timeoutMs int,
	lockTimeoutMs int,
) error {
	if err := ValidateExecutorSQL(sql); err != nil {
		return fmt.Errorf("analyze validation: %w", err)
	}
	if !isAnalyzeStatement(sql) {
		return fmt.Errorf(
			"analyze: expected ANALYZE statement, got %q", sql,
		)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("analyze acquire conn: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx,
		fmt.Sprintf("SET statement_timeout = %d", timeoutMs),
	)
	if err != nil {
		return fmt.Errorf("analyze set stmt_timeout: %w", err)
	}
	if lockTimeoutMs > 0 {
		_, err = conn.Exec(ctx, fmt.Sprintf(
			"SET lock_timeout = '%dms'", lockTimeoutMs,
		))
		if err != nil {
			return fmt.Errorf("analyze set lock_timeout: %w", err)
		}
	}
	_, execErr := conn.Exec(ctx, sql)

	// Reset connection-level overrides before returning to pool.
	_, _ = conn.Exec(ctx, "SET statement_timeout = 0")
	_, _ = conn.Exec(ctx, "SET lock_timeout = 0")

	if execErr != nil {
		return wrapDDLError(execErr)
	}
	return nil
}

// isAnalyzeStatement verifies the leading token is ANALYZE so
// only stat-refresh SQL reaches runAnalyzeOnConn.
func isAnalyzeStatement(sql string) bool {
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	return strings.HasPrefix(trimmed, "ANALYZE")
}
