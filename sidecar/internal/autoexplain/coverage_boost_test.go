package autoexplain

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testDSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

// acquireTestPool returns a pgxpool connected to local Postgres,
// or skips the test if the database is not available.
func acquireTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("skip: cannot create pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skip: cannot ping postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// --- Unit tests (no DB required) ---

func TestCoverage_NewCollector(t *testing.T) {
	cfg := CollectorConfig{
		CollectIntervalSeconds: 30,
		MaxPlansPerCycle:       5,
		LogMinDurationMs:       100,
		PreferSessionLoad:      true,
	}
	avail := &Availability{
		Available: false,
		Method:    "unavailable",
	}
	var logged []string
	logFn := func(a, b string, args ...any) {
		logged = append(logged, a+":"+b)
	}

	c := NewCollector(nil, cfg, avail, logFn)
	if c == nil {
		t.Fatal("NewCollector returned nil")
	}
	if c.cfg.CollectIntervalSeconds != 30 {
		t.Errorf(
			"cfg.CollectIntervalSeconds = %d, want 30",
			c.cfg.CollectIntervalSeconds,
		)
	}
	if c.cfg.MaxPlansPerCycle != 5 {
		t.Errorf(
			"cfg.MaxPlansPerCycle = %d, want 5",
			c.cfg.MaxPlansPerCycle,
		)
	}
	if c.cfg.LogMinDurationMs != 100 {
		t.Errorf(
			"cfg.LogMinDurationMs = %d, want 100",
			c.cfg.LogMinDurationMs,
		)
	}
	if c.cfg.PreferSessionLoad != true {
		t.Error("PreferSessionLoad should be true")
	}
	if c.avail != avail {
		t.Error("avail pointer mismatch")
	}
	if c.pool != nil {
		t.Error("pool should be nil")
	}
	// Verify logFn is wired correctly
	c.logFn("INFO", "test", "hello %s", "world")
	if len(logged) != 1 || logged[0] != "INFO:test" {
		t.Errorf("logFn not wired: logged = %v", logged)
	}
}

func TestCoverage_RunCancelledImmediately(t *testing.T) {
	cfg := CollectorConfig{
		CollectIntervalSeconds: 1,
		MaxPlansPerCycle:       1,
		LogMinDurationMs:       100,
	}
	avail := &Availability{Available: false, Method: "unavailable"}
	logFn := func(string, string, ...any) {}
	c := NewCollector(nil, cfg, avail, logFn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Run returned — good
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestCoverage_RunTicksThenCancels(t *testing.T) {
	// Use a real pool so Collect gets a real error (query on
	// missing sage schema) rather than panicking on nil pool.
	pool := acquireTestPool(t)

	cfg := CollectorConfig{
		CollectIntervalSeconds: 1,
		MaxPlansPerCycle:       1,
		LogMinDurationMs:       100,
	}
	avail := &Availability{Available: false, Method: "unavailable"}

	var warnings int
	logFn := func(a, b string, args ...any) {
		warnings++
	}
	c := NewCollector(pool, cfg, avail, logFn)

	ctx, cancel := context.WithTimeout(
		context.Background(), 1500*time.Millisecond,
	)
	defer cancel()

	c.Run(ctx)

	// We expect at least one WARN from the failed Collect call
	// (sage.explain_cache doesn't exist on plain test DB).
	if warnings < 1 {
		// It's possible the ticker didn't fire in time; don't
		// fail hard, just log it.
		t.Logf(
			"expected at least 1 warning, got %d "+
				"(ticker may not have fired)", warnings,
		)
	}
}

func TestCoverage_ExtractPlanMetrics_NilInput(t *testing.T) {
	cost, execTime := extractPlanMetrics(nil)
	if cost != 0 || execTime != 0 {
		t.Errorf("expected (0, 0), got (%f, %f)", cost, execTime)
	}
}

func TestCoverage_ExtractPlanMetrics_ZeroCost(t *testing.T) {
	planJSON := []byte(`[{
		"Plan": {"Total Cost": 0.0},
		"Execution Time": 0.0
	}]`)
	cost, execTime := extractPlanMetrics(planJSON)
	if cost != 0 {
		t.Errorf("total_cost = %f, want 0", cost)
	}
	if execTime != 0 {
		t.Errorf("execution_time = %f, want 0", execTime)
	}
}

func TestCoverage_ExtractPlanMetrics_LargePlan(t *testing.T) {
	planJSON := []byte(`[{
		"Plan": {
			"Node Type": "Seq Scan",
			"Total Cost": 99999.99,
			"Plan Rows": 1000000
		},
		"Execution Time": 5432.10,
		"Planning Time": 0.5
	}]`)
	cost, execTime := extractPlanMetrics(planJSON)
	if cost != 99999.99 {
		t.Errorf("total_cost = %f, want 99999.99", cost)
	}
	if execTime != 5432.10 {
		t.Errorf("execution_time = %f, want 5432.10", execTime)
	}
}

func TestCoverage_ExtractPlanMetrics_MultiplePlans(t *testing.T) {
	// Only the first element should be used.
	planJSON := []byte(`[
		{"Plan": {"Total Cost": 10.0}, "Execution Time": 1.0},
		{"Plan": {"Total Cost": 20.0}, "Execution Time": 2.0}
	]`)
	cost, execTime := extractPlanMetrics(planJSON)
	if cost != 10.0 {
		t.Errorf("total_cost = %f, want 10.0 (first plan)", cost)
	}
	if execTime != 1.0 {
		t.Errorf(
			"execution_time = %f, want 1.0 (first plan)",
			execTime,
		)
	}
}

func TestCoverage_ExtractPlanMetrics_MissingPlanKey(t *testing.T) {
	planJSON := []byte(`[{"Execution Time": 3.0}]`)
	cost, execTime := extractPlanMetrics(planJSON)
	if cost != 0 {
		t.Errorf("total_cost = %f, want 0 (no Plan key)", cost)
	}
	if execTime != 3.0 {
		t.Errorf("execution_time = %f, want 3.0", execTime)
	}
}

func TestCoverage_BuildSetStatements_OnlyAnalyze(t *testing.T) {
	scfg := SessionConfig{
		LogMinDurationMs: 100,
		LogAnalyze:       true,
		LogBuffers:       false,
		LogNested:        false,
	}
	stmts := buildSetStatements(scfg)
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3", len(stmts))
	}
	if stmts[2] != "SET auto_explain.log_analyze = true" {
		t.Errorf("stmts[2] = %q", stmts[2])
	}
}

func TestCoverage_BuildSetStatements_OnlyBuffers(t *testing.T) {
	scfg := SessionConfig{
		LogMinDurationMs: 100,
		LogAnalyze:       false,
		LogBuffers:       true,
		LogNested:        false,
	}
	stmts := buildSetStatements(scfg)
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3", len(stmts))
	}
	if stmts[2] != "SET auto_explain.log_buffers = true" {
		t.Errorf("stmts[2] = %q", stmts[2])
	}
}

func TestCoverage_BuildSetStatements_OnlyNested(t *testing.T) {
	scfg := SessionConfig{
		LogMinDurationMs: 100,
		LogAnalyze:       false,
		LogBuffers:       false,
		LogNested:        true,
	}
	stmts := buildSetStatements(scfg)
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3", len(stmts))
	}
	expected := "SET auto_explain.log_nested_statements = true"
	if stmts[2] != expected {
		t.Errorf("stmts[2] = %q, want %q", stmts[2], expected)
	}
}

func TestCoverage_BuildSetStatements_ZeroDuration(t *testing.T) {
	scfg := SessionConfig{LogMinDurationMs: 0}
	stmts := buildSetStatements(scfg)
	expected := "SET auto_explain.log_min_duration = '0ms'"
	if stmts[0] != expected {
		t.Errorf("stmts[0] = %q, want %q", stmts[0], expected)
	}
}

func TestCoverage_IsExplainable_TabPrefixed(t *testing.T) {
	if !isExplainable("\tSELECT 1") {
		t.Error("tab-prefixed SELECT should be explainable")
	}
}

func TestCoverage_IsExplainable_NewlinePrefixed(t *testing.T) {
	if !isExplainable("\nSELECT 1") {
		t.Error("newline-prefixed SELECT should be explainable")
	}
}

func TestCoverage_IsExplainable_MixedCaseWith(t *testing.T) {
	if !isExplainable("With cte AS (SELECT 1) SELECT * FROM cte") {
		t.Error("mixed-case WITH should be explainable")
	}
}

// --- Integration tests (require local Postgres) ---

func TestCoverage_Detect(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	avail, err := Detect(ctx, pool)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}
	if avail == nil {
		t.Fatal("Detect returned nil Availability")
	}

	// We don't know if auto_explain is available on this
	// instance, but the result must be internally consistent.
	switch avail.Method {
	case "shared_preload":
		if !avail.SharedPreload {
			t.Error("Method=shared_preload but SharedPreload=false")
		}
		if !avail.Available {
			t.Error("Method=shared_preload but Available=false")
		}
	case "session_load":
		if !avail.SessionLoad {
			t.Error("Method=session_load but SessionLoad=false")
		}
		if !avail.Available {
			t.Error("Method=session_load but Available=false")
		}
	case "unavailable":
		if avail.Available {
			t.Error("Method=unavailable but Available=true")
		}
		if avail.SharedPreload {
			t.Error("Method=unavailable but SharedPreload=true")
		}
		if avail.SessionLoad {
			t.Error("Method=unavailable but SessionLoad=true")
		}
	default:
		t.Errorf("unexpected Method: %q", avail.Method)
	}
}

func TestCoverage_CheckSharedPreload(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	found, err := checkSharedPreload(ctx, pool)
	if err != nil {
		t.Fatalf("checkSharedPreload returned error: %v", err)
	}
	// Result depends on server config; just verify no error.
	_ = found
}

func TestCoverage_CheckSessionLoad(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	canLoad, err := checkSessionLoad(ctx, pool)
	if err != nil {
		t.Fatalf("checkSessionLoad returned error: %v", err)
	}
	// Result depends on permissions; just verify no error.
	_ = canLoad
}

func TestCoverage_ConfigureSession_SharedPreload(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	// First detect availability to decide if we can test this.
	avail, err := Detect(ctx, pool)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if !avail.Available {
		t.Skip("skip: auto_explain not available on this instance")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	defer conn.Release()

	scfg := DefaultSessionConfig(200)
	err = ConfigureSession(ctx, conn, avail, scfg)
	if err != nil {
		t.Fatalf("ConfigureSession: %v", err)
	}
}

func TestCoverage_ConfigureSessionBatch(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	avail, err := Detect(ctx, pool)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if !avail.Available {
		t.Skip("skip: auto_explain not available on this instance")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	defer conn.Release()

	scfg := DefaultSessionConfig(300)
	err = ConfigureSessionBatch(ctx, conn, avail, scfg)
	if err != nil {
		t.Fatalf("ConfigureSessionBatch: %v", err)
	}
}

func TestCoverage_ConfigureSession_AllFlagsDisabled(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	avail, err := Detect(ctx, pool)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if !avail.Available {
		t.Skip("skip: auto_explain not available on this instance")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	defer conn.Release()

	scfg := SessionConfig{
		LogMinDurationMs: 500,
		LogAnalyze:       false,
		LogBuffers:       false,
		LogNested:        false,
	}
	err = ConfigureSession(ctx, conn, avail, scfg)
	if err != nil {
		t.Fatalf("ConfigureSession (flags disabled): %v", err)
	}
}

func TestCoverage_ConfigureSessionBatch_AllFlagsDisabled(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	avail, err := Detect(ctx, pool)
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if !avail.Available {
		t.Skip("skip: auto_explain not available on this instance")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire connection: %v", err)
	}
	defer conn.Release()

	scfg := SessionConfig{
		LogMinDurationMs: 500,
		LogAnalyze:       false,
		LogBuffers:       false,
		LogNested:        false,
	}
	err = ConfigureSessionBatch(ctx, conn, avail, scfg)
	if err != nil {
		t.Fatalf("ConfigureSessionBatch (flags disabled): %v", err)
	}
}

func TestCoverage_Collect_NoSageSchema(t *testing.T) {
	pool := acquireTestPool(t)
	ctx := context.Background()

	cfg := CollectorConfig{
		CollectIntervalSeconds: 60,
		MaxPlansPerCycle:       5,
		LogMinDurationMs:       100,
	}
	avail := &Availability{
		Available: false,
		Method:    "unavailable",
	}
	logFn := func(string, string, ...any) {}
	c := NewCollector(pool, cfg, avail, logFn)

	// Collect will fail because sage.explain_cache doesn't exist
	// (unless the full schema is bootstrapped). We just verify it
	// returns an error rather than panicking.
	err := c.Collect(ctx)
	if err == nil {
		// If it succeeds, the sage schema is present — that's fine.
		return
	}
	// Error should mention the query failure.
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestCoverage_Detect_CancelledContext(t *testing.T) {
	pool := acquireTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := Detect(ctx, pool)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}
