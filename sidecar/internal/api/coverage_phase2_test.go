package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pg-sage/sidecar/internal/auth"
	"github.com/pg-sage/sidecar/internal/config"
	"github.com/pg-sage/sidecar/internal/crypto"
	"github.com/pg-sage/sidecar/internal/fleet"
	"github.com/pg-sage/sidecar/internal/notify"
	"github.com/pg-sage/sidecar/internal/schema"
	"github.com/pg-sage/sidecar/internal/store"
)

// ================================================================
// Test infrastructure: connect to DB, bootstrap schema, skip if
// unavailable.
// ================================================================

var (
	p2Pool     *pgxpool.Pool
	p2PoolOnce sync.Once
	p2PoolErr  error
	p2Key      = crypto.DeriveKey("phase2-test-key")
)

func phase2DSN() string {
	if v := os.Getenv("SAGE_DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/" +
		"postgres?sslmode=disable"
}

func phase2RequireDB(t *testing.T) (
	*pgxpool.Pool, context.Context,
) {
	t.Helper()
	ctx := context.Background()
	p2PoolOnce.Do(func() {
		dsn := phase2DSN()
		qctx, cancel := context.WithTimeout(
			ctx, 15*time.Second)
		defer cancel()

		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			p2PoolErr = fmt.Errorf("parsing DSN: %w", err)
			return
		}

		p2Pool, p2PoolErr = pgxpool.NewWithConfig(qctx, cfg)
		if p2PoolErr != nil {
			return
		}

		if err := p2Pool.Ping(qctx); err != nil {
			p2PoolErr = fmt.Errorf("ping: %w", err)
			p2Pool.Close()
			p2Pool = nil
			return
		}

		if err := schema.Bootstrap(qctx, p2Pool); err != nil {
			p2PoolErr = fmt.Errorf("bootstrap: %w", err)
			p2Pool.Close()
			p2Pool = nil
			return
		}
		schema.ReleaseAdvisoryLock(qctx, p2Pool)

		if err := schema.EnsureDatabasesTable(
			qctx, p2Pool); err != nil {
			p2PoolErr = fmt.Errorf(
				"ensure databases: %w", err)
			return
		}

		if err := schema.MigrateConfigSchema(
			qctx, p2Pool); err != nil {
			p2PoolErr = fmt.Errorf(
				"migrate config: %w", err)
			return
		}
	})

	if p2PoolErr != nil {
		t.Skipf("database unavailable: %v", p2PoolErr)
	}
	return p2Pool, ctx
}

// phase2CleanTables truncates tables used in tests so tests are
// independent.
func phase2CleanTables(
	t *testing.T, pool *pgxpool.Pool, ctx context.Context,
) {
	t.Helper()
	tables := []string{
		"sage.notification_log",
		"sage.notification_rules",
		"sage.notification_channels",
		"sage.alert_log",
		"sage.query_hints",
		"sage.snapshots",
		"sage.action_queue",
		"sage.action_log",
		"sage.findings",
		"sage.sessions",
		"sage.config_audit",
		"sage.config",
		"sage.databases",
		"sage.users",
	}
	for _, tbl := range tables {
		_, err := pool.Exec(ctx,
			"DELETE FROM "+tbl)
		if err != nil {
			// Some tables may not exist or have FK issues;
			// best-effort.
			t.Logf("clean %s: %v", tbl, err)
		}
	}
}

// phase2MgrWithPool creates a fleet manager backed by a real pool.
func phase2MgrWithPool(
	pool *pgxpool.Pool,
) *fleet.DatabaseManager {
	cfg := &config.Config{
		Mode:  "standalone",
		Trust: config.TrustConfig{Level: "advisory"},
	}
	mgr := fleet.NewManager(cfg)
	mgr.RegisterInstance(&fleet.DatabaseInstance{
		Name: "testdb",
		Pool: pool,
		Config: config.DatabaseConfig{
			Name: "testdb",
		},
		Status: &fleet.InstanceStatus{
			Connected:  true,
			PGVersion:  "16",
			TrustLevel: "advisory",
		},
	})
	return mgr
}

// phase2EnsureUser creates a user in sage.users and returns its ID.
// The user can be referenced by config FK constraints.
func phase2EnsureUser(
	t *testing.T, pool *pgxpool.Pool, ctx context.Context,
) int {
	t.Helper()
	id, err := auth.CreateUser(ctx, pool,
		"phase2@test.com", "testpass123", "admin")
	if err != nil {
		// May already exist from a previous sub-test.
		if !strings.Contains(err.Error(), "duplicate") &&
			!strings.Contains(err.Error(), "exists") {
			t.Fatalf("create user: %v", err)
		}
		// Fetch existing user ID.
		err = pool.QueryRow(ctx,
			`SELECT id FROM sage.users
			 WHERE email = 'phase2@test.com'`).Scan(&id)
		if err != nil {
			t.Fatalf("query user id: %v", err)
		}
	}
	return id
}

// ================================================================
// queryFindings / scanFindingRows
// ================================================================

func TestPhase2_QueryFindings_EmptyTable(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	f := fleet.FindingFilters{
		Status: "open",
		Limit:  10,
		Offset: 0,
	}
	findings, total, err := queryFindings(
		ctx, pool, f, "testdb")
	if err != nil {
		t.Fatalf("queryFindings: %v", err)
	}
	if total != 0 {
		t.Errorf("total: got %d, want 0", total)
	}
	if len(findings) != 0 {
		t.Errorf("findings: got %d, want 0", len(findings))
	}
}

func TestPhase2_QueryFindings_WithData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Insert test findings.
	_, err := pool.Exec(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_type, object_identifier, recommendation,
		  recommended_sql)
		 VALUES
		 ('index_health', 'critical', 'Unused index',
		  '{"table":"users"}', 'open',
		  'index', 'idx_users_email',
		  'Drop the index',
		  'DROP INDEX idx_users_email'),
		 ('query_perf', 'warning', 'Slow query',
		  '{"query":"SELECT 1"}', 'open',
		  'query', 'q1', 'Add index', NULL),
		 ('vacuum', 'info', 'Dead tuples',
		  '{}', 'suppressed',
		  NULL, NULL, NULL, NULL)`)
	if err != nil {
		t.Fatalf("inserting findings: %v", err)
	}

	// Query open findings only, severity ascending
	// (CASE maps critical=1, warning=2, info=3 so ASC
	// puts critical first).
	f := fleet.FindingFilters{
		Status: "open",
		Sort:   "severity",
		Order:  "asc",
		Limit:  50,
		Offset: 0,
	}
	findings, total, err := queryFindings(
		ctx, pool, f, "testdb")
	if err != nil {
		t.Fatalf("queryFindings: %v", err)
	}
	if total != 2 {
		t.Errorf("total: got %d, want 2", total)
	}
	if len(findings) != 2 {
		t.Errorf("findings: got %d, want 2", len(findings))
	}

	// First finding should be critical.
	if findings[0]["severity"] != "critical" {
		t.Errorf("first severity: got %v, want critical",
			findings[0]["severity"])
	}
	if findings[0]["database_name"] != "testdb" {
		t.Errorf("database_name: got %v, want testdb",
			findings[0]["database_name"])
	}
	if findings[0]["object_type"] != "index" {
		t.Errorf("object_type: got %v, want index",
			findings[0]["object_type"])
	}
	if findings[0]["recommendation"] != "Drop the index" {
		t.Errorf("recommendation: got %v",
			findings[0]["recommendation"])
	}
}

func TestPhase2_QueryFindings_SeverityFilter(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES
		 ('a', 'critical', 'F1', '{}', 'open', 'obj1'),
		 ('b', 'warning', 'F2', '{}', 'open', 'obj2')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	f := fleet.FindingFilters{
		Status:   "open",
		Severity: "critical",
		Limit:    50,
	}
	findings, total, err := queryFindings(
		ctx, pool, f, "testdb")
	if err != nil {
		t.Fatalf("queryFindings: %v", err)
	}
	if total != 1 {
		t.Errorf("total: got %d, want 1", total)
	}
	if len(findings) != 1 {
		t.Errorf("findings: got %d, want 1", len(findings))
	}
}

func TestPhase2_QueryFindings_CategoryFilter(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES
		 ('index_health', 'warning', 'F1', '{}', 'open', 'o1'),
		 ('vacuum', 'warning', 'F2', '{}', 'open', 'o2')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	f := fleet.FindingFilters{
		Status:   "open",
		Category: "vacuum",
		Limit:    50,
	}
	findings, total, err := queryFindings(
		ctx, pool, f, "db1")
	if err != nil {
		t.Fatalf("queryFindings: %v", err)
	}
	if total != 1 {
		t.Errorf("total: got %d, want 1", total)
	}
	if len(findings) != 1 {
		t.Errorf("len: got %d, want 1", len(findings))
	}
	if findings[0]["category"] != "vacuum" {
		t.Errorf("category: got %v, want vacuum",
			findings[0]["category"])
	}
}

func TestPhase2_QueryFindings_Pagination(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	for i := 0; i < 5; i++ {
		_, err := pool.Exec(ctx,
			`INSERT INTO sage.findings
			 (category, severity, title, detail, status,
			  object_identifier)
			 VALUES ($1, 'info', $2, '{}', 'open', $3)`,
			fmt.Sprintf("cat_%d", i),
			fmt.Sprintf("Finding %d", i),
			fmt.Sprintf("obj_%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	f := fleet.FindingFilters{
		Status: "open",
		Sort:   "created_at",
		Order:  "asc",
		Limit:  2,
		Offset: 2,
	}
	findings, total, err := queryFindings(
		ctx, pool, f, "db")
	if err != nil {
		t.Fatalf("queryFindings: %v", err)
	}
	if total != 5 {
		t.Errorf("total: got %d, want 5", total)
	}
	if len(findings) != 2 {
		t.Errorf("findings: got %d, want 2", len(findings))
	}
}

// ================================================================
// queryFindingByID
// ================================================================

func TestPhase2_QueryFindingByID_Found(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier, recommendation,
		  recommended_sql, rollback_sql,
		  estimated_cost_usd)
		 VALUES ('test', 'warning', 'Test finding',
		  '{"key":"val"}', 'open', 'obj1',
		  'Do something', 'SELECT 1',
		  'SELECT 0', 3.50)
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	finding, err := queryFindingByID(
		ctx, pool, fmt.Sprintf("%d", id))
	if err != nil {
		t.Fatalf("queryFindingByID: %v", err)
	}
	if finding["title"] != "Test finding" {
		t.Errorf("title: got %v", finding["title"])
	}
	if finding["recommendation"] != "Do something" {
		t.Errorf("recommendation: got %v",
			finding["recommendation"])
	}
	if finding["rollback_sql"] != "SELECT 0" {
		t.Errorf("rollback_sql: got %v",
			finding["rollback_sql"])
	}
	if finding["status"] != "open" {
		t.Errorf("status: got %v", finding["status"])
	}
}

func TestPhase2_QueryFindingByID_NotFound(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := queryFindingByID(ctx, pool, "99999")
	if err == nil {
		t.Error("expected error for nonexistent ID")
	}
}

func TestPhase2_QueryFindingByID_InvalidID(t *testing.T) {
	pool, ctx := phase2RequireDB(t)

	_, err := queryFindingByID(ctx, pool, "not-a-number")
	if err == nil {
		t.Error("expected error for invalid ID")
	}
}

// ================================================================
// updateFindingStatus
// ================================================================

func TestPhase2_UpdateFindingStatus_SuppressOpen(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'warning', 'Suppress me',
		  '{}', 'open', 'suppress_obj')
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	err = updateFindingStatus(
		ctx, pool, fmt.Sprintf("%d", id),
		"open", "suppressed")
	if err != nil {
		t.Fatalf("updateFindingStatus: %v", err)
	}

	// Verify status changed.
	var status string
	pool.QueryRow(ctx,
		`SELECT status FROM sage.findings WHERE id = $1`,
		id).Scan(&status)
	if status != "suppressed" {
		t.Errorf("status: got %q, want suppressed", status)
	}
}

func TestPhase2_UpdateFindingStatus_WrongFromStatus(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'info', 'Wrong status',
		  '{}', 'open', 'wrong_status_obj')
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Try to unsuppress an open finding — should fail.
	err = updateFindingStatus(
		ctx, pool, fmt.Sprintf("%d", id),
		"suppressed", "open")
	if err == nil {
		t.Error("expected error for wrong from status")
	}
	if err != nil && !strings.Contains(
		err.Error(), "no matching finding") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPhase2_UpdateFindingStatus_NonexistentID(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	err := updateFindingStatus(
		ctx, pool, "99999", "open", "suppressed")
	if err == nil {
		t.Error("expected error for nonexistent ID")
	}
}

// ================================================================
// queryActions / scanActionRows / queryActionByID
// ================================================================

func TestPhase2_QueryActions_EmptyTable(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	actions, total, err := queryActions(
		ctx, pool, 50, 0)
	if err != nil {
		t.Fatalf("queryActions: %v", err)
	}
	if total != 0 {
		t.Errorf("total: got %d, want 0", total)
	}
	if len(actions) != 0 {
		t.Errorf("actions: got %d, want 0", len(actions))
	}
}

func TestPhase2_QueryActions_WithData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Insert test actions.
	_, err := pool.Exec(ctx,
		`INSERT INTO sage.action_log
		 (action_type, sql_executed, rollback_sql,
		  before_state, after_state, outcome)
		 VALUES
		 ('create_index', 'CREATE INDEX idx_t ON t(c)',
		  'DROP INDEX idx_t',
		  '{"size": 0}', '{"size": 1024}', 'success'),
		 ('vacuum', 'VACUUM t', NULL,
		  NULL, NULL, 'success')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	actions, total, err := queryActions(
		ctx, pool, 50, 0)
	if err != nil {
		t.Fatalf("queryActions: %v", err)
	}
	if total != 2 {
		t.Errorf("total: got %d, want 2", total)
	}
	if len(actions) != 2 {
		t.Errorf("actions: got %d, want 2", len(actions))
	}

	// Check fields present.
	a := actions[0]
	if _, ok := a["action_type"]; !ok {
		t.Error("missing action_type key")
	}
	if _, ok := a["sql_executed"]; !ok {
		t.Error("missing sql_executed key")
	}
	if _, ok := a["outcome"]; !ok {
		t.Error("missing outcome key")
	}
}

func TestPhase2_QueryActions_Pagination(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	for i := 0; i < 5; i++ {
		_, err := pool.Exec(ctx,
			`INSERT INTO sage.action_log
			 (action_type, sql_executed, outcome)
			 VALUES ($1, 'SELECT 1', 'success')`,
			fmt.Sprintf("type_%d", i))
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	actions, total, err := queryActions(
		ctx, pool, 2, 1)
	if err != nil {
		t.Fatalf("queryActions: %v", err)
	}
	if total != 5 {
		t.Errorf("total: got %d, want 5", total)
	}
	if len(actions) != 2 {
		t.Errorf("actions: got %d, want 2", len(actions))
	}
}

func TestPhase2_QueryActionByID_Found(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.action_log
		 (action_type, sql_executed, rollback_sql,
		  before_state, after_state, outcome,
		  rollback_reason)
		 VALUES ('create_index', 'CREATE INDEX idx_t ON t(c)',
		  'DROP INDEX idx_t',
		  '{"before":1}', '{"after":2}', 'success',
		  NULL)
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	action, err := queryActionByID(
		ctx, pool, fmt.Sprintf("%d", id))
	if err != nil {
		t.Fatalf("queryActionByID: %v", err)
	}
	if action["action_type"] != "create_index" {
		t.Errorf("action_type: got %v",
			action["action_type"])
	}
	if action["rollback_sql"] != "DROP INDEX idx_t" {
		t.Errorf("rollback_sql: got %v",
			action["rollback_sql"])
	}
}

func TestPhase2_QueryActionByID_NotFound(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := queryActionByID(ctx, pool, "99999")
	if err == nil {
		t.Error("expected error for nonexistent ID")
	}
}

// ================================================================
// querySnapshotLatest / querySnapshotHistory
// ================================================================

func TestPhase2_QuerySnapshotLatest_NoData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := querySnapshotLatest(
		ctx, pool, "cache_hit_ratio")
	if err == nil {
		t.Error("expected error for no snapshots")
	}
}

func TestPhase2_QuerySnapshotLatest_WithData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.snapshots
		 (category, data)
		 VALUES ('cache_hit_ratio', '{"ratio": 0.99}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	data, err := querySnapshotLatest(
		ctx, pool, "cache_hit_ratio")
	if err != nil {
		t.Fatalf("querySnapshotLatest: %v", err)
	}
	if data == nil {
		t.Error("data should not be nil")
	}
	m, ok := data.(map[string]any)
	if !ok {
		t.Fatal("data should be a map")
	}
	if m["ratio"] != 0.99 {
		t.Errorf("ratio: got %v, want 0.99", m["ratio"])
	}
}

func TestPhase2_QuerySnapshotHistory_Empty(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	points, err := querySnapshotHistory(
		ctx, pool, "tps", 24)
	if err != nil {
		t.Fatalf("querySnapshotHistory: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("points: got %d, want 0", len(points))
	}
}

func TestPhase2_QuerySnapshotHistory_WithData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.snapshots
		 (category, data, collected_at)
		 VALUES
		 ('tps', '{"value": 100}', now() - interval '1 hour'),
		 ('tps', '{"value": 200}', now() - interval '30 minutes'),
		 ('tps', '{"value": 300}', now())`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	points, err := querySnapshotHistory(
		ctx, pool, "tps", 24)
	if err != nil {
		t.Fatalf("querySnapshotHistory: %v", err)
	}
	if len(points) != 3 {
		t.Errorf("points: got %d, want 3", len(points))
	}

	// Points should be ordered by collected_at ASC.
	if points[0]["data"] == nil {
		t.Error("first point data should not be nil")
	}
}

func TestPhase2_QuerySnapshotHistory_HoursFilter(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Insert one in the last hour, one 48 hours ago.
	_, err := pool.Exec(ctx,
		`INSERT INTO sage.snapshots
		 (category, data, collected_at)
		 VALUES
		 ('tps', '{"v":1}', now() - interval '30 minutes'),
		 ('tps', '{"v":2}', now() - interval '48 hours')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	points, err := querySnapshotHistory(
		ctx, pool, "tps", 2)
	if err != nil {
		t.Fatalf("querySnapshotHistory: %v", err)
	}
	if len(points) != 1 {
		t.Errorf("points: got %d, want 1", len(points))
	}
}

// ================================================================
// queryForecasts / scanForecastRows
// ================================================================

func TestPhase2_QueryForecasts_Empty(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	forecasts, err := queryForecasts(ctx, pool)
	if err != nil {
		t.Fatalf("queryForecasts: %v", err)
	}
	if len(forecasts) != 0 {
		t.Errorf("forecasts: got %d, want 0",
			len(forecasts))
	}
}

func TestPhase2_QueryForecasts_WithData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES
		 ('forecast_disk', 'warning', 'Disk full in 7d',
		  '{"days_left":7}', 'open', 'disk_obj'),
		 ('forecast_conn', 'critical', 'Conn exhaustion',
		  '{}', 'open', 'conn_obj'),
		 ('index_health', 'warning', 'Not a forecast',
		  '{}', 'open', 'idx_obj')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	forecasts, err := queryForecasts(ctx, pool)
	if err != nil {
		t.Fatalf("queryForecasts: %v", err)
	}
	if len(forecasts) != 2 {
		t.Errorf("forecasts: got %d, want 2",
			len(forecasts))
	}
	// Critical should come first (severity ordering).
	if forecasts[0]["severity"] != "critical" {
		t.Errorf("first severity: got %v, want critical",
			forecasts[0]["severity"])
	}
}

// ================================================================
// queryQueryHints / scanQueryHintRows
// ================================================================

func TestPhase2_QueryQueryHints_Empty(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	hints, err := queryQueryHints(ctx, pool)
	if err != nil {
		t.Fatalf("queryQueryHints: %v", err)
	}
	if len(hints) != 0 {
		t.Errorf("hints: got %d, want 0", len(hints))
	}
}

func TestPhase2_QueryQueryHints_WithData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.query_hints
		 (queryid, hint_text, symptom, status,
		  before_cost, after_cost)
		 VALUES
		 (12345, 'Use index scan', 'seq_scan', 'active',
		  100.0, 10.0),
		 (67890, 'Use hash join', 'nested_loop',
		  'inactive', NULL, NULL)`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	hints, err := queryQueryHints(ctx, pool)
	if err != nil {
		t.Fatalf("queryQueryHints: %v", err)
	}
	// Only active hints returned.
	if len(hints) != 1 {
		t.Errorf("hints: got %d, want 1", len(hints))
	}
	if hints[0]["queryid"] != int64(12345) {
		t.Errorf("queryid: got %v", hints[0]["queryid"])
	}
	if hints[0]["hint_text"] != "Use index scan" {
		t.Errorf("hint_text: got %v",
			hints[0]["hint_text"])
	}
}

// ================================================================
// queryAlertLog / scanAlertLogRows
// ================================================================

func TestPhase2_QueryAlertLog_Empty(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	alerts, err := queryAlertLog(ctx, pool)
	if err != nil {
		t.Fatalf("queryAlertLog: %v", err)
	}
	if len(alerts) != 0 {
		t.Errorf("alerts: got %d, want 0", len(alerts))
	}
}

func TestPhase2_QueryAlertLog_WithData(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Insert a finding first for FK.
	var findingID int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'critical', 'Alert target',
		  '{}', 'open', 'alert_obj')
		 RETURNING id`).Scan(&findingID)
	if err != nil {
		t.Fatalf("insert finding: %v", err)
	}

	_, err = pool.Exec(ctx,
		`INSERT INTO sage.alert_log
		 (finding_id, severity, channel, dedup_key,
		  status, error_message)
		 VALUES
		 ($1, 'critical', 'slack', 'key1',
		  'sent', NULL),
		 ($1, 'warning', 'email', 'key2',
		  'failed', 'SMTP timeout')`,
		findingID)
	if err != nil {
		t.Fatalf("insert alerts: %v", err)
	}

	alerts, err := queryAlertLog(ctx, pool)
	if err != nil {
		t.Fatalf("queryAlertLog: %v", err)
	}
	if len(alerts) != 2 {
		t.Errorf("alerts: got %d, want 2", len(alerts))
	}

	// Check fields populated.
	found := false
	for _, a := range alerts {
		if a["channel"] == "email" {
			found = true
			if a["error_message"] != "SMTP timeout" {
				t.Errorf("error_message: got %v",
					a["error_message"])
			}
		}
	}
	if !found {
		t.Error("email alert not found")
	}
}

// ================================================================
// Handler tests with real DB: findingsListHandler
// ================================================================

func TestPhase2_FindingsListHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Insert a finding.
	_, err := pool.Exec(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'warning', 'Test finding',
		  '{"info":"details"}', 'open', 'handler_obj')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := findingsListHandler(mgr)

	req := httptest.NewRequest(
		"GET", "/api/v1/findings?database=testdb", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["total"].(float64) != 1 {
		t.Errorf("total: got %v", resp["total"])
	}
	findings := resp["findings"].([]any)
	if len(findings) != 1 {
		t.Errorf("findings: got %d", len(findings))
	}
}

// ================================================================
// Handler tests: findingDetailHandler with real DB
// ================================================================

func TestPhase2_FindingDetailHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'info', 'Detail test',
		  '{}', 'open', 'detail_obj')
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /api/v1/findings/{id}",
		findingDetailHandler(mgr))

	req := httptest.NewRequest("GET",
		fmt.Sprintf("/api/v1/findings/%d", id), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["title"] != "Detail test" {
		t.Errorf("title: got %v", resp["title"])
	}
}

// ================================================================
// Handler tests: suppress/unsuppress with real DB
// ================================================================

func TestPhase2_SuppressHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'warning', 'Suppress target',
		  '{}', 'open', 'suppress_handler_obj')
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"POST /api/v1/findings/{id}/suppress",
		suppressHandler(mgr))

	req := httptest.NewRequest("POST",
		fmt.Sprintf("/api/v1/findings/%d/suppress", id),
		nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["status"] != "suppressed" {
		t.Errorf("status: got %v", resp["status"])
	}
}

func TestPhase2_UnsuppressHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'info', 'Unsuppress target',
		  '{}', 'suppressed', 'unsuppress_handler_obj')
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"POST /api/v1/findings/{id}/unsuppress",
		unsuppressHandler(mgr))

	req := httptest.NewRequest("POST",
		fmt.Sprintf("/api/v1/findings/%d/unsuppress", id),
		nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

// ================================================================
// Handler tests: actions with real DB
// ================================================================

func TestPhase2_ActionsListHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.action_log
		 (action_type, sql_executed, outcome)
		 VALUES ('vacuum', 'VACUUM t', 'success')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := actionsListHandler(mgr)

	req := httptest.NewRequest(
		"GET", "/api/v1/actions?database=testdb", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["total"].(float64) != 1 {
		t.Errorf("total: got %v", resp["total"])
	}
}

func TestPhase2_ActionDetailHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.action_log
		 (action_type, sql_executed, outcome)
		 VALUES ('reindex', 'REINDEX INDEX i', 'success')
		 RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /api/v1/actions/{id}",
		actionDetailHandler(mgr))

	req := httptest.NewRequest("GET",
		fmt.Sprintf("/api/v1/actions/%d", id), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

// ================================================================
// Handler tests: snapshots with real DB
// ================================================================

func TestPhase2_SnapshotLatestHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.snapshots
		 (category, data)
		 VALUES ('cache_hit_ratio', '{"ratio":0.95}')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := snapshotLatestHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/snapshots/latest?database=testdb"+
			"&metric=cache_hit_ratio", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["snapshot"] == nil {
		t.Error("snapshot should not be nil")
	}
}

func TestPhase2_SnapshotHistoryHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.snapshots
		 (category, data, collected_at)
		 VALUES
		 ('tps', '{"v":1}', now() - interval '1 hour'),
		 ('tps', '{"v":2}', now())`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := snapshotHistoryHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/snapshots/history?database=testdb"+
			"&metric=tps&hours=24", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	points := resp["points"].([]any)
	if len(points) != 2 {
		t.Errorf("points: got %d, want 2", len(points))
	}
}

func TestPhase2_SnapshotHistoryHandler_InvalidMetric(
	t *testing.T,
) {
	pool, _ := phase2RequireDB(t)
	mgr := phase2MgrWithPool(pool)
	handler := snapshotHistoryHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/snapshots/history?metric=DROP_TABLE", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ================================================================
// Handler tests: forecasts, hints, alert-log with real DB
// ================================================================

func TestPhase2_ForecastsHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('forecast_disk', 'warning', 'Disk 90%',
		  '{}', 'open', 'forecast_handler_obj')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := forecastsHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/forecasts?database=testdb", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	forecasts := resp["forecasts"].([]any)
	if len(forecasts) != 1 {
		t.Errorf("forecasts: got %d, want 1",
			len(forecasts))
	}
}

func TestPhase2_QueryHintsHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.query_hints
		 (queryid, hint_text, symptom, status)
		 VALUES (111, 'Use idx', 'seq_scan', 'active')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := queryHintsHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/query-hints?database=testdb", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	hints := resp["hints"].([]any)
	if len(hints) != 1 {
		t.Errorf("hints: got %d, want 1", len(hints))
	}
}

func TestPhase2_AlertLogHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	var findingID int64
	err := pool.QueryRow(ctx,
		`INSERT INTO sage.findings
		 (category, severity, title, detail, status,
		  object_identifier)
		 VALUES ('test', 'critical', 'Alert src',
		  '{}', 'open', 'alert_handler_obj')
		 RETURNING id`).Scan(&findingID)
	if err != nil {
		t.Fatalf("insert finding: %v", err)
	}

	_, err = pool.Exec(ctx,
		`INSERT INTO sage.alert_log
		 (finding_id, severity, channel, dedup_key, status)
		 VALUES ($1, 'critical', 'slack', 'k1', 'sent')`,
		findingID)
	if err != nil {
		t.Fatalf("insert alert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := alertLogHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/alert-log?database=testdb", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	alerts := resp["alerts"].([]any)
	if len(alerts) != 1 {
		t.Errorf("alerts: got %d, want 1", len(alerts))
	}
}

// ================================================================
// applyConfigOverrides
// ================================================================

func TestPhase2_ApplyConfigOverrides_ValidKey(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)
	userID := phase2EnsureUser(t, pool, ctx)

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{
		Trust: config.TrustConfig{Level: "observation"},
	}

	body := map[string]any{
		"trust.level": "advisory",
	}
	errs := applyConfigOverrides(
		ctx, cs, cfg, body, 0, userID)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if cfg.Trust.Level != "advisory" {
		t.Errorf("trust.level: got %q, want advisory",
			cfg.Trust.Level)
	}
}

func TestPhase2_ApplyConfigOverrides_InvalidKey(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{}

	body := map[string]any{
		"bogus.key": "value",
	}
	errs := applyConfigOverrides(
		ctx, cs, cfg, body, 0, 0)
	if len(errs) == 0 {
		t.Error("expected error for invalid key")
	}
}

func TestPhase2_ApplyConfigOverrides_MultipleKeys(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)
	userID := phase2EnsureUser(t, pool, ctx)

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{
		Collector: config.CollectorConfig{
			IntervalSeconds: 60,
			BatchSize:       1000,
		},
	}

	body := map[string]any{
		"collector.interval_seconds": "30",
		"collector.batch_size":       "500",
	}
	errs := applyConfigOverrides(
		ctx, cs, cfg, body, 0, userID)
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if cfg.Collector.IntervalSeconds != 30 {
		t.Errorf("interval: got %d, want 30",
			cfg.Collector.IntervalSeconds)
	}
	if cfg.Collector.BatchSize != 500 {
		t.Errorf("batch_size: got %d, want 500",
			cfg.Collector.BatchSize)
	}
}

func TestPhase2_ApplyConfigOverrides_PerDatabase(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)
	userID := phase2EnsureUser(t, pool, ctx)

	// Insert a database row so FK on database_id is satisfied.
	_, err := pool.Exec(ctx,
		`INSERT INTO sage.databases
		 (id, name, host, port, database_name, username,
		  password_enc, sslmode)
		 VALUES (1, 'perdb-test', 'localhost', 5432,
		  'testdb', 'user', '\x00', 'disable')`)
	if err != nil {
		t.Fatalf("insert db: %v", err)
	}

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{
		Trust: config.TrustConfig{Level: "observation"},
	}

	// Per-database override should NOT hot-reload global config.
	body := map[string]any{
		"trust.level": "autonomous",
	}
	errs := applyConfigOverrides(
		ctx, cs, cfg, body, 1, userID)
	if len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	// Global config should stay at "observation" since
	// databaseID=1.
	if cfg.Trust.Level != "observation" {
		t.Errorf("trust.level: got %q, want observation",
			cfg.Trust.Level)
	}
}

// ================================================================
// hotReload coverage for all config sections
// ================================================================

func TestPhase2_HotReload_Collector(t *testing.T) {
	cfg := &config.Config{}
	hotReload(cfg, "collector.interval_seconds", "120")
	if cfg.Collector.IntervalSeconds != 120 {
		t.Errorf("interval: got %d", cfg.Collector.IntervalSeconds)
	}
	hotReload(cfg, "collector.batch_size", "2000")
	if cfg.Collector.BatchSize != 2000 {
		t.Errorf("batch: got %d", cfg.Collector.BatchSize)
	}
	hotReload(cfg, "collector.max_queries", "500")
	if cfg.Collector.MaxQueries != 500 {
		t.Errorf("max_queries: got %d", cfg.Collector.MaxQueries)
	}
}

func TestPhase2_HotReload_Analyzer(t *testing.T) {
	cfg := &config.Config{}
	hotReload(cfg, "analyzer.interval_seconds", "300")
	if cfg.Analyzer.IntervalSeconds != 300 {
		t.Errorf("interval: got %d",
			cfg.Analyzer.IntervalSeconds)
	}
	hotReload(cfg, "analyzer.slow_query_threshold_ms", "1000")
	if cfg.Analyzer.SlowQueryThresholdMs != 1000 {
		t.Errorf("slow_query: got %d",
			cfg.Analyzer.SlowQueryThresholdMs)
	}
	hotReload(cfg, "analyzer.seq_scan_min_rows", "5000")
	if cfg.Analyzer.SeqScanMinRows != 5000 {
		t.Errorf("seq_scan: got %d",
			cfg.Analyzer.SeqScanMinRows)
	}
	hotReload(cfg, "analyzer.unused_index_window_days", "14")
	if cfg.Analyzer.UnusedIndexWindowDays != 14 {
		t.Errorf("unused_idx: got %d",
			cfg.Analyzer.UnusedIndexWindowDays)
	}
	hotReload(cfg,
		"analyzer.index_bloat_threshold_pct", "50")
	if cfg.Analyzer.IndexBloatThresholdPct != 50 {
		t.Errorf("bloat: got %d",
			cfg.Analyzer.IndexBloatThresholdPct)
	}
	hotReload(cfg,
		"analyzer.table_bloat_dead_tuple_pct", "30")
	if cfg.Analyzer.TableBloatDeadTuplePct != 30 {
		t.Errorf("dead_tuple: got %d",
			cfg.Analyzer.TableBloatDeadTuplePct)
	}
	hotReload(cfg,
		"analyzer.regression_threshold_pct", "25")
	if cfg.Analyzer.RegressionThresholdPct != 25 {
		t.Errorf("regression: got %d",
			cfg.Analyzer.RegressionThresholdPct)
	}
	hotReload(cfg,
		"analyzer.cache_hit_ratio_warning", "0.85")
	if cfg.Analyzer.CacheHitRatioWarning != 0.85 {
		t.Errorf("cache_hit: got %f",
			cfg.Analyzer.CacheHitRatioWarning)
	}
}

func TestPhase2_HotReload_Trust(t *testing.T) {
	cfg := &config.Config{}
	hotReload(cfg, "trust.level", "autonomous")
	if cfg.Trust.Level != "autonomous" {
		t.Errorf("level: got %q", cfg.Trust.Level)
	}
	hotReload(cfg, "trust.tier3_safe", "true")
	if !cfg.Trust.Tier3Safe {
		t.Error("tier3_safe should be true")
	}
	hotReload(cfg, "trust.tier3_moderate", "true")
	if !cfg.Trust.Tier3Moderate {
		t.Error("tier3_moderate should be true")
	}
	hotReload(cfg, "trust.tier3_high_risk", "false")
	if cfg.Trust.Tier3HighRisk {
		t.Error("tier3_high_risk should be false")
	}
	hotReload(cfg, "trust.maintenance_window", "02:00-06:00")
	if cfg.Trust.MaintenanceWindow != "02:00-06:00" {
		t.Errorf("maint: got %q", cfg.Trust.MaintenanceWindow)
	}
	hotReload(cfg, "trust.rollback_threshold_pct", "20")
	if cfg.Trust.RollbackThresholdPct != 20 {
		t.Errorf("rollback_pct: got %d",
			cfg.Trust.RollbackThresholdPct)
	}
	hotReload(cfg, "trust.rollback_window_minutes", "30")
	if cfg.Trust.RollbackWindowMinutes != 30 {
		t.Errorf("rollback_win: got %d",
			cfg.Trust.RollbackWindowMinutes)
	}
	hotReload(cfg, "trust.rollback_cooldown_days", "3")
	if cfg.Trust.RollbackCooldownDays != 3 {
		t.Errorf("cooldown: got %d",
			cfg.Trust.RollbackCooldownDays)
	}
	hotReload(cfg, "trust.cascade_cooldown_cycles", "5")
	if cfg.Trust.CascadeCooldownCycles != 5 {
		t.Errorf("cascade: got %d",
			cfg.Trust.CascadeCooldownCycles)
	}
}

func TestPhase2_HotReload_Safety(t *testing.T) {
	cfg := &config.Config{}
	hotReload(cfg, "safety.cpu_ceiling_pct", "80")
	if cfg.Safety.CPUCeilingPct != 80 {
		t.Errorf("cpu: got %d", cfg.Safety.CPUCeilingPct)
	}
	hotReload(cfg, "safety.query_timeout_ms", "5000")
	if cfg.Safety.QueryTimeoutMs != 5000 {
		t.Errorf("timeout: got %d", cfg.Safety.QueryTimeoutMs)
	}
	hotReload(cfg, "safety.ddl_timeout_seconds", "60")
	if cfg.Safety.DDLTimeoutSeconds != 60 {
		t.Errorf("ddl: got %d", cfg.Safety.DDLTimeoutSeconds)
	}
	hotReload(cfg, "safety.lock_timeout_ms", "1000")
	if cfg.Safety.LockTimeoutMs != 1000 {
		t.Errorf("lock: got %d", cfg.Safety.LockTimeoutMs)
	}
}

func TestPhase2_HotReload_LLM(t *testing.T) {
	cfg := &config.Config{}
	hotReload(cfg, "llm.enabled", "true")
	if !cfg.LLM.Enabled {
		t.Error("enabled should be true")
	}
	hotReload(cfg, "llm.endpoint", "http://llm:8080")
	if cfg.LLM.Endpoint != "http://llm:8080" {
		t.Errorf("endpoint: got %q", cfg.LLM.Endpoint)
	}
	hotReload(cfg, "llm.api_key", "sk-test")
	if cfg.LLM.APIKey != "sk-test" {
		t.Errorf("api_key: got %q", cfg.LLM.APIKey)
	}
	hotReload(cfg, "llm.model", "gpt-4")
	if cfg.LLM.Model != "gpt-4" {
		t.Errorf("model: got %q", cfg.LLM.Model)
	}
	hotReload(cfg, "llm.timeout_seconds", "30")
	if cfg.LLM.TimeoutSeconds != 30 {
		t.Errorf("timeout: got %d", cfg.LLM.TimeoutSeconds)
	}
	hotReload(cfg, "llm.token_budget_daily", "100000")
	if cfg.LLM.TokenBudgetDaily != 100000 {
		t.Errorf("budget: got %d", cfg.LLM.TokenBudgetDaily)
	}
	hotReload(cfg, "llm.context_budget_tokens", "4096")
	if cfg.LLM.ContextBudgetTokens != 4096 {
		t.Errorf("ctx: got %d", cfg.LLM.ContextBudgetTokens)
	}
}

func TestPhase2_HotReload_Alerting(t *testing.T) {
	cfg := &config.Config{}
	hotReload(cfg, "alerting.enabled", "true")
	if !cfg.Alerting.Enabled {
		t.Error("enabled should be true")
	}
	hotReload(cfg, "alerting.slack_webhook_url",
		"https://hooks.slack.com/xxx")
	if cfg.Alerting.SlackWebhookURL !=
		"https://hooks.slack.com/xxx" {
		t.Errorf("slack: got %q",
			cfg.Alerting.SlackWebhookURL)
	}
	hotReload(cfg, "alerting.pagerduty_routing_key", "pd123")
	if cfg.Alerting.PagerDutyRoutingKey != "pd123" {
		t.Errorf("pd: got %q",
			cfg.Alerting.PagerDutyRoutingKey)
	}
	hotReload(cfg, "alerting.check_interval_seconds", "120")
	if cfg.Alerting.CheckIntervalSeconds != 120 {
		t.Errorf("check: got %d",
			cfg.Alerting.CheckIntervalSeconds)
	}
	hotReload(cfg, "alerting.cooldown_minutes", "15")
	if cfg.Alerting.CooldownMinutes != 15 {
		t.Errorf("cooldown: got %d",
			cfg.Alerting.CooldownMinutes)
	}
	hotReload(cfg, "alerting.quiet_hours_start", "22:00")
	if cfg.Alerting.QuietHoursStart != "22:00" {
		t.Errorf("start: got %q",
			cfg.Alerting.QuietHoursStart)
	}
	hotReload(cfg, "alerting.quiet_hours_end", "06:00")
	if cfg.Alerting.QuietHoursEnd != "06:00" {
		t.Errorf("end: got %q",
			cfg.Alerting.QuietHoursEnd)
	}
}

func TestPhase2_HotReload_Retention(t *testing.T) {
	cfg := &config.Config{}
	hotReload(cfg, "retention.snapshots_days", "30")
	if cfg.Retention.SnapshotsDays != 30 {
		t.Errorf("snap: got %d", cfg.Retention.SnapshotsDays)
	}
	hotReload(cfg, "retention.findings_days", "90")
	if cfg.Retention.FindingsDays != 90 {
		t.Errorf("find: got %d", cfg.Retention.FindingsDays)
	}
	hotReload(cfg, "retention.actions_days", "180")
	if cfg.Retention.ActionsDays != 180 {
		t.Errorf("act: got %d", cfg.Retention.ActionsDays)
	}
	hotReload(cfg, "retention.explains_days", "14")
	if cfg.Retention.ExplainsDays != 14 {
		t.Errorf("exp: got %d", cfg.Retention.ExplainsDays)
	}
}

func TestPhase2_HotReload_UnknownPrefix(t *testing.T) {
	cfg := &config.Config{
		Trust: config.TrustConfig{Level: "observation"},
	}
	// Should be a no-op, not panic.
	hotReload(cfg, "unknown.key", "value")
	if cfg.Trust.Level != "observation" {
		t.Error("config should not have changed")
	}
}

// ================================================================
// configDBGetHandler with real DB
// ================================================================

func TestPhase2_ConfigDBGetHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Insert a database record.
	_, err := pool.Exec(ctx,
		`INSERT INTO sage.databases
		 (id, name, host, port, database_name, username,
		  password_enc, sslmode, execution_mode)
		 VALUES (1, 'test', 'localhost', 5432, 'testdb',
		  'user', '\x00', 'disable', 'manual')`)
	if err != nil {
		t.Fatalf("insert db: %v", err)
	}

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{
		Trust: config.TrustConfig{Level: "observation"},
	}

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /api/v1/config/databases/{id}",
		configDBGetHandler(cs, cfg, pool))

	req := httptest.NewRequest("GET",
		"/api/v1/config/databases/1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["database_id"].(float64) != 1 {
		t.Errorf("database_id: got %v", resp["database_id"])
	}
	cfgMap := resp["config"].(map[string]any)
	if cfgMap == nil {
		t.Error("config should not be nil")
	}
}

func TestPhase2_ConfigDBGetHandler_InvalidID(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	cs := store.NewConfigStore(pool)
	cfg := &config.Config{}

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /api/v1/config/databases/{id}",
		configDBGetHandler(cs, cfg, pool))

	req := httptest.NewRequest("GET",
		"/api/v1/config/databases/abc", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestPhase2_ConfigDBGetHandler_ZeroID(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	cs := store.NewConfigStore(pool)
	cfg := &config.Config{}

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /api/v1/config/databases/{id}",
		configDBGetHandler(cs, cfg, pool))

	req := httptest.NewRequest("GET",
		"/api/v1/config/databases/0", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ================================================================
// loginHandler with real DB
// ================================================================

func TestPhase2_LoginHandler_Success(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Create a test user.
	_, err := auth.CreateUser(
		ctx, pool, "test@example.com", "secret123", "admin")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	handler := loginHandler(pool)
	body := `{"email":"test@example.com","password":"secret123"}`
	req := httptest.NewRequest("POST",
		"/api/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["email"] != "test@example.com" {
		t.Errorf("email: got %v", resp["email"])
	}
	if resp["role"] != "admin" {
		t.Errorf("role: got %v", resp["role"])
	}

	// Check session cookie was set.
	cookies := w.Result().Cookies()
	found := false
	for _, c := range cookies {
		if c.Name == "sage_session" && c.Value != "" {
			found = true
		}
	}
	if !found {
		t.Error("sage_session cookie not set")
	}
}

func TestPhase2_LoginHandler_BadPassword(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := auth.CreateUser(
		ctx, pool, "user@test.com", "correct", "viewer")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	handler := loginHandler(pool)
	body := `{"email":"user@test.com","password":"wrong"}`
	req := httptest.NewRequest("POST",
		"/api/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 401 {
		t.Errorf("status: got %d, want 401", w.Code)
	}
}

func TestPhase2_LoginHandler_MissingFields(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	handler := loginHandler(pool)

	body := `{"email":"","password":""}`
	req := httptest.NewRequest("POST",
		"/api/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestPhase2_LoginHandler_InvalidJSON(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	handler := loginHandler(pool)

	req := httptest.NewRequest("POST",
		"/api/v1/auth/login", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestPhase2_LoginHandler_NonexistentUser(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	handler := loginHandler(pool)
	body := `{"email":"nobody@test.com","password":"abc"}`
	req := httptest.NewRequest("POST",
		"/api/v1/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 401 {
		t.Errorf("status: got %d, want 401", w.Code)
	}
}

// ================================================================
// listUsersHandler, createUserHandler, deleteUserHandler
// ================================================================

func TestPhase2_ListUsersHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := auth.CreateUser(
		ctx, pool, "admin@test.com", "pass", "admin")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	handler := listUsersHandler(pool)
	req := httptest.NewRequest(
		"GET", "/api/v1/users", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	users := resp["users"].([]any)
	if len(users) != 1 {
		t.Errorf("users: got %d, want 1", len(users))
	}
}

func TestPhase2_CreateUserHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	handler := createUserHandler(pool)
	body := `{"email":"new@test.com","password":"pass","role":"viewer"}`
	req := httptest.NewRequest("POST",
		"/api/v1/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 201 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["email"] != "new@test.com" {
		t.Errorf("email: got %v", resp["email"])
	}
}

func TestPhase2_CreateUserHandler_DuplicateEmail(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := auth.CreateUser(
		ctx, pool, "dup@test.com", "pass", "viewer")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	handler := createUserHandler(pool)
	body := `{"email":"dup@test.com","password":"pass","role":"viewer"}`
	req := httptest.NewRequest("POST",
		"/api/v1/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 409 {
		t.Errorf("status: got %d, want 409", w.Code)
	}
}

func TestPhase2_CreateUserHandler_InvalidRole(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	handler := createUserHandler(pool)
	body := `{"email":"r@test.com","password":"pass","role":"superadmin"}`
	req := httptest.NewRequest("POST",
		"/api/v1/users", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestPhase2_DeleteUserHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	id, err := auth.CreateUser(
		ctx, pool, "del@test.com", "pass", "viewer")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	handler := deleteUserHandler(pool)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"DELETE /api/v1/users/{id}", handler)

	req := httptest.NewRequest("DELETE",
		fmt.Sprintf("/api/v1/users/%d", id), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

func TestPhase2_DeleteUserHandler_NotFound(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	handler := deleteUserHandler(pool)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"DELETE /api/v1/users/{id}", handler)

	req := httptest.NewRequest("DELETE",
		"/api/v1/users/99999", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Errorf("status: got %d, want 404", w.Code)
	}
}

// ================================================================
// updateUserRoleHandler
// ================================================================

func TestPhase2_UpdateUserRoleHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	id, err := auth.CreateUser(
		ctx, pool, "role@test.com", "pass", "viewer")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	handler := updateUserRoleHandler(pool)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"PUT /api/v1/users/{id}/role", handler)

	body := `{"role":"operator"}`
	req := httptest.NewRequest("PUT",
		fmt.Sprintf("/api/v1/users/%d/role", id),
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

// ================================================================
// Notification handlers with real DB
// ================================================================

func TestPhase2_ListChannelsHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	_, err := ns.CreateChannel(ctx, "test-slack", "slack",
		map[string]string{"webhook_url": "https://x"}, 0)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	handler := listChannelsHandler(ns)
	req := httptest.NewRequest("GET",
		"/api/v1/notifications/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	channels := resp["channels"].([]any)
	if len(channels) != 1 {
		t.Errorf("channels: got %d, want 1",
			len(channels))
	}
}

func TestPhase2_ListChannelsHandler_EmptyDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	handler := listChannelsHandler(ns)
	req := httptest.NewRequest("GET",
		"/api/v1/notifications/channels", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	channels := resp["channels"].([]any)
	if len(channels) != 0 {
		t.Errorf("channels: got %d, want 0",
			len(channels))
	}
}

func TestPhase2_ListRulesHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	// Create channel first.
	chID, err := ns.CreateChannel(ctx, "rule-ch", "slack",
		map[string]string{"webhook_url": "https://x"}, 0)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	_, err = ns.CreateRule(ctx, chID,
		"finding_critical", "warning")
	if err != nil {
		t.Fatalf("create rule: %v", err)
	}

	handler := listRulesHandler(ns)
	req := httptest.NewRequest("GET",
		"/api/v1/notifications/rules", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	rules := resp["rules"].([]any)
	if len(rules) != 1 {
		t.Errorf("rules: got %d, want 1", len(rules))
	}
}

func TestPhase2_ListNotificationLogHandler_RealDB(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	handler := listNotificationLogHandler(ns)
	req := httptest.NewRequest("GET",
		"/api/v1/notifications/log", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	logEntries := resp["log"].([]any)
	if len(logEntries) != 0 {
		t.Errorf("log: got %d, want 0", len(logEntries))
	}
}

// ================================================================
// Database managed handlers with real DB
// ================================================================

func TestPhase2_ListManagedDBHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	handler := listManagedDBHandler(deps)
	req := httptest.NewRequest("GET",
		"/api/v1/databases/managed", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	dbs := resp["databases"].([]any)
	if len(dbs) != 0 {
		t.Errorf("databases: got %d, want 0", len(dbs))
	}
}

func TestPhase2_CreateManagedDBHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	handler := createManagedDBHandler(deps)
	body := `{
		"name": "test-db",
		"host": "localhost",
		"port": 5432,
		"database_name": "testdb",
		"username": "user",
		"password": "pass",
		"sslmode": "disable",
		"trust_level": "observation",
		"execution_mode": "manual"
	}`
	req := httptest.NewRequest("POST",
		"/api/v1/databases/managed",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 201 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["name"] != "test-db" {
		t.Errorf("name: got %v", resp["name"])
	}
}

func TestPhase2_CreateManagedDBHandler_InvalidJSON(
	t *testing.T,
) {
	pool, _ := phase2RequireDB(t)
	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	handler := createManagedDBHandler(deps)
	req := httptest.NewRequest("POST",
		"/api/v1/databases/managed",
		strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestPhase2_DeleteManagedDBHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	id, err := ds.Create(ctx, store.DatabaseInput{
		Name:          "del-db",
		Host:          "localhost",
		Port:          5432,
		DatabaseName:  "testdb",
		Username:      "user",
		Password:      "pass",
		SSLMode:       "disable",
		TrustLevel:    "observation",
		ExecutionMode: "manual",
	}, 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	handler := deleteManagedDBHandler(deps)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"DELETE /api/v1/databases/managed/{id}", handler)

	req := httptest.NewRequest("DELETE",
		fmt.Sprintf("/api/v1/databases/managed/%d", id),
		nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["ok"] != true {
		t.Errorf("ok: got %v", resp["ok"])
	}
}

func TestPhase2_DeleteManagedDBHandler_NotFound(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	handler := deleteManagedDBHandler(deps)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"DELETE /api/v1/databases/managed/{id}", handler)

	req := httptest.NewRequest("DELETE",
		"/api/v1/databases/managed/99999", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Errorf("status: got %d, want 404", w.Code)
	}
}

// ================================================================
// importCSVHandler with real DB
// ================================================================

func TestPhase2_ImportCSVHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	csvContent := "name,host,port,database_name,username,password,sslmode\n" +
		"db1,h1,5432,d1,u1,p1,disable\n" +
		"db2,h2,5432,d2,u2,p2,disable\n"

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("file", "import.csv")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	part.Write([]byte(csvContent))
	writer.Close()

	handler := importCSVHandler(deps)
	req := httptest.NewRequest("POST",
		"/api/v1/databases/managed/import", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	imported := resp["imported"].(float64)
	if imported != 2 {
		t.Errorf("imported: got %v, want 2", imported)
	}
}

func TestPhase2_ImportCSVHandler_InvalidHeader(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	csvContent := "wrong,header,here\n" +
		"db1,h1,5432\n"

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile("file", "bad.csv")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	part.Write([]byte(csvContent))
	writer.Close()

	handler := importCSVHandler(deps)
	req := httptest.NewRequest("POST",
		"/api/v1/databases/managed/import", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	errs := resp["errors"].([]any)
	if len(errs) == 0 {
		t.Error("expected errors for invalid header")
	}
}

func TestPhase2_ImportCSVHandler_MissingFile(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	ds := store.NewDatabaseStore(pool, p2Key)
	deps := &DatabaseDeps{Store: ds}

	handler := importCSVHandler(deps)
	req := httptest.NewRequest("POST",
		"/api/v1/databases/managed/import",
		strings.NewReader("not multipart"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ================================================================
// fleetPendingActionsHandler with real DB
// ================================================================

func TestPhase2_FleetPendingActionsHandler_RealDB(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Insert a pending action.
	_, err := pool.Exec(ctx,
		`INSERT INTO sage.action_queue
		 (finding_id, proposed_sql, action_risk, status)
		 VALUES (0, 'SELECT 1', 'SAFE', 'pending')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := fleetPendingActionsHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/actions/pending", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	total := resp["total"].(float64)
	if total < 1 {
		t.Errorf("total: got %v, want >= 1", total)
	}
}

// ================================================================
// fleetPendingCountHandler with real DB
// ================================================================

func TestPhase2_FleetPendingCountHandler_RealDB(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.action_queue
		 (finding_id, proposed_sql, action_risk, status)
		 VALUES (0, 'SELECT 1', 'SAFE', 'pending')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	mgr := phase2MgrWithPool(pool)
	handler := fleetPendingCountHandler(mgr)

	req := httptest.NewRequest("GET",
		"/api/v1/actions/pending/count", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	count := resp["count"].(float64)
	if count < 1 {
		t.Errorf("count: got %v, want >= 1", count)
	}
}

// ================================================================
// registerAuthRoutes / registerUserRoutes / registerConfigRoutes
// / registerNotificationRoutes — verify registration doesn't panic
// ================================================================

func TestPhase2_RegisterAuthRoutes_NoPanic(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	mux := http.NewServeMux()
	cfg := &config.Config{}
	// Should not panic with nil OAuth.
	registerAuthRoutes(mux, pool, nil, cfg)
}

func TestPhase2_RegisterUserRoutes_NoPanic(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	mux := http.NewServeMux()
	registerUserRoutes(mux, pool)
}

func TestPhase2_RegisterConfigRoutes_NoPanic(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	mux := http.NewServeMux()
	cfg := &config.Config{}
	registerConfigRoutes(mux, pool, cfg)
}

func TestPhase2_RegisterNotificationRoutes_NoPanic(
	t *testing.T,
) {
	pool, _ := phase2RequireDB(t)
	mux := http.NewServeMux()
	registerNotificationRoutes(mux, pool)
}

// ================================================================
// Full router with real DB pool
// ================================================================

func TestPhase2_NewRouterFull_WithPool(t *testing.T) {
	pool, _ := phase2RequireDB(t)
	cfg := &config.Config{
		Mode:  "fleet",
		Trust: config.TrustConfig{Level: "advisory"},
	}
	mgr := fleet.NewManager(cfg)
	mgr.RegisterInstance(&fleet.DatabaseInstance{
		Name: "test",
		Pool: pool,
		Status: &fleet.InstanceStatus{
			Connected: true,
		},
	})

	router := NewRouterFull(
		mgr, cfg, pool, nil, nil,
		SessionAuthMiddleware(pool))
	if router == nil {
		t.Fatal("router should not be nil")
	}

	// Test that auth routes are registered.
	req := httptest.NewRequest("POST",
		"/api/v1/auth/login",
		strings.NewReader(`{"email":"a","password":"b"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Should get 401 (invalid credentials), not 404 (route
	// not found).
	if w.Code == 404 {
		t.Error("login route should be registered")
	}
}

// ================================================================
// configGlobalGetHandler / configGlobalPutHandler
// ================================================================

func TestPhase2_ConfigGlobalGetHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{
		Mode:  "standalone",
		Trust: config.TrustConfig{Level: "observation"},
		Collector: config.CollectorConfig{
			IntervalSeconds: 60,
		},
	}

	handler := configGlobalGetHandler(cs, cfg)
	req := httptest.NewRequest("GET",
		"/api/v1/config/global", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["mode"] != "standalone" {
		t.Errorf("mode: got %v", resp["mode"])
	}
	if resp["config"] == nil {
		t.Error("config should not be nil")
	}
}

func TestPhase2_ConfigGlobalPutHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)
	userID := phase2EnsureUser(t, pool, ctx)

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{
		Trust: config.TrustConfig{Level: "observation"},
	}

	handler := configGlobalPutHandler(cs, cfg, nil)
	body := `{"trust.level": "advisory"}`
	req := httptest.NewRequest("PUT",
		"/api/v1/config/global",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	// Inject user so handler gets a valid userID for FK.
	req = withUser(req, &auth.User{
		ID: userID, Email: "phase2@test.com",
		Role: auth.RoleAdmin,
	})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	if cfg.Trust.Level != "advisory" {
		t.Errorf("trust.level: got %q, want advisory",
			cfg.Trust.Level)
	}
}

func TestPhase2_ConfigGlobalPutHandler_InvalidKey(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	cs := store.NewConfigStore(pool)
	cfg := &config.Config{}

	handler := configGlobalPutHandler(cs, cfg, nil)
	body := `{"bad.key": "value"}`
	req := httptest.NewRequest("PUT",
		"/api/v1/config/global",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestPhase2_ConfigGlobalPutHandler_InvalidJSON(
	t *testing.T,
) {
	pool, _ := phase2RequireDB(t)
	cs := store.NewConfigStore(pool)
	cfg := &config.Config{}

	handler := configGlobalPutHandler(cs, cfg, nil)
	req := httptest.NewRequest("PUT",
		"/api/v1/config/global",
		strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ================================================================
// configAuditHandler with real DB
// ================================================================

func TestPhase2_ConfigAuditHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)
	userID := phase2EnsureUser(t, pool, ctx)

	// Create an override to generate audit entries.
	cs := store.NewConfigStore(pool)
	cfg := &config.Config{}
	errs := applyConfigOverrides(
		ctx, cs, cfg,
		map[string]any{"trust.level": "advisory"},
		0, userID)
	if len(errs) != 0 {
		t.Fatalf("setup override: %v", errs)
	}

	handler := configAuditHandler(cs)
	req := httptest.NewRequest("GET",
		"/api/v1/config/audit", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	audit := resp["audit"].([]any)
	if len(audit) < 1 {
		t.Errorf("audit: got %d, want >= 1", len(audit))
	}
}

// ================================================================
// updateDBExecutionMode with real DB
// ================================================================

func TestPhase2_UpdateDBExecutionMode_Valid(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	_, err := pool.Exec(ctx,
		`INSERT INTO sage.databases
		 (id, name, host, port, database_name, username,
		  password_enc, sslmode, execution_mode)
		 VALUES (1, 'em-test', 'localhost', 5432, 'testdb',
		  'user', '\x00', 'disable', 'manual')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	err = updateDBExecutionMode(ctx, pool, 1, "auto")
	if err != nil {
		t.Fatalf("updateDBExecutionMode: %v", err)
	}

	var mode string
	pool.QueryRow(ctx,
		`SELECT execution_mode FROM sage.databases
		 WHERE id = 1`).Scan(&mode)
	if mode != "auto" {
		t.Errorf("mode: got %q, want auto", mode)
	}
}

func TestPhase2_UpdateDBExecutionMode_InvalidMode(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)

	err := updateDBExecutionMode(ctx, pool, 1, "invalid")
	if err == nil {
		t.Error("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "must be") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPhase2_UpdateDBExecutionMode_NotFound(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	err := updateDBExecutionMode(ctx, pool, 99999, "auto")
	if err == nil {
		t.Error("expected error for nonexistent DB")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ================================================================
// logoutHandler with real DB
// ================================================================

func TestPhase2_LogoutHandler_WithSession(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	// Create user and session.
	userID, err := auth.CreateUser(
		ctx, pool, "logout@test.com", "pass", "viewer")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	sessionID, err := auth.CreateSession(ctx, pool, userID)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	handler := logoutHandler(pool)
	req := httptest.NewRequest("POST",
		"/api/v1/auth/logout", nil)
	req.AddCookie(&http.Cookie{
		Name:  "sage_session",
		Value: sessionID,
	})
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	// Session should be deleted.
	_, err = auth.ValidateSession(ctx, pool, sessionID)
	if err == nil {
		t.Error("session should be invalidated after logout")
	}
}

func TestPhase2_LogoutHandler_NoCookie(t *testing.T) {
	pool, _ := phase2RequireDB(t)

	handler := logoutHandler(pool)
	req := httptest.NewRequest("POST",
		"/api/v1/auth/logout", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should succeed even without cookie.
	if w.Code != 200 {
		t.Errorf("status: got %d, want 200", w.Code)
	}
}

// ================================================================
// meHandler
// ================================================================

func TestPhase2_MeHandler_Authenticated(t *testing.T) {
	handler := meHandler()
	req := httptest.NewRequest("GET",
		"/api/v1/auth/me", nil)
	req = withUser(req, testAdminUser())
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["email"] != "admin@test.com" {
		t.Errorf("email: got %v", resp["email"])
	}
}

func TestPhase2_MeHandler_Unauthenticated(t *testing.T) {
	handler := meHandler()
	req := httptest.NewRequest("GET",
		"/api/v1/auth/me", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 401 {
		t.Errorf("status: got %d, want 401", w.Code)
	}
}

// ================================================================
// oauthConfigHandler / oauthAuthorizeHandler
// ================================================================

func TestPhase2_OAuthConfigHandler_Disabled(t *testing.T) {
	handler := oauthConfigHandler(nil, "")
	req := httptest.NewRequest("GET",
		"/api/v1/auth/oauth/config", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["enabled"] != false {
		t.Errorf("enabled: got %v, want false",
			resp["enabled"])
	}
}

func TestPhase2_OAuthAuthorizeHandler_NotConfigured(
	t *testing.T,
) {
	handler := oauthAuthorizeHandler(nil)
	req := httptest.NewRequest("GET",
		"/api/v1/auth/oauth/authorize", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Errorf("status: got %d, want 404", w.Code)
	}
}

// ================================================================
// oauthCallbackHandler with nil provider
// ================================================================

func TestPhase2_OAuthCallbackHandler_NotConfigured(
	t *testing.T,
) {
	pool, _ := phase2RequireDB(t)
	handler := oauthCallbackHandler(nil, pool, "viewer", "")
	req := httptest.NewRequest("GET",
		"/api/v1/auth/oauth/callback?code=x&state=y", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 404 {
		t.Errorf("status: got %d, want 404", w.Code)
	}
}

// ================================================================
// buildFindingsOrder — sort columns
// ================================================================

func TestPhase2_BuildFindingsOrder_AllSortOptions(
	t *testing.T,
) {
	tests := []struct {
		sort  string
		order string
		want  string
	}{
		{"severity", "desc", "WHEN 'critical'"},
		{"severity", "asc", "ASC"},
		{"created_at", "desc", "created_at DESC"},
		{"last_seen", "asc", "last_seen ASC"},
		{"category", "desc", "category DESC"},
		{"title", "asc", "title ASC"},
		{"unknown", "desc", "last_seen DESC"},
	}
	for _, tt := range tests {
		t.Run(tt.sort+"_"+tt.order, func(t *testing.T) {
			f := fleet.FindingFilters{
				Sort:  tt.sort,
				Order: tt.order,
			}
			order := buildFindingsOrder(f)
			if !strings.Contains(order, tt.want) {
				t.Errorf("order for %s/%s: got %q, want containing %q",
					tt.sort, tt.order, order, tt.want)
			}
		})
	}
}

// ================================================================
// validateMetric
// ================================================================

func TestPhase2_ValidateMetric_AllValid(t *testing.T) {
	valid := []string{
		"", "tables", "indexes", "queries", "sequences",
		"foreign_keys", "system", "io", "locks",
		"config_data", "partitions", "cache_hit_ratio",
		"connections", "tps", "dead_tuples",
		"database_size", "replication_lag",
	}
	for _, m := range valid {
		if !validateMetric(m) {
			t.Errorf("validateMetric(%q) = false, want true", m)
		}
	}
}

func TestPhase2_ValidateMetric_Invalid(t *testing.T) {
	invalid := []string{
		"DROP_TABLE", "unknown_metric", "../../etc",
		"SELECT",
	}
	for _, m := range invalid {
		if validateMetric(m) {
			t.Errorf("validateMetric(%q) = true, want false", m)
		}
	}
}

// ================================================================
// Notification create/update/delete handlers with real DB
// ================================================================

func TestPhase2_CreateChannelHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	handler := createChannelHandler(ns)
	body := `{"name":"test-ch","type":"slack","config":{"webhook_url":"https://x"}}`
	req := httptest.NewRequest("POST",
		"/api/v1/notifications/channels",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 201 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

func TestPhase2_CreateChannelHandler_MissingName(
	t *testing.T,
) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	handler := createChannelHandler(ns)
	body := `{"name":"","type":"slack","config":{}}`
	req := httptest.NewRequest("POST",
		"/api/v1/notifications/channels",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 400 {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestPhase2_DeleteChannelHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	id, err := ns.CreateChannel(ctx, "del-ch", "slack",
		map[string]string{"webhook_url": "https://x"}, 0)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	handler := deleteChannelHandler(ns)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"DELETE /api/v1/notifications/channels/{id}",
		handler)

	req := httptest.NewRequest("DELETE",
		fmt.Sprintf(
			"/api/v1/notifications/channels/%d", id),
		nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestPhase2_CreateRuleHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	chID, err := ns.CreateChannel(ctx, "rule-ch", "slack",
		map[string]string{"webhook_url": "https://x"}, 0)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	handler := createRuleHandler(ns)
	body := fmt.Sprintf(
		`{"channel_id":%d,"event":"finding_critical","min_severity":"warning"}`,
		chID)
	req := httptest.NewRequest("POST",
		"/api/v1/notifications/rules",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != 201 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

func TestPhase2_DeleteRuleHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	chID, err := ns.CreateChannel(ctx, "rule-ch2", "slack",
		map[string]string{"webhook_url": "https://x"}, 0)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	ruleID, err := ns.CreateRule(ctx, chID,
		"finding_critical", "warning")
	if err != nil {
		t.Fatalf("create rule: %v", err)
	}

	handler := deleteRuleHandler(ns)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"DELETE /api/v1/notifications/rules/{id}",
		handler)

	req := httptest.NewRequest("DELETE",
		fmt.Sprintf(
			"/api/v1/notifications/rules/%d", ruleID),
		nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestPhase2_UpdateRuleHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	chID, err := ns.CreateChannel(ctx, "upd-rule-ch", "slack",
		map[string]string{"webhook_url": "https://x"}, 0)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	ruleID, err := ns.CreateRule(ctx, chID,
		"finding_critical", "warning")
	if err != nil {
		t.Fatalf("create rule: %v", err)
	}

	handler := updateRuleHandler(ns)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"PUT /api/v1/notifications/rules/{id}", handler)

	body := `{"enabled":false}`
	req := httptest.NewRequest("PUT",
		fmt.Sprintf(
			"/api/v1/notifications/rules/%d", ruleID),
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

func TestPhase2_UpdateChannelHandler_RealDB(t *testing.T) {
	pool, ctx := phase2RequireDB(t)
	phase2CleanTables(t, pool, ctx)

	logFn := func(_, _ string, _ ...any) {}
	d := notify.NewDispatcher(pool, logFn)
	ns := store.NewNotificationStore(pool, d)

	id, err := ns.CreateChannel(ctx, "upd-ch", "slack",
		map[string]string{"webhook_url": "https://x"}, 0)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	handler := updateChannelHandler(ns)
	mux := http.NewServeMux()
	mux.HandleFunc(
		"PUT /api/v1/notifications/channels/{id}",
		handler)

	body := `{"name":"updated-ch","config":{"webhook_url":"https://y"},"enabled":false}`
	req := httptest.NewRequest("PUT",
		fmt.Sprintf(
			"/api/v1/notifications/channels/%d", id),
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("status: %d, body: %s",
			w.Code, w.Body.String())
	}
}

// ================================================================
// syncTrustLevelToFleet
// ================================================================

func TestPhase2_SyncTrustLevelToFleet(t *testing.T) {
	cfg := &config.Config{}
	mgr := fleet.NewManager(cfg)
	mgr.RegisterInstance(&fleet.DatabaseInstance{
		Name: "a",
		Status: &fleet.InstanceStatus{
			TrustLevel: "observation",
		},
	})
	mgr.RegisterInstance(&fleet.DatabaseInstance{
		Name: "b",
		Status: &fleet.InstanceStatus{
			TrustLevel: "observation",
		},
	})

	syncTrustLevelToFleet(mgr, "autonomous")

	for _, inst := range mgr.Instances() {
		if inst.Status.TrustLevel != "autonomous" {
			t.Errorf("instance %s: got %q, want autonomous",
				inst.Name, inst.Status.TrustLevel)
		}
	}
}

// ================================================================
// parseFindingFilters
// ================================================================

func TestPhase2_ParseFindingFilters_Defaults(t *testing.T) {
	q := map[string][]string{}
	f := parseFindingFilters(q)
	if f.Status != "open" {
		t.Errorf("status: got %q, want open", f.Status)
	}
	if f.Limit != 50 {
		t.Errorf("limit: got %d, want 50", f.Limit)
	}
	if f.Offset != 0 {
		t.Errorf("offset: got %d, want 0", f.Offset)
	}
	if f.Sort != "severity" {
		t.Errorf("sort: got %q, want severity", f.Sort)
	}
	if f.Order != "desc" {
		t.Errorf("order: got %q, want desc", f.Order)
	}
}

func TestPhase2_ParseFindingFilters_OverrideAll(t *testing.T) {
	q := map[string][]string{
		"status":   {"suppressed"},
		"severity": {"critical"},
		"category": {"vacuum"},
		"sort":     {"created_at"},
		"order":    {"asc"},
		"limit":    {"25"},
		"offset":   {"10"},
	}
	f := parseFindingFilters(q)
	if f.Status != "suppressed" {
		t.Errorf("status: got %q", f.Status)
	}
	if f.Severity != "critical" {
		t.Errorf("severity: got %q", f.Severity)
	}
	if f.Category != "vacuum" {
		t.Errorf("category: got %q", f.Category)
	}
	if f.Sort != "created_at" {
		t.Errorf("sort: got %q", f.Sort)
	}
	if f.Limit != 25 {
		t.Errorf("limit: got %d", f.Limit)
	}
	if f.Offset != 10 {
		t.Errorf("offset: got %d", f.Offset)
	}
}

func TestPhase2_ParseFindingFilters_LimitCap(t *testing.T) {
	q := map[string][]string{
		"limit": {"999"},
	}
	f := parseFindingFilters(q)
	if f.Limit != 200 {
		t.Errorf("limit: got %d, want 200 (capped)", f.Limit)
	}
}

// ================================================================
// findingsEmptyResponse
// ================================================================

func TestPhase2_FindingsEmptyResponse(t *testing.T) {
	f := fleet.FindingFilters{
		Status: "open", Limit: 50, Offset: 0,
	}
	resp := findingsEmptyResponse("mydb", f)
	if resp["database"] != "mydb" {
		t.Errorf("database: got %v", resp["database"])
	}
	if resp["total"] != 0 {
		t.Errorf("total: got %v", resp["total"])
	}
	findings := resp["findings"].([]any)
	if len(findings) != 0 {
		t.Errorf("findings: got %d", len(findings))
	}
}

// ================================================================
// buildFindingMap with nil fields
// ================================================================

func TestPhase2_BuildFindingMap_NilOptionalFields(
	t *testing.T,
) {
	now := time.Now()
	m := buildFindingMap(
		1, now, now, 5,
		"cat", "warning",
		nil, nil, "title",
		nil, nil, nil,
		"open", "db",
	)
	if m["object_type"] != "" {
		t.Errorf("object_type: got %v, want empty",
			m["object_type"])
	}
	if m["object_identifier"] != "" {
		t.Errorf("object_identifier: got %v, want empty",
			m["object_identifier"])
	}
	if m["recommendation"] != "" {
		t.Errorf("recommendation: got %v, want empty",
			m["recommendation"])
	}
	if m["recommended_sql"] != "" {
		t.Errorf("recommended_sql: got %v, want empty",
			m["recommended_sql"])
	}
}

func TestPhase2_BuildFindingMap_WithAllFields(t *testing.T) {
	now := time.Now()
	objType := "index"
	objIdent := "idx_t"
	rec := "Drop it"
	recSQL := "DROP INDEX idx_t"
	detail := []byte(`{"key":"value"}`)

	m := buildFindingMap(
		42, now, now, 3,
		"idx", "critical",
		&objType, &objIdent, "Bad index",
		detail, &rec, &recSQL,
		"open", "proddb",
	)
	if m["id"] != "42" {
		t.Errorf("id: got %v", m["id"])
	}
	if m["category"] != "idx" {
		t.Errorf("category: got %v", m["category"])
	}
	if m["database_name"] != "proddb" {
		t.Errorf("database_name: got %v",
			m["database_name"])
	}
	if m["detail"] == nil {
		t.Error("detail should be parsed JSON, not nil")
	}
}

// ================================================================
// buildActionMap
// ================================================================

func TestPhase2_BuildActionMap_WithFindingID(t *testing.T) {
	now := time.Now()
	fID := int64(99)
	rollSQL := "DROP INDEX idx"
	before := []byte(`{"size":0}`)
	after := []byte(`{"size":1024}`)
	rbReason := "perf regressed"

	m := buildActionMap(
		1, now, "create_index", &fID,
		"CREATE INDEX idx ON t(c)", &rollSQL,
		before, after, "success",
		&rbReason, &now,
	)
	if m["finding_id"] == nil {
		t.Error("finding_id should not be nil")
	}
	if *(m["finding_id"].(*string)) != "99" {
		t.Errorf("finding_id: got %v",
			*(m["finding_id"].(*string)))
	}
	if m["before_state"] == nil {
		t.Error("before_state should be parsed JSON")
	}
	if m["rollback_reason"] != "perf regressed" {
		t.Errorf("rollback_reason: got %v",
			m["rollback_reason"])
	}
}

func TestPhase2_BuildActionMap_NilOptionalFields(
	t *testing.T,
) {
	now := time.Now()
	m := buildActionMap(
		1, now, "vacuum", nil,
		"VACUUM t", nil,
		nil, nil, "success",
		nil, nil,
	)
	// finding_id is (*string)(nil) — a typed nil stored as any.
	// Use type assertion to verify the pointer is nil.
	if fid, ok := m["finding_id"].(*string); !ok || fid != nil {
		t.Errorf("finding_id: expected nil *string, got %v",
			m["finding_id"])
	}
	if m["rollback_sql"] != "" {
		t.Errorf("rollback_sql: got %v", m["rollback_sql"])
	}
	if m["before_state"] != nil {
		t.Errorf("before_state: got %v, want nil",
			m["before_state"])
	}
	// measured_at is (*time.Time)(nil) — typed nil.
	if mat, ok := m["measured_at"].(*time.Time); !ok ||
		mat != nil {
		t.Errorf("measured_at: expected nil *time.Time, got %v",
			m["measured_at"])
	}
}

// ================================================================
// atoi / atof helpers
// ================================================================

func TestPhase2_Atoi_Valid(t *testing.T) {
	if atoi("42") != 42 {
		t.Errorf("atoi(42): got %d", atoi("42"))
	}
}

func TestPhase2_Atoi_Invalid(t *testing.T) {
	if atoi("abc") != 0 {
		t.Errorf("atoi(abc): got %d", atoi("abc"))
	}
}

func TestPhase2_Atof_Valid(t *testing.T) {
	if atof("3.14") != 3.14 {
		t.Errorf("atof(3.14): got %f", atof("3.14"))
	}
}

func TestPhase2_Atof_Invalid(t *testing.T) {
	if atof("abc") != 0 {
		t.Errorf("atof(abc): got %f", atof("abc"))
	}
}
