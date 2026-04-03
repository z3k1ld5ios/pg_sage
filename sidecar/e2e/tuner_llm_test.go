//go:build e2e

// Package e2e — LLM-based tuner tests. Each subtest crafts a
// realistic query + EXPLAIN plan scenario designed to trigger a
// specific LLM hint type (MergeJoin, IndexOnlyScan, BitmapScan,
// etc.) that the deterministic rules engine cannot produce.
//
// Run with:
//
//	SAGE_LLM_API_KEY=<key> go test -tags=e2e -count=1 \
//	    -timeout 600s ./e2e/ -run TestTunerLLM
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pg-sage/sidecar/internal/llm"
	"github.com/pg-sage/sidecar/internal/tuner"
)

// llmPrescription mirrors the internal LLMPrescription type.
type llmPrescription struct {
	HintDirective string  `json:"hint_directive"`
	Rationale     string  `json:"rationale"`
	Confidence    float64 `json:"confidence"`
}

// validHintPrefixes are the valid pg_hint_plan directive
// prefixes the tuner accepts.
var validHintPrefixes = []string{
	"Set(", "HashJoin(", "MergeJoin(", "NestLoop(",
	"IndexScan(", "IndexOnlyScan(", "SeqScan(",
	"NoSeqScan(",
	"Parallel(", "NoParallel(",
	"BitmapScan(", "NoBitmapScan(",
	"NoIndexScan(", "NoNestLoop(", "NoHashJoin(",
	"NoMergeJoin(",
}

func hasValidHintPrefix(s string) bool {
	for _, p := range validHintPrefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// tunerScenario describes a complete tuner LLM test case.
type tunerScenario struct {
	Name          string
	Query         string
	MeanExecMs    float64
	Calls         int64
	PlanJSON      string
	Symptoms      string // formatted symptom lines
	Tables        string // formatted table context
	System        string // formatted system context
	FallbackHints string
	// Expected: at least one of these prefixes in the hint.
	WantOneOf []string
}

// buildPrompt constructs the user prompt string manually,
// matching the format FormatTunerPrompt produces.
func (s tunerScenario) buildPrompt() string {
	var b strings.Builder
	fmt.Fprintf(&b, "## Query (queryid: 0)\n%s\n\n", s.Query)
	fmt.Fprintf(&b,
		"Mean exec: %.1fms | Calls: %d | "+
			"Temp blks written: 0 | Mean plan: 0.0ms\n\n",
		s.MeanExecMs, s.Calls,
	)
	b.WriteString("## Detected Symptoms\n")
	b.WriteString(s.Symptoms)
	b.WriteString("\n")
	if s.PlanJSON != "" {
		b.WriteString("## Execution Plan\n")
		b.WriteString(s.PlanJSON)
		b.WriteString("\n\n")
	}
	b.WriteString(s.Tables)
	b.WriteString("\n")
	b.WriteString("## System\n")
	b.WriteString(s.System)
	b.WriteString("\n")
	if s.FallbackHints != "" {
		b.WriteString("## Deterministic Fallback Hints\n")
		b.WriteString(s.FallbackHints)
		b.WriteString("\n\n")
	}
	b.WriteString(
		"\nRESPOND NOW with ONLY the JSON array. " +
			"Start with [ immediately.",
	)
	return b.String()
}

// callTunerLLM sends a tuner prompt to Gemini and returns
// parsed prescriptions.
func callTunerLLM(
	t *testing.T,
	client *llm.Client,
	scenario tunerScenario,
) []llmPrescription {
	t.Helper()

	system := tuner.TunerSystemPrompt()
	prompt := scenario.buildPrompt()

	ctx, cancel := context.WithTimeout(
		context.Background(), 90*time.Second,
	)
	defer cancel()

	resp, tokens, err := client.Chat(
		ctx, system, prompt, 4096,
	)
	if err != nil {
		t.Fatalf("LLM chat failed: %v", err)
	}

	t.Logf("Response (%d tokens):\n%s", tokens, resp)

	cleaned := extractJSONArray(resp)
	if cleaned == "" {
		t.Fatalf("could not extract JSON from: %s", resp)
	}

	var recs []llmPrescription
	if err := json.Unmarshal(
		[]byte(cleaned), &recs,
	); err != nil {
		t.Fatalf("JSON parse failed: %v\n%s", err, cleaned)
	}
	return recs
}

// validatePrescriptions checks structure and hint syntax.
func validatePrescriptions(
	t *testing.T,
	recs []llmPrescription,
) {
	t.Helper()
	for i, r := range recs {
		if r.HintDirective == "" {
			t.Errorf("rec[%d]: empty hint_directive", i)
			continue
		}
		if r.Rationale == "" {
			t.Errorf("rec[%d]: empty rationale", i)
		}
		if r.Confidence < 0 || r.Confidence > 1.0 {
			t.Errorf(
				"rec[%d]: confidence %.2f out of [0,1]",
				i, r.Confidence,
			)
		}
		parts := splitDirectives(r.HintDirective)
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if !hasValidHintPrefix(p) {
				t.Errorf(
					"rec[%d]: invalid hint prefix in %q",
					i, p,
				)
			}
		}
	}
}

// splitDirectives splits on ) boundaries.
func splitDirectives(hint string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, ch := range hint {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				parts = append(parts, hint[start:i+1])
				start = i + 1
			}
		}
	}
	if trail := strings.TrimSpace(hint[start:]); trail != "" {
		parts = append(parts, trail)
	}
	return parts
}

func newTunerClient(t *testing.T) *llm.Client {
	t.Helper()
	apiKey := requireAPIKey(t)
	cfg := newTestLLMConfig(apiKey, largeBudget)
	cfg.TimeoutSeconds = 90
	return llm.New(cfg, testLogFn(t))
}

func logPrescriptions(t *testing.T, recs []llmPrescription) {
	t.Helper()
	for i, r := range recs {
		t.Logf(
			"  [%d] hint=%s confidence=%.2f\n"+
				"       rationale: %s",
			i+1, r.HintDirective, r.Confidence,
			r.Rationale,
		)
	}
}

func assertHintContainsOneOf(
	t *testing.T,
	recs []llmPrescription,
	targets []string,
) {
	t.Helper()
	for _, r := range recs {
		for _, target := range targets {
			if strings.Contains(r.HintDirective, target) {
				t.Logf("Found expected hint: %s", target)
				return
			}
		}
	}
	directives := make([]string, len(recs))
	for i, r := range recs {
		directives[i] = r.HintDirective
	}
	t.Errorf(
		"no prescription contains any of %v; got: %v",
		targets, directives,
	)
}

// --- Test scenarios ---

const commonSystem = "active_backends=20, max_connections=100, " +
	"work_mem=64MB, shared_buffers=4GB, " +
	"effective_cache_size=12GB, " +
	"max_parallel_workers_per_gather=4"

// TestTunerLLM_MergeJoin: Two large pre-sorted tables joined
// on a column with high correlation + btree indexes on join
// columns. LLM should recommend MergeJoin.
func TestTunerLLM_MergeJoin(t *testing.T) {
	client := newTunerClient(t)
	recs := callTunerLLM(t, client, tunerScenario{
		Name: "MergeJoin",
		Query: "SELECT o.id, o.total, t.description " +
			"FROM orders o " +
			"JOIN order_tags t ON o.id = t.order_id " +
			"WHERE o.created_at > '2025-01-01'",
		MeanExecMs: 350.0, Calls: 50000,
		PlanJSON: `[{"Plan": {
			"Node Type": "Hash Join",
			"Join Type": "Inner",
			"Hash Cond": "(o.id = t.order_id)",
			"Plan Rows": 5000000,
			"Actual Rows": 4800000,
			"Hash Batches": 16,
			"Peak Memory Usage": 32768,
			"Plans": [
				{"Node Type": "Index Scan",
				 "Relation Name": "orders", "Alias": "o",
				 "Index Name": "orders_pkey",
				 "Plan Rows": 5000000,
				 "Filter": "(created_at > '2025-01-01')"},
				{"Node Type": "Hash", "Plan Rows": 5000000,
				 "Plans": [
					{"Node Type": "Index Scan",
					 "Relation Name": "order_tags",
					 "Alias": "t",
					 "Index Name": "order_tags_order_id_idx",
					 "Plan Rows": 5000000}
				]}
			]
		}}]`,
		Symptoms: "- hash_spill, hash_batches=16, " +
			"peak_memory_kb=32768\n",
		Tables: `## Table: public.orders
Live tuples: 10000000 | Dead tuples: 50000 | Size: 3.0 GB
### Columns
- id bigint
- created_at timestamptz
- total numeric(12,2)
### Indexes
- orders_pkey [UNIQUE]: btree (id) (scans: 9000000)
### Column Stats
- id: n_distinct=-1.00, correlation=0.9900

## Table: public.order_tags
Live tuples: 8000000 | Dead tuples: 30000 | Size: 1.5 GB
### Columns
- order_id bigint
- description text
### Indexes
- order_tags_order_id_idx: btree (order_id) (scans: 5000000)
### Column Stats
- order_id: n_distinct=-0.90, correlation=0.9700
`,
		System:        commonSystem,
		FallbackHints: `Set(work_mem "256MB")`,
		WantOneOf:     []string{"MergeJoin("},
	})

	if len(recs) == 0 {
		t.Fatal("expected at least 1 prescription")
	}
	validatePrescriptions(t, recs)
	assertHintContainsOneOf(t, recs,
		[]string{"MergeJoin(", "Set(work_mem"})
	logPrescriptions(t, recs)
}

// TestTunerLLM_IndexOnlyScan: A covering index exists that
// satisfies the entire query. LLM should recommend
// IndexOnlyScan.
func TestTunerLLM_IndexOnlyScan(t *testing.T) {
	client := newTunerClient(t)
	recs := callTunerLLM(t, client, tunerScenario{
		Name: "IndexOnlyScan",
		Query: "SELECT customer_id, SUM(amount) " +
			"FROM payments " +
			"WHERE created_at > '2025-06-01' " +
			"GROUP BY customer_id",
		MeanExecMs: 180.0, Calls: 30000,
		PlanJSON: `[{"Plan": {
			"Node Type": "HashAggregate",
			"Plan Rows": 50000,
			"Plans": [
				{"Node Type": "Seq Scan",
				 "Relation Name": "payments",
				 "Alias": "payments",
				 "Plan Rows": 2000000,
				 "Filter": "(created_at > '2025-06-01')"}
			]
		}}]`,
		Symptoms: "- seq_scan_with_index on payments\n",
		Tables: `## Table: public.payments
Live tuples: 8000000 | Dead tuples: 20000 | Size: 2.0 GB
### Columns
- id bigint
- customer_id bigint
- amount numeric(12,2)
- created_at timestamptz
### Indexes
- payments_pkey [UNIQUE]: btree (id) (scans: 5000000)
- idx_payments_covering: btree (created_at, customer_id) INCLUDE (amount) (scans: 100)
### Column Stats
- created_at: n_distinct=-0.80, correlation=0.9500
- customer_id: n_distinct=-0.02, correlation=0.1000
`,
		System:        commonSystem,
		FallbackHints: "IndexScan(payments)",
		WantOneOf: []string{
			"IndexOnlyScan(", "IndexScan(",
		},
	})

	if len(recs) == 0 {
		t.Fatal("expected at least 1 prescription")
	}
	validatePrescriptions(t, recs)
	assertHintContainsOneOf(t, recs,
		[]string{"IndexOnlyScan(", "IndexScan("})
	logPrescriptions(t, recs)
}

// TestTunerLLM_BitmapScan: OR condition across two separately
// indexed columns. LLM should recommend BitmapScan.
func TestTunerLLM_BitmapScan(t *testing.T) {
	client := newTunerClient(t)
	recs := callTunerLLM(t, client, tunerScenario{
		Name: "BitmapScan",
		Query: "SELECT * FROM events " +
			"WHERE category = 'error' " +
			"OR priority = 'critical'",
		MeanExecMs: 95.0, Calls: 80000,
		PlanJSON: `[{"Plan": {
			"Node Type": "Seq Scan",
			"Relation Name": "events",
			"Alias": "events",
			"Plan Rows": 500000,
			"Filter": "((category = 'error') OR (priority = 'critical'))",
			"Rows Removed by Filter": 9500000
		}}]`,
		Symptoms: "- seq_scan_with_index on events\n",
		Tables: `## Table: public.events
Live tuples: 10000000 | Dead tuples: 40000 | Size: 4.0 GB
### Columns
- id bigint
- category text
- priority text
- payload jsonb (nullable)
- created_at timestamptz
### Indexes
- events_pkey [UNIQUE]: btree (id) (scans: 2000000)
- idx_events_category: btree (category) (scans: 500000)
- idx_events_priority: btree (priority) (scans: 200000)
### Column Stats
- category: n_distinct=25.00, correlation=0.0500
- priority: n_distinct=5.00, correlation=0.0200
`,
		System: "active_backends=30, max_connections=200, " +
			"work_mem=32MB, shared_buffers=8GB, " +
			"effective_cache_size=24GB, " +
			"max_parallel_workers_per_gather=4",
		FallbackHints: "IndexScan(events)",
		WantOneOf: []string{
			"BitmapScan(", "IndexScan(", "NoSeqScan(",
		},
	})

	if len(recs) == 0 {
		t.Fatal("expected at least 1 prescription")
	}
	validatePrescriptions(t, recs)
	assertHintContainsOneOf(t, recs,
		[]string{"BitmapScan(", "IndexScan(", "NoSeqScan("})
	logPrescriptions(t, recs)
}

// TestTunerLLM_NestLoop: Small outer result set (50 admin
// users) joined to large table with indexed join column.
// LLM should recommend NestLoop.
func TestTunerLLM_NestLoop(t *testing.T) {
	client := newTunerClient(t)
	recs := callTunerLLM(t, client, tunerScenario{
		Name: "NestLoop",
		Query: "SELECT u.name, o.total " +
			"FROM users u " +
			"JOIN orders o ON o.user_id = u.id " +
			"WHERE u.role = 'admin'",
		MeanExecMs: 250.0, Calls: 40000,
		PlanJSON: `[{"Plan": {
			"Node Type": "Hash Join",
			"Join Type": "Inner",
			"Hash Cond": "(o.user_id = u.id)",
			"Plan Rows": 500,
			"Actual Rows": 480,
			"Hash Batches": 4,
			"Peak Memory Usage": 8192,
			"Plans": [
				{"Node Type": "Seq Scan",
				 "Relation Name": "orders",
				 "Alias": "o", "Plan Rows": 5000000},
				{"Node Type": "Hash", "Plan Rows": 50,
				 "Plans": [
					{"Node Type": "Seq Scan",
					 "Relation Name": "users",
					 "Alias": "u", "Plan Rows": 50,
					 "Filter": "(role = 'admin')"}
				]}
			]
		}}]`,
		Symptoms: "- hash_spill, hash_batches=4, " +
			"peak_memory_kb=8192\n" +
			"- seq_scan_with_index on orders\n",
		Tables: `## Table: public.users
Live tuples: 10000 | Dead tuples: 100 | Size: 5.0 MB
### Columns
- id bigint
- name text
- role text
### Indexes
- users_pkey [UNIQUE]: btree (id) (scans: 500000)
### Column Stats
- role: n_distinct=5.00, correlation=0.3000

## Table: public.orders
Live tuples: 5000000 | Dead tuples: 20000 | Size: 2.0 GB
### Columns
- id bigint
- user_id bigint
- total numeric(12,2)
### Indexes
- orders_pkey [UNIQUE]: btree (id) (scans: 3000000)
- idx_orders_user_id: btree (user_id) (scans: 1500000)
### Column Stats
- user_id: n_distinct=-0.00, correlation=0.1500
`,
		System:        commonSystem,
		FallbackHints: `Set(work_mem "128MB")`,
		WantOneOf:     []string{"NestLoop("},
	})

	if len(recs) == 0 {
		t.Fatal("expected at least 1 prescription")
	}
	validatePrescriptions(t, recs)
	assertHintContainsOneOf(t, recs,
		[]string{"NestLoop(", "IndexScan("})
	logPrescriptions(t, recs)
}

// TestTunerLLM_Parallel: 200M row table, seq scan with no
// parallel workers. LLM should recommend Parallel.
func TestTunerLLM_Parallel(t *testing.T) {
	client := newTunerClient(t)
	recs := callTunerLLM(t, client, tunerScenario{
		Name: "Parallel",
		Query: "SELECT COUNT(*) FROM audit_log " +
			"WHERE event_type = 'login_failed' " +
			"AND created_at > '2025-01-01'",
		MeanExecMs: 800.0, Calls: 15000,
		PlanJSON: `[{"Plan": {
			"Node Type": "Aggregate",
			"Plan Rows": 1,
			"Plans": [
				{"Node Type": "Seq Scan",
				 "Relation Name": "audit_log",
				 "Alias": "audit_log",
				 "Plan Rows": 25000000,
				 "Filter": "((event_type = 'login_failed') AND (created_at > '2025-01-01'))"}
			]
		}}]`,
		Symptoms: "- seq_scan_with_index on audit_log\n" +
			"- parallel_disabled on audit_log\n",
		Tables: `## Table: public.audit_log
Live tuples: 200000000 | Dead tuples: 500000 | Size: 50.0 GB
### Columns
- id bigint
- event_type text
- created_at timestamptz
- details jsonb (nullable)
### Indexes
- audit_log_pkey [UNIQUE]: btree (id) (scans: 100)
- idx_audit_created: btree (created_at) (scans: 50000)
### Column Stats
- event_type: n_distinct=30.00, correlation=0.0100
- created_at: n_distinct=-0.90, correlation=0.9800
`,
		System: "active_backends=10, max_connections=100, " +
			"work_mem=64MB, shared_buffers=8GB, " +
			"effective_cache_size=24GB, " +
			"max_parallel_workers_per_gather=4",
		FallbackHints: `Set(max_parallel_workers_per_gather "4")`,
		WantOneOf: []string{
			"Parallel(", "max_parallel", "Set(",
		},
	})

	if len(recs) == 0 {
		t.Fatal("expected at least 1 prescription")
	}
	validatePrescriptions(t, recs)
	assertHintContainsOneOf(t, recs,
		[]string{"Parallel(", "max_parallel", "Set("})
	logPrescriptions(t, recs)
}

// TestTunerLLM_NoSeqScan: Unique index lookup returning 1
// row from 5M, but planner chose seq scan. LLM should
// force index usage with IndexScan or NoSeqScan.
func TestTunerLLM_NoSeqScan(t *testing.T) {
	client := newTunerClient(t)
	recs := callTunerLLM(t, client, tunerScenario{
		Name: "NoSeqScan",
		Query: "SELECT * FROM products " +
			"WHERE sku = 'ABC-12345'",
		MeanExecMs: 45.0, Calls: 200000,
		PlanJSON: `[{"Plan": {
			"Node Type": "Seq Scan",
			"Relation Name": "products",
			"Alias": "products",
			"Plan Rows": 1,
			"Actual Rows": 1,
			"Filter": "(sku = 'ABC-12345')",
			"Rows Removed by Filter": 4999999
		}}]`,
		Symptoms: "- seq_scan_with_index on products\n",
		Tables: `## Table: public.products
Live tuples: 5000000 | Dead tuples: 10000 | Size: 1.5 GB
### Columns
- id bigint
- sku text
- name text
- price numeric(10,2)
### Indexes
- products_pkey [UNIQUE]: btree (id) (scans: 3000000)
- idx_products_sku [UNIQUE]: btree (sku) (scans: 1000000)
### Column Stats
- sku: n_distinct=-1.00, correlation=0.4000
`,
		System:        commonSystem,
		FallbackHints: "IndexScan(products)",
		WantOneOf: []string{
			"IndexScan(", "NoSeqScan(",
		},
	})

	if len(recs) == 0 {
		t.Fatal("expected at least 1 prescription")
	}
	validatePrescriptions(t, recs)
	assertHintContainsOneOf(t, recs,
		[]string{"IndexScan(", "NoSeqScan("})
	logPrescriptions(t, recs)
}
