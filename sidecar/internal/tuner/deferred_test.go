package tuner

import (
	"context"
	"strings"
	"testing"
)

// ----------------------------------------------------------------
// matchDeferred — pure helper, no DB required.
// ----------------------------------------------------------------

func TestMatchDeferred_HitOnDeferredTable(t *testing.T) {
	plan := []byte(`[{"Plan": {
		"Node Type": "Seq Scan",
		"Schema": "public",
		"Relation Name": "orders",
		"Plan Rows": 100
	}}]`)
	deferred := map[string]bool{"public.orders": true}
	reason, err := matchDeferred(plan, deferred)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason == "" {
		t.Fatal("expected non-empty reason for deferred table hit")
	}
	if !strings.Contains(reason, "public.orders") {
		t.Errorf("reason %q should mention public.orders", reason)
	}
	if !strings.Contains(reason, "pending index") {
		t.Errorf("reason %q should mention pending index", reason)
	}
}

func TestMatchDeferred_NoHit(t *testing.T) {
	plan := []byte(`[{"Plan": {
		"Node Type": "Seq Scan",
		"Schema": "public",
		"Relation Name": "users",
		"Plan Rows": 100
	}}]`)
	deferred := map[string]bool{"public.orders": true}
	reason, err := matchDeferred(plan, deferred)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "" {
		t.Errorf("expected empty reason, got %q", reason)
	}
}

func TestMatchDeferred_HitInNestedJoin(t *testing.T) {
	// Plan has a join with two relations; one is deferred.
	plan := []byte(`[{"Plan": {
		"Node Type": "Hash Join",
		"Plan Rows": 1000,
		"Plans": [
			{"Node Type": "Seq Scan",
			 "Schema": "public", "Relation Name": "users",
			 "Plan Rows": 50},
			{"Node Type": "Hash", "Plan Rows": 200, "Plans": [
				{"Node Type": "Seq Scan",
				 "Schema": "billing", "Relation Name": "invoices",
				 "Plan Rows": 200}
			]}
		]
	}}]`)
	deferred := map[string]bool{"billing.invoices": true}
	reason, err := matchDeferred(plan, deferred)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(reason, "billing.invoices") {
		t.Errorf("reason %q should mention billing.invoices", reason)
	}
}

func TestMatchDeferred_EmptySet(t *testing.T) {
	plan := []byte(`[{"Plan": {
		"Node Type": "Seq Scan",
		"Schema": "public",
		"Relation Name": "orders",
		"Plan Rows": 100
	}}]`)
	reason, err := matchDeferred(plan, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "" {
		t.Errorf("expected empty reason for nil set, got %q", reason)
	}

	reason, err = matchDeferred(plan, map[string]bool{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "" {
		t.Errorf("expected empty reason for empty set, got %q", reason)
	}
}

func TestMatchDeferred_EmptyPlan(t *testing.T) {
	deferred := map[string]bool{"public.orders": true}
	reason, err := matchDeferred(nil, deferred)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "" {
		t.Errorf("expected empty reason for nil plan, got %q", reason)
	}
}

func TestMatchDeferred_MalformedPlan(t *testing.T) {
	deferred := map[string]bool{"public.orders": true}
	_, err := matchDeferred([]byte("garbage"), deferred)
	if err == nil {
		t.Error("expected error from malformed plan, got nil")
	}
}

func TestMatchDeferred_PlanWithoutSchemaDefaultsPublic(t *testing.T) {
	// Plan node lacks "Schema" — relation should canonicalize as
	// "public.orders" and match a deferred entry of the same form.
	plan := []byte(`[{"Plan": {
		"Node Type": "Seq Scan",
		"Relation Name": "orders",
		"Plan Rows": 100
	}}]`)
	deferred := map[string]bool{"public.orders": true}
	reason, err := matchDeferred(plan, deferred)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason == "" {
		t.Error("expected match when plan node has no schema field")
	}
}

func TestMatchDeferred_CaseInsensitive(t *testing.T) {
	plan := []byte(`[{"Plan": {
		"Node Type": "Seq Scan",
		"Schema": "PUBLIC",
		"Relation Name": "Orders",
		"Plan Rows": 100
	}}]`)
	deferred := map[string]bool{"public.orders": true}
	reason, err := matchDeferred(plan, deferred)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason == "" {
		t.Error("case differences should not prevent match")
	}
}

func TestMatchDeferred_PlanWithoutRelations(t *testing.T) {
	plan := []byte(`[{"Plan": {
		"Node Type": "Result",
		"Plan Rows": 1
	}}]`)
	deferred := map[string]bool{"public.orders": true}
	reason, err := matchDeferred(plan, deferred)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reason != "" {
		t.Errorf("Result node should not match anything, got %q",
			reason)
	}
}

// ----------------------------------------------------------------
// deferralReason — DB integration: needs explain_cache row.
// ----------------------------------------------------------------

func TestDeferralReason_NilPoolNoCrash(t *testing.T) {
	// With no pool, fetchPlanJSON returns "" — gating must
	// silently no-op rather than panic.
	tu := &Tuner{
		pool:          nil,
		logFn:         noopLog2,
		recentlyTuned: make(map[int64]int),
	}
	c := candidate{QueryID: 42}
	got := tu.deferralReason(
		context.Background(), c,
		map[string]bool{"public.orders": true},
	)
	if got != "" {
		t.Errorf("expected empty reason with nil pool, got %q", got)
	}
}

func TestDeferralReason_EmptyDeferredShortCircuits(t *testing.T) {
	// Even with a pool, an empty deferred set must skip the
	// fetchPlanJSON call entirely. We rely on the early-return
	// in deferralReason to avoid calling pool.QueryRow.
	tu := &Tuner{
		pool:          nil, // would panic if reached
		logFn:         noopLog2,
		recentlyTuned: make(map[int64]int),
	}
	c := candidate{QueryID: 42}
	got := tu.deferralReason(
		context.Background(), c, nil,
	)
	if got != "" {
		t.Errorf("expected empty reason for nil set, got %q", got)
	}
	got = tu.deferralReason(
		context.Background(), c, map[string]bool{},
	)
	if got != "" {
		t.Errorf("expected empty reason for empty set, got %q", got)
	}
}

// TestDeferralReason_DBIntegration uses a real Postgres pool to
// confirm that deferralReason correctly fetches a plan from
// sage.explain_cache and matches against the deferred set.
func TestDeferralReason_DBIntegration(t *testing.T) {
	pool, ctx := requireTunerDB(t)

	// Insert a synthetic plan_json for queryid=987654321.
	const qid int64 = 987654321
	planJSON := `[{"Plan": {
		"Node Type": "Seq Scan",
		"Schema": "public",
		"Relation Name": "orders_test_deferral",
		"Plan Rows": 100
	}}]`

	_, err := pool.Exec(ctx,
		`DELETE FROM sage.explain_cache WHERE queryid = $1`, qid)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO sage.explain_cache
			(queryid, plan_json, source, captured_at)
		 VALUES ($1, $2::jsonb, 'test', now())`,
		qid, planJSON,
	)
	if err != nil {
		t.Fatalf("insert plan: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx,
			`DELETE FROM sage.explain_cache WHERE queryid = $1`, qid)
	}()

	tu := New(pool, TunerConfig{}, nil, noopLog2)
	c := candidate{QueryID: qid}

	// Hit: deferred set contains the relation.
	deferred := map[string]bool{"public.orders_test_deferral": true}
	reason := tu.deferralReason(ctx, c, deferred)
	if reason == "" {
		t.Error("expected non-empty reason when relation is deferred")
	}

	// Miss: deferred set has a different relation.
	other := map[string]bool{"public.unrelated": true}
	reason = tu.deferralReason(ctx, c, other)
	if reason != "" {
		t.Errorf("expected empty reason when relation not deferred, "+
			"got %q", reason)
	}
}
