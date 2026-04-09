package analyzer

import (
	"context"
	"testing"
)

// canonicalTable normalizes table refs so the deferred-tables set
// passed to the tuner reliably matches plan-extracted relations.
// The optimizer LLM may emit unqualified names, while plans always
// expose schema separately, so both sides need a canonical form.

func TestCanonicalTable_Bare(t *testing.T) {
	if got := canonicalTable("orders"); got != "public.orders" {
		t.Errorf("got %q, want public.orders", got)
	}
}

func TestCanonicalTable_Qualified(t *testing.T) {
	if got := canonicalTable("billing.invoices"); got != "billing.invoices" {
		t.Errorf("got %q, want billing.invoices", got)
	}
}

func TestCanonicalTable_MixedCase(t *testing.T) {
	if got := canonicalTable("PUBLIC.Orders"); got != "public.orders" {
		t.Errorf("got %q, want public.orders", got)
	}
}

func TestCanonicalTable_Whitespace(t *testing.T) {
	if got := canonicalTable("  public.orders  "); got != "public.orders" {
		t.Errorf("got %q, want public.orders", got)
	}
}

func TestCanonicalTable_Empty(t *testing.T) {
	if got := canonicalTable(""); got != "" {
		t.Errorf("got %q, want empty", got)
	}
	if got := canonicalTable("   "); got != "" {
		t.Errorf("got %q for whitespace, want empty", got)
	}
}

func TestCanonicalTable_BareUppercase(t *testing.T) {
	if got := canonicalTable("Orders"); got != "public.orders" {
		t.Errorf("got %q, want public.orders", got)
	}
}

// ----------------------------------------------------------------
// openIndexRecommendationTables — DB integration
// ----------------------------------------------------------------

func TestOpenIndexRecommendationTables_NilPool(t *testing.T) {
	a := New(nil, coverageTestConfig(),
		nil, nil, nil, nil, nil, noopLog)
	got := a.openIndexRecommendationTables(context.Background())
	if got != nil {
		t.Errorf("expected nil for nil pool, got %v", got)
	}
}

func TestOpenIndexRecommendationTables_FiltersByCategoryAndStatus(t *testing.T) {
	pool := phase2Pool(t)
	cfg := phase2Config()
	a := New(pool, cfg, nil, nil, nil, nil, nil, noopLog)
	ctx := context.Background()

	// Wipe any leftover findings from prior runs that would
	// pollute our assertion.
	_, err := pool.Exec(ctx,
		`DELETE FROM sage.findings WHERE title LIKE 'deferred_test_%'`)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx,
			`DELETE FROM sage.findings WHERE title LIKE 'deferred_test_%'`)
	}()

	// Insert one open index finding (should appear), one resolved
	// index finding (should NOT appear), and one open non-index
	// finding (should NOT appear).
	rows := []struct {
		category, ident, status, title string
	}{
		{"missing_index", "public.orders",
			"open", "deferred_test_open"},
		{"missing_index", "public.archived",
			"resolved", "deferred_test_resolved"},
		{"vacuum_bloat", "public.bloat",
			"open", "deferred_test_vacuum"},
		{"composite_index", "Sales.Items",
			"open", "deferred_test_uppercase"},
	}
	for _, r := range rows {
		_, err := pool.Exec(ctx,
			`INSERT INTO sage.findings
				(category, severity, object_type,
				 object_identifier, title, detail, status)
			 VALUES ($1, 'warning', 'index',
				 $2, $3, '{}'::jsonb, $4)`,
			r.category, r.ident, r.title, r.status,
		)
		if err != nil {
			t.Fatalf("insert %q: %v", r.title, err)
		}
	}

	got := a.openIndexRecommendationTables(ctx)
	set := make(map[string]bool, len(got))
	for _, name := range got {
		set[name] = true
	}

	if !set["public.orders"] {
		t.Errorf("expected public.orders in result, got %v", got)
	}
	if !set["sales.items"] {
		t.Errorf("expected sales.items (lowercased) in result, got %v",
			got)
	}
	if set["public.archived"] {
		t.Errorf("resolved finding should be excluded: %v", got)
	}
	if set["public.bloat"] {
		t.Errorf("non-index finding should be excluded: %v", got)
	}
}
