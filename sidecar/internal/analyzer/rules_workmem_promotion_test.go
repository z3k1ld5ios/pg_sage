package analyzer

import (
	"strings"
	"testing"
)

// TestQuoteRoleIdentifier covers the identifier-quoting behaviour
// for plain, mixed-case, spaced, reserved-word, and embedded-quote
// role names (CHECK-W07, CHECK-W09).
func TestQuoteRoleIdentifier(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"plain", "app", `"app"`},
		{"mixed case with space", "Mixed Case", `"Mixed Case"`},
		{"reserved word", "user", `"user"`},
		{"embedded quote", `ro"le`, `"ro""le"`},
		{"empty", "", `""`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := quoteRoleIdentifier(tc.in)
			if got != tc.want {
				t.Errorf("quoteRoleIdentifier(%q) = %q, want %q",
					tc.in, got, tc.want)
			}
		})
	}
}

// TestBuildWorkMemPromotionFinding_ShapeAndFields exercises the
// pure finding builder with representative inputs. Happy path.
func TestBuildWorkMemPromotionFinding_ShapeAndFields(t *testing.T) {
	f := buildWorkMemPromotionFinding("app_worker", 5, 256, 5)

	if f.Category != "work_mem_promotion" {
		t.Errorf("Category = %q, want work_mem_promotion", f.Category)
	}
	if f.Severity != "info" {
		t.Errorf("Severity = %q, want info", f.Severity)
	}
	if f.ObjectType != "role" {
		t.Errorf("ObjectType = %q, want role", f.ObjectType)
	}
	if f.ObjectIdentifier != "app_worker" {
		t.Errorf("ObjectIdentifier = %q, want app_worker",
			f.ObjectIdentifier)
	}
	if f.ActionRisk != "moderate" {
		t.Errorf("ActionRisk = %q, want moderate", f.ActionRisk)
	}
	if got := f.Detail["role_name"]; got != "app_worker" {
		t.Errorf("Detail.role_name = %v, want app_worker", got)
	}
	if got := f.Detail["hint_count"]; got != 5 {
		t.Errorf("Detail.hint_count = %v, want 5", got)
	}
	if got := f.Detail["max_mb"]; got != 256 {
		t.Errorf("Detail.max_mb = %v, want 256", got)
	}
	if got := f.Detail["suggested_work_mem"]; got != "256MB" {
		t.Errorf("Detail.suggested_work_mem = %v, want 256MB", got)
	}
	wantSQL := `ALTER ROLE "app_worker" SET work_mem = '256MB'`
	if f.RecommendedSQL != wantSQL {
		t.Errorf("RecommendedSQL = %q, want %q",
			f.RecommendedSQL, wantSQL)
	}
	wantRollback := `ALTER ROLE "app_worker" RESET work_mem`
	if f.RollbackSQL != wantRollback {
		t.Errorf("RollbackSQL = %q, want %q",
			f.RollbackSQL, wantRollback)
	}
	if !strings.Contains(f.Recommendation, "ALTER ROLE") ||
		!strings.Contains(f.Recommendation, "256MB") {
		t.Errorf("Recommendation missing key tokens: %q",
			f.Recommendation)
	}
}

// TestBuildWorkMemPromotionFinding_MaxOfMixedSizes covers CHECK-W04:
// five hints for same role with sizes (128, 256, 256, 256, 512),
// the builder receives maxMB=512 and must emit that value in both
// the recommended SQL and the detail payload.
func TestBuildWorkMemPromotionFinding_MaxOfMixedSizes(t *testing.T) {
	f := buildWorkMemPromotionFinding("app", 5, 512, 5)
	if !strings.Contains(f.RecommendedSQL, "'512MB'") {
		t.Errorf("RecommendedSQL = %q, want containing '512MB'",
			f.RecommendedSQL)
	}
	if got := f.Detail["max_mb"]; got != 512 {
		t.Errorf("Detail.max_mb = %v, want 512", got)
	}
}

// TestBuildWorkMemPromotionFinding_ReservedWordRoleQuoted —
// CHECK-W09 surface: a role literally named `user` must be wrapped
// in double quotes in the emitted SQL so PostgreSQL parses it.
func TestBuildWorkMemPromotionFinding_ReservedWordRoleQuoted(t *testing.T) {
	f := buildWorkMemPromotionFinding("user", 5, 128, 5)
	if !strings.Contains(f.RecommendedSQL, `"user"`) {
		t.Errorf("RecommendedSQL = %q, want to quote reserved word",
			f.RecommendedSQL)
	}
}

// TestBuildWorkMemPromotionFinding_MixedCaseRoleQuoted covers
// CHECK-W07: role name with a literal space requires double
// quotes so `ALTER ROLE "Mixed Case" ...` parses correctly.
func TestBuildWorkMemPromotionFinding_MixedCaseRoleQuoted(t *testing.T) {
	f := buildWorkMemPromotionFinding("Mixed Case", 5, 256, 5)
	if !strings.Contains(f.RecommendedSQL, `"Mixed Case"`) {
		t.Errorf("RecommendedSQL = %q, want to quote spaced name",
			f.RecommendedSQL)
	}
}

// TestCheckWorkMemPromotion_NilPool — defensive: if the analyzer
// is constructed with a nil pool (e.g. during unit-test scaffolding)
// the advisor must return an empty slice and not panic.
func TestCheckWorkMemPromotion_NilPool(t *testing.T) {
	a := &Analyzer{
		pool:  nil,
		cfg:   testConfig(),
		logFn: noopLog,
	}
	a.cfg.Analyzer.WorkMemPromotionThreshold = 5
	findings := a.checkWorkMemPromotion(nil)
	if findings != nil {
		t.Errorf("expected nil findings for nil pool, got %v",
			findings)
	}
}

// TestCheckWorkMemPromotion_ThresholdZeroDisables — the advisor is
// opt-out via setting the threshold to 0 or negative. Verifies the
// early-exit branch so the SQL query is never issued.
func TestCheckWorkMemPromotion_ThresholdZeroDisables(t *testing.T) {
	a := &Analyzer{
		pool:  nil, // would panic if query issued
		cfg:   testConfig(),
		logFn: noopLog,
	}
	a.cfg.Analyzer.WorkMemPromotionThreshold = 0
	findings := a.checkWorkMemPromotion(nil)
	if findings != nil {
		t.Errorf("expected nil findings for disabled threshold, got %v",
			findings)
	}
}
