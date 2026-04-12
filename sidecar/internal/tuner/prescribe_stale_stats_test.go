package tuner

import (
	"strings"
	"testing"
)

func TestPrescribeStaleStats_WithSchema(t *testing.T) {
	s := PlanSymptom{
		Kind:         SymptomStaleStats,
		Schema:       "sales",
		RelationName: "orders",
	}
	p := Prescribe(s, TunerConfig{})
	if p == nil {
		t.Fatal("Prescribe returned nil")
	}
	if p.AnalyzeTarget != "sales.orders" {
		t.Errorf(
			"AnalyzeTarget = %q, want sales.orders",
			p.AnalyzeTarget,
		)
	}
	if p.HintDirective != "" {
		t.Errorf(
			"HintDirective should be empty, got %q",
			p.HintDirective,
		)
	}
	if !strings.Contains(p.Rationale, "sales.orders") {
		t.Errorf("rationale missing canonical: %q", p.Rationale)
	}
}

func TestPrescribeStaleStats_DefaultSchema(t *testing.T) {
	s := PlanSymptom{
		Kind:         SymptomStaleStats,
		RelationName: "users",
	}
	p := Prescribe(s, TunerConfig{})
	if p == nil {
		t.Fatal("Prescribe returned nil")
	}
	if p.AnalyzeTarget != "public.users" {
		t.Errorf(
			"AnalyzeTarget = %q, want public.users",
			p.AnalyzeTarget,
		)
	}
}
