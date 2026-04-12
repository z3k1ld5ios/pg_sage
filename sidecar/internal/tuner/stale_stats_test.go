package tuner

import (
	"testing"
	"time"
)

func mkCache(cfg TunerConfig) *StaleStatsCache {
	return &StaleStatsCache{
		cfg:     cfg,
		entries: map[string]staleStatsEntry{},
	}
}

func TestIsStale_NilAndMissingEntry(t *testing.T) {
	var nilCache *StaleStatsCache
	if nilCache.IsStale("public.users") {
		t.Error("nil cache should return false")
	}
	c := mkCache(TunerConfig{StaleStatsModRatio: 0.1})
	if c.IsStale("public.users") {
		t.Error("missing entry should return false")
	}
}

func TestIsStale_ZeroMods(t *testing.T) {
	c := mkCache(TunerConfig{
		StaleStatsModRatio:   0.1,
		StaleStatsAgeMinutes: 10,
	})
	c.entries["public.users"] = staleStatsEntry{
		liveTuples:      1000,
		modSinceAnalyze: 0,
	}
	if c.IsStale("public.users") {
		t.Error("zero modifications should return false")
	}
}

func TestIsStale_ModRatioGateBelow(t *testing.T) {
	c := mkCache(TunerConfig{
		StaleStatsModRatio:   0.2,
		StaleStatsAgeMinutes: 0,
	})
	c.entries["public.users"] = staleStatsEntry{
		liveTuples:      1000,
		modSinceAnalyze: 100, // ratio 0.1 < 0.2
		lastAnalyze:     time.Now().Add(-24 * time.Hour),
	}
	if c.IsStale("public.users") {
		t.Error("below mod ratio should return false")
	}
}

func TestIsStale_ModRatioGateAboveAndAgeOK(t *testing.T) {
	c := mkCache(TunerConfig{
		StaleStatsModRatio:   0.1,
		StaleStatsAgeMinutes: 10,
	})
	c.entries["public.users"] = staleStatsEntry{
		liveTuples:      1000,
		modSinceAnalyze: 500, // ratio 0.5 > 0.1
		lastAnalyze:     time.Now().Add(-1 * time.Hour),
	}
	if !c.IsStale("public.users") {
		t.Error(
			"both gates crossed but IsStale returned false",
		)
	}
}

func TestIsStale_AgeGateTooRecent(t *testing.T) {
	c := mkCache(TunerConfig{
		StaleStatsModRatio:   0.1,
		StaleStatsAgeMinutes: 60,
	})
	c.entries["public.users"] = staleStatsEntry{
		liveTuples:      1000,
		modSinceAnalyze: 900,
		lastAnalyze:     time.Now().Add(-2 * time.Minute),
	}
	if c.IsStale("public.users") {
		t.Error(
			"analyze within age window should return false",
		)
	}
}

func TestIsStale_NeverAnalyzed(t *testing.T) {
	c := mkCache(TunerConfig{
		StaleStatsModRatio:   0.1,
		StaleStatsAgeMinutes: 60,
	})
	// zero time.Time on lastAnalyze/lastAutoAnalyze means
	// the age gate is skipped and mod ratio alone decides.
	c.entries["public.users"] = staleStatsEntry{
		liveTuples:      1000,
		modSinceAnalyze: 500,
	}
	if !c.IsStale("public.users") {
		t.Error("never-analyzed hot table should be stale")
	}
}

func TestIsStale_ZeroLiveTuples(t *testing.T) {
	c := mkCache(TunerConfig{
		StaleStatsModRatio:   0.1,
		StaleStatsAgeMinutes: 0,
	})
	c.entries["public.users"] = staleStatsEntry{
		liveTuples:      0,
		modSinceAnalyze: 1,
		lastAnalyze:     time.Now().Add(-24 * time.Hour),
	}
	// Division-by-zero is avoided; ratio gate is skipped.
	if !c.IsStale("public.users") {
		t.Error("zero live_tup + 1 mod should be stale")
	}
}

func TestSizeMB(t *testing.T) {
	c := mkCache(TunerConfig{})
	c.entries["public.users"] = staleStatsEntry{sizeMB: 42}
	if got := c.SizeMB("public.users"); got != 42 {
		t.Errorf("SizeMB = %d, want 42", got)
	}
	if got := c.SizeMB("public.missing"); got != 0 {
		t.Errorf("missing SizeMB = %d, want 0", got)
	}
	var nilCache *StaleStatsCache
	if got := nilCache.SizeMB("public.users"); got != 0 {
		t.Errorf("nil cache SizeMB = %d, want 0", got)
	}
}

func TestLatestTime(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-1 * time.Hour)
	if got := latestTime(now, earlier); !got.Equal(now) {
		t.Error("latestTime(now, earlier) should return now")
	}
	if got := latestTime(earlier, now); !got.Equal(now) {
		t.Error("latestTime(earlier, now) should return now")
	}
	zero := time.Time{}
	if got := latestTime(zero, now); !got.Equal(now) {
		t.Error("latestTime(zero, now) should return now")
	}
}

// --- annotateForStaleStats ---------------------------------------

const staleStatsTestPlanJSON = `{
  "Plan": {
    "Node Type": "Nested Loop",
    "Plans": [
      {
        "Node Type": "Seq Scan",
        "Relation Name": "orders",
        "Schema": "public"
      },
      {
        "Node Type": "Index Scan",
        "Relation Name": "users",
        "Schema": "public"
      }
    ]
  }
}`

func TestAnnotateForStaleStats_NoCache(t *testing.T) {
	syms := []PlanSymptom{{Kind: SymptomBadNestedLoop}}
	got := annotateForStaleStats(
		[]byte(staleStatsTestPlanJSON), syms, nil,
	)
	if len(got) != 1 || got[0].Kind != SymptomBadNestedLoop {
		t.Errorf(
			"nil cache should passthrough symptoms, got %+v", got,
		)
	}
}

func TestAnnotateForStaleStats_NoBadNL(t *testing.T) {
	c := mkCache(TunerConfig{StaleStatsModRatio: 0.1})
	c.entries["public.users"] = staleStatsEntry{
		liveTuples:      100,
		modSinceAnalyze: 50,
	}
	syms := []PlanSymptom{{Kind: SymptomDiskSort}}
	got := annotateForStaleStats(
		[]byte(staleStatsTestPlanJSON), syms, c,
	)
	if len(got) != 1 || got[0].Kind != SymptomDiskSort {
		t.Errorf("no BNL should passthrough, got %+v", got)
	}
}

func TestAnnotateForStaleStats_NoStaleTables(t *testing.T) {
	c := mkCache(TunerConfig{StaleStatsModRatio: 0.1})
	// neither table in cache → IsStale false → passthrough
	syms := []PlanSymptom{{Kind: SymptomBadNestedLoop}}
	got := annotateForStaleStats(
		[]byte(staleStatsTestPlanJSON), syms, c,
	)
	if len(got) != 1 || got[0].Kind != SymptomBadNestedLoop {
		t.Errorf(
			"no stale tables should passthrough BNL, got %+v", got,
		)
	}
}

func TestAnnotateForStaleStats_ReplacesBNLWithStale(t *testing.T) {
	c := mkCache(TunerConfig{
		StaleStatsModRatio:   0.1,
		StaleStatsAgeMinutes: 0,
	})
	c.entries["public.orders"] = staleStatsEntry{
		liveTuples:      1000,
		modSinceAnalyze: 900,
	}
	syms := []PlanSymptom{
		{Kind: SymptomBadNestedLoop},
		{Kind: SymptomDiskSort},
	}
	got := annotateForStaleStats(
		[]byte(staleStatsTestPlanJSON), syms, c,
	)
	// BNL dropped, DiskSort kept, one StaleStats added.
	if len(got) != 2 {
		t.Fatalf("got %d symptoms, want 2: %+v", len(got), got)
	}
	var sawDiskSort, sawStale bool
	for _, s := range got {
		switch s.Kind {
		case SymptomBadNestedLoop:
			t.Error("BNL should have been dropped")
		case SymptomDiskSort:
			sawDiskSort = true
		case SymptomStaleStats:
			sawStale = true
			if s.RelationName != "orders" {
				t.Errorf(
					"StaleStats.RelationName = %q, want orders",
					s.RelationName,
				)
			}
			if s.Schema != "public" {
				t.Errorf(
					"StaleStats.Schema = %q, want public", s.Schema,
				)
			}
			if s.Detail["canonical"] != "public.orders" {
				t.Errorf(
					"canonical = %v, want public.orders",
					s.Detail["canonical"],
				)
			}
		}
	}
	if !sawDiskSort || !sawStale {
		t.Errorf(
			"missing symptoms: diskSort=%v stale=%v",
			sawDiskSort, sawStale,
		)
	}
}

func TestAnnotateForStaleStats_EmptyPlanJSON(t *testing.T) {
	c := mkCache(TunerConfig{StaleStatsModRatio: 0.1})
	syms := []PlanSymptom{{Kind: SymptomBadNestedLoop}}
	got := annotateForStaleStats(nil, syms, c)
	if len(got) != 1 || got[0].Kind != SymptomBadNestedLoop {
		t.Errorf(
			"empty plan should passthrough, got %+v", got,
		)
	}
}
