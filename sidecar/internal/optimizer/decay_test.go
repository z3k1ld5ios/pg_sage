package optimizer

import (
	"testing"
)

func TestComputeDecayPct(t *testing.T) {
	tests := []struct {
		name    string
		prior   int64
		current int64
		want    float64
	}{
		{"50% decline", 1000, 500, 50.0},
		{"100% decline", 1000, 0, 100.0},
		{"no baseline", 0, 100, 0.0},
		{"usage increased", 100, 200, -100.0},
		{"no change", 100, 100, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeDecayPct(tt.prior, tt.current)
			if got != tt.want {
				t.Errorf("ComputeDecayPct(%d, %d) = %v, want %v",
					tt.prior, tt.current, got, tt.want)
			}
		})
	}
}

func TestAnalyzeDecay(t *testing.T) {
	tests := []struct {
		name       string
		current    []IndexInfo
		historical map[string]int64
		threshold  float64
		wantCount  int
		wantNames  []string
	}{
		{
			name: "one decaying above threshold",
			current: []IndexInfo{
				{Name: "idx_a", Scans: 200},
				{Name: "idx_b", Scans: 900},
			},
			historical: map[string]int64{
				"idx_a": 1000,
				"idx_b": 1000,
			},
			threshold: 50.0,
			wantCount: 2,
			wantNames: []string{"idx_a"},
		},
		{
			name:    "no historical data",
			current: []IndexInfo{{Name: "idx_c", Scans: 500}},
			historical: map[string]int64{},
			threshold:  50.0,
			wantCount:  0,
			wantNames:  nil,
		},
		{
			name: "all below threshold",
			current: []IndexInfo{
				{Name: "idx_d", Scans: 800},
				{Name: "idx_e", Scans: 700},
			},
			historical: map[string]int64{
				"idx_d": 1000,
				"idx_e": 1000,
			},
			threshold: 50.0,
			wantCount: 2,
			wantNames: nil, // none should be flagged as decaying
		},
		{
			name: "threshold 50: 60% flagged 40% not",
			current: []IndexInfo{
				{Name: "idx_f", Scans: 400},
				{Name: "idx_g", Scans: 600},
			},
			historical: map[string]int64{
				"idx_f": 1000,
				"idx_g": 1000,
			},
			threshold: 50.0,
			wantCount: 2,
			wantNames: []string{"idx_f"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := AnalyzeDecay(tt.current, tt.historical, tt.threshold)

			if len(results) != tt.wantCount {
				t.Fatalf("got %d results, want %d", len(results), tt.wantCount)
			}

			decaying := make(map[string]bool)
			for _, r := range results {
				if r.IsDecaying {
					decaying[r.IndexName] = true
				}
			}

			for _, name := range tt.wantNames {
				if !decaying[name] {
					t.Errorf("expected %s to be flagged as decaying", name)
				}
			}

			// Verify non-wanted indexes are NOT flagged
			for _, r := range results {
				if r.IsDecaying && !contains(tt.wantNames, r.IndexName) {
					t.Errorf("unexpected decaying index: %s (%.1f%%)",
						r.IndexName, r.DecayPct)
				}
			}
		})
	}
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
