package optimizer

import (
	"math"
	"testing"
)

func TestFingerprintQuery(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "simple select normalized",
			in:   "SELECT id, name FROM users WHERE active = true",
			want: "select id, name from users where active = true",
		},
		{
			name: "IN-list collapsed",
			in:   "SELECT * FROM t WHERE id IN ($1, $2, $3)",
			want: "select * from t where id in (...)",
		},
		{
			name: "numeric literals replaced",
			in:   "SELECT * FROM t WHERE id = 42",
			want: "select * from t where id = ?",
		},
		{
			name: "$N params preserved",
			in:   "SELECT * FROM t WHERE id = $1",
			want: "select * from t where id = $1",
		},
		{
			name: "whitespace normalized",
			in:   "SELECT   id\t\nFROM   users",
			want: "select id from users",
		},
		{
			name: "case insensitive output",
			in:   "SELECT ID FROM USERS WHERE NAME = $1",
			want: "select id from users where name = $1",
		},
		{
			name: "empty string returns empty",
			in:   "",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FingerprintQuery(tt.in)
			if got != tt.want {
				t.Errorf("FingerprintQuery(%q)\n got  %q\n want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestGroupByFingerprint(t *testing.T) {
	t.Run("same fingerprint merged with stats summed", func(t *testing.T) {
		queries := []QueryInfo{
			{QueryID: 1, Text: "SELECT * FROM t WHERE id = 1", Calls: 10, TotalTimeMs: 100, MeanTimeMs: 10},
			{QueryID: 2, Text: "SELECT * FROM t WHERE id = 99", Calls: 20, TotalTimeMs: 200, MeanTimeMs: 10},
		}
		result := GroupByFingerprint(queries)
		if len(result) != 1 {
			t.Fatalf("expected 1 group, got %d", len(result))
		}
		if result[0].Calls != 30 {
			t.Errorf("Calls: got %d, want 30", result[0].Calls)
		}
		if result[0].TotalTimeMs != 300 {
			t.Errorf("TotalTimeMs: got %f, want 300", result[0].TotalTimeMs)
		}
	})

	t.Run("different fingerprints stay separate", func(t *testing.T) {
		queries := []QueryInfo{
			{QueryID: 1, Text: "SELECT * FROM t WHERE id = 1", Calls: 5, TotalTimeMs: 50},
			{QueryID: 2, Text: "DELETE FROM t WHERE id = 1", Calls: 3, TotalTimeMs: 30},
		}
		result := GroupByFingerprint(queries)
		if len(result) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(result))
		}
	})

	t.Run("representative is highest calls", func(t *testing.T) {
		queries := []QueryInfo{
			{QueryID: 1, Text: "SELECT * FROM t WHERE id = 1", Calls: 5, TotalTimeMs: 50},
			{QueryID: 2, Text: "SELECT * FROM t WHERE id = 99", Calls: 20, TotalTimeMs: 200},
		}
		result := GroupByFingerprint(queries)
		if len(result) != 1 {
			t.Fatalf("expected 1 group, got %d", len(result))
		}
		if result[0].QueryID != 2 {
			t.Errorf("representative QueryID: got %d, want 2", result[0].QueryID)
		}
	})

	t.Run("weighted MeanTimeMs computed correctly", func(t *testing.T) {
		queries := []QueryInfo{
			{QueryID: 1, Text: "SELECT * FROM t WHERE id = 1", Calls: 10, TotalTimeMs: 100},
			{QueryID: 2, Text: "SELECT * FROM t WHERE id = 2", Calls: 30, TotalTimeMs: 300},
		}
		result := GroupByFingerprint(queries)
		if len(result) != 1 {
			t.Fatalf("expected 1 group, got %d", len(result))
		}
		// MeanTimeMs = totalTimeMs / totalCalls = 400 / 40 = 10.0
		want := 10.0
		if math.Abs(result[0].MeanTimeMs-want) > 0.001 {
			t.Errorf("MeanTimeMs: got %f, want %f", result[0].MeanTimeMs, want)
		}
	})

	t.Run("empty input returns empty output", func(t *testing.T) {
		result := GroupByFingerprint(nil)
		if len(result) != 0 {
			t.Errorf("expected empty result, got %d entries", len(result))
		}
	})

	t.Run("single query returned as-is", func(t *testing.T) {
		queries := []QueryInfo{
			{QueryID: 7, Text: "SELECT 1", Calls: 5, TotalTimeMs: 25, MeanTimeMs: 5},
		}
		result := GroupByFingerprint(queries)
		if len(result) != 1 {
			t.Fatalf("expected 1 result, got %d", len(result))
		}
		if result[0].QueryID != 7 {
			t.Errorf("QueryID: got %d, want 7", result[0].QueryID)
		}
		if result[0].Calls != 5 {
			t.Errorf("Calls: got %d, want 5", result[0].Calls)
		}
	})
}
