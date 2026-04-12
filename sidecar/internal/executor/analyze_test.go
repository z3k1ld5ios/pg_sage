package executor

import (
	"testing"
	"time"
)

func TestIsAnalyzeStatement(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"ANALYZE public.users", true},
		{"analyze public.users", true},
		{"  ANALYZE VERBOSE orders  ", true},
		{"ANALYZE", true},
		{"VACUUM ANALYZE users", false},
		{"SELECT 1", false},
		{"CREATE INDEX foo", false},
		{"", false},
		{"--ANALYZE users", false},
	}
	for _, tt := range tests {
		got := isAnalyzeStatement(tt.sql)
		if got != tt.want {
			t.Errorf(
				"isAnalyzeStatement(%q) = %v, want %v",
				tt.sql, got, tt.want,
			)
		}
	}
}

func TestCheckAnalyzeCooldown_Disabled(t *testing.T) {
	if checkAnalyzeCooldown("public.users", 0) {
		t.Error("cooldown=0 should always return false")
	}
	if checkAnalyzeCooldown("public.users", -1) {
		t.Error("cooldown<0 should always return false")
	}
}

func TestCheckAnalyzeCooldown_NoEntry(t *testing.T) {
	// Fresh key never touched.
	key := "test_cooldown_noentry.table_" +
		time.Now().Format("150405.000000")
	if checkAnalyzeCooldown(key, 10) {
		t.Error(
			"unmarked table should not be in cooldown",
		)
	}
}

func TestCheckAnalyzeCooldown_Recent(t *testing.T) {
	key := "test_cooldown_recent.table_" +
		time.Now().Format("150405.000000")
	markAnalyzed(key)
	if !checkAnalyzeCooldown(key, 10) {
		t.Error(
			"freshly marked table should be in cooldown",
		)
	}
	// Cooldown of 0 disables.
	if checkAnalyzeCooldown(key, 0) {
		t.Error("cooldown=0 should disable")
	}
}

func TestCheckAnalyzeCooldown_Expired(t *testing.T) {
	key := "test_cooldown_expired.table_" +
		time.Now().Format("150405.000000")
	// Manually seed as if analyzed 20 minutes ago.
	recentAnalyzesMu.Lock()
	recentAnalyzes[key] = time.Now().Add(-20 * time.Minute)
	recentAnalyzesMu.Unlock()
	if checkAnalyzeCooldown(key, 10) {
		t.Error(
			"20-min-old analyze should not be in a 10-min cooldown",
		)
	}
}

func TestMarkAnalyzed(t *testing.T) {
	key := "test_mark.table_" +
		time.Now().Format("150405.000000")
	markAnalyzed(key)
	recentAnalyzesMu.Lock()
	defer recentAnalyzesMu.Unlock()
	ts, ok := recentAnalyzes[key]
	if !ok {
		t.Fatal("markAnalyzed did not record the key")
	}
	if time.Since(ts) > time.Second {
		t.Errorf("recorded timestamp stale: %v ago", time.Since(ts))
	}
}
