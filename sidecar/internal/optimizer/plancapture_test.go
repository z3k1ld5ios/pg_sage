package optimizer

import (
	"strings"
	"testing"
)

func TestSummarizePlan_SeqScan(t *testing.T) {
	plan := []byte(`[{"Plan":{"Node Type":"Seq Scan","Total Cost":100.0,"Plan Rows":1000}}]`)
	ps := summarizePlan(plan, 1)
	if ps.ScanType != "Seq Scan" {
		t.Errorf("ScanType = %q, want %q", ps.ScanType, "Seq Scan")
	}
	if !strings.Contains(ps.Summary, "Seq Scan") {
		t.Errorf("Summary = %q, want it to contain %q", ps.Summary, "Seq Scan")
	}
}

func TestSummarizePlan_HeapFetches(t *testing.T) {
	plan := []byte(
		`[{"Plan":{"Node Type":"Index Scan","Total Cost":50.0,` +
			`"Plan Rows":100,"Heap Fetches":42}}]`,
	)
	ps := summarizePlan(plan, 2)
	if ps.HeapFetches != 42 {
		t.Errorf("HeapFetches = %d, want 42", ps.HeapFetches)
	}
}

func TestSummarizePlan_DiskSort(t *testing.T) {
	plan := []byte(
		`[{"Plan":{"Node Type":"Sort","Total Cost":200.0,` +
			`"Plan Rows":5000,"Sort Space Used":1024,` +
			`"Sort Space Type":"Disk"}}]`,
	)
	ps := summarizePlan(plan, 3)
	if ps.SortDisk != 1024 {
		t.Errorf("SortDisk = %d, want 1024", ps.SortDisk)
	}
}

func TestSummarizePlan_RowsRemoved(t *testing.T) {
	plan := []byte(
		`[{"Plan":{"Node Type":"Seq Scan","Total Cost":100.0,` +
			`"Plan Rows":100,"Rows Removed by Filter":900}}]`,
	)
	ps := summarizePlan(plan, 4)
	if ps.RowsRemoved != 900 {
		t.Errorf("RowsRemoved = %d, want 900", ps.RowsRemoved)
	}
}

func TestSummarizePlan_AllFields(t *testing.T) {
	plan := []byte(
		`[{"Plan":{"Node Type":"Index Scan","Total Cost":50.0,` +
			`"Plan Rows":100,"Heap Fetches":42,` +
			`"Sort Space Used":512,"Sort Space Type":"Disk",` +
			`"Rows Removed by Filter":10}}]`,
	)
	ps := summarizePlan(plan, 5)
	if ps.ScanType != "Index Scan" {
		t.Errorf("ScanType = %q, want %q", ps.ScanType, "Index Scan")
	}
	if ps.HeapFetches != 42 {
		t.Errorf("HeapFetches = %d, want 42", ps.HeapFetches)
	}
	if ps.SortDisk != 512 {
		t.Errorf("SortDisk = %d, want 512", ps.SortDisk)
	}
	if ps.RowsRemoved != 10 {
		t.Errorf("RowsRemoved = %d, want 10", ps.RowsRemoved)
	}
	for _, part := range []string{
		"Index Scan", "Rows Removed: 10", "Heap Fetches: 42", "Sort Disk: 512kB",
	} {
		if !strings.Contains(ps.Summary, part) {
			t.Errorf("Summary = %q, missing %q", ps.Summary, part)
		}
	}
}

func TestSummarizePlan_InvalidJSON(t *testing.T) {
	ps := summarizePlan([]byte(`not json`), 6)
	if ps.ScanType != "" {
		t.Errorf("ScanType = %q, want empty string", ps.ScanType)
	}
}

func TestSummarizePlan_EmptyArray(t *testing.T) {
	ps := summarizePlan([]byte(`[]`), 7)
	if ps.ScanType != "" {
		t.Errorf("ScanType = %q, want empty string", ps.ScanType)
	}
}

func TestSummarizePlan_Truncation(t *testing.T) {
	longType := strings.Repeat("X", 210)
	plan := []byte(
		`[{"Plan":{"Node Type":"` + longType +
			`","Total Cost":1.0,"Plan Rows":1}}]`,
	)
	ps := summarizePlan(plan, 8)
	if len(ps.Summary) > 200 {
		t.Errorf("Summary length = %d, want <= 200", len(ps.Summary))
	}
	if !strings.HasSuffix(ps.Summary, "...") {
		t.Errorf("Summary = %q, want suffix %q", ps.Summary, "...")
	}
}
