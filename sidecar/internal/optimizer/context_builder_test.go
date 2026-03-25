package optimizer

import (
	"testing"

	"github.com/pg-sage/sidecar/internal/collector"
)

func TestClassifyWorkload(t *testing.T) {
	tests := []struct {
		name       string
		writeRate  float64
		liveTuples int64
		want       string
	}{
		{"high write rate", 80, 1000, "oltp_write"},
		{"low write high tuples", 5, 500000, "olap"},
		{"low write low tuples", 5, 50000, "oltp_read"},
		{"moderate write under 30", 25, 1000, "oltp_read"},
		{"mixed workload", 50, 1000, "htap"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyWorkload(tt.writeRate, tt.liveTuples)
			if got != tt.want {
				t.Errorf("classifyWorkload(%v, %v) = %q, want %q",
					tt.writeRate, tt.liveTuples, got, tt.want)
			}
		})
	}
}

func TestComputeWriteRate(t *testing.T) {
	tests := []struct {
		name string
		ts   collector.TableStats
		want float64
	}{
		{
			name: "all zeros",
			ts:   collector.TableStats{},
			want: 0,
		},
		{
			name: "100 percent writes",
			ts:   collector.TableStats{NTupIns: 100},
			want: 100,
		},
		{
			name: "50 50 split",
			ts:   collector.TableStats{SeqScan: 50, NTupIns: 50},
			want: 50,
		},
		{
			name: "reads only",
			ts:   collector.TableStats{SeqScan: 100},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeWriteRate(tt.ts)
			if got != tt.want {
				t.Errorf("computeWriteRate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractTablesFromQuery_Simple(t *testing.T) {
	tables := extractTablesFromQuery("SELECT * FROM orders")
	if len(tables) != 1 || tables[0] != "public.orders" {
		t.Errorf("expected [public.orders], got %v", tables)
	}
}

func TestExtractTablesFromQuery_SchemaQualified(t *testing.T) {
	tables := extractTablesFromQuery("SELECT * FROM myschema.orders")
	if len(tables) != 1 || tables[0] != "myschema.orders" {
		t.Errorf("expected [myschema.orders], got %v", tables)
	}
}

func TestExtractTablesFromQuery_Join(t *testing.T) {
	tables := extractTablesFromQuery("SELECT * FROM orders JOIN items ON orders.id = items.order_id")
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
	if tables[0] != "public.orders" {
		t.Errorf("expected public.orders first, got %s", tables[0])
	}
	if tables[1] != "public.items" {
		t.Errorf("expected public.items second, got %s", tables[1])
	}
}

func TestExtractTablesFromQuery_Update(t *testing.T) {
	tables := extractTablesFromQuery("UPDATE orders SET x = 1")
	if len(tables) != 1 || tables[0] != "public.orders" {
		t.Errorf("expected [public.orders], got %v", tables)
	}
}

func TestExtractTablesFromQuery_Insert(t *testing.T) {
	tables := extractTablesFromQuery("INSERT INTO orders VALUES (1)")
	if len(tables) != 1 || tables[0] != "public.orders" {
		t.Errorf("expected [public.orders], got %v", tables)
	}
}

func TestExtractTablesFromQuery_PgTable(t *testing.T) {
	tables := extractTablesFromQuery("SELECT * FROM pg_stat_activity")
	if len(tables) != 0 {
		t.Errorf("expected empty slice for pg_ table, got %v", tables)
	}
}

func TestExtractTablesFromQuery_Subquery(t *testing.T) {
	tables := extractTablesFromQuery(
		"SELECT * FROM orders WHERE id IN (SELECT id FROM items)",
	)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
	if tables[0] != "public.orders" {
		t.Errorf("expected public.orders first, got %s", tables[0])
	}
	if tables[1] != "public.items" {
		t.Errorf("expected public.items second, got %s", tables[1])
	}
}

func TestSkipSchema(t *testing.T) {
	tests := []struct {
		schema string
		want   bool
	}{
		{"sage", true},
		{"pg_catalog", true},
		{"public", false},
	}
	for _, tt := range tests {
		t.Run(tt.schema, func(t *testing.T) {
			got := skipSchema(tt.schema)
			if got != tt.want {
				t.Errorf("skipSchema(%q) = %v, want %v", tt.schema, got, tt.want)
			}
		})
	}
}

func TestFilterByMinCalls(t *testing.T) {
	queries := []QueryInfo{
		{QueryID: 1, Calls: 5},
		{QueryID: 2, Calls: 10},
		{QueryID: 3, Calls: 15},
	}
	got := filterByMinCalls(queries, 10)
	if len(got) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(got))
	}
	if got[0].Calls != 10 || got[1].Calls != 15 {
		t.Errorf("unexpected queries: %v", got)
	}
}

func TestFilterByMinCalls_Empty(t *testing.T) {
	got := filterByMinCalls(nil, 10)
	if len(got) != 0 {
		t.Errorf("expected empty result, got %v", got)
	}
}

func TestCountIndexes(t *testing.T) {
	indexes := []collector.IndexStats{
		{SchemaName: "public", RelName: "orders", IndexRelName: "idx1"},
		{SchemaName: "public", RelName: "orders", IndexRelName: "idx2"},
		{SchemaName: "public", RelName: "items", IndexRelName: "idx3"},
	}
	got := countIndexes(indexes, "public", "orders")
	if got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
}
