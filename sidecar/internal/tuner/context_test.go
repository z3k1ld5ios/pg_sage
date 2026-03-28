package tuner

import (
	"testing"
)

func TestExtractTables_Simple(t *testing.T) {
	tables := extractTables(
		"SELECT * FROM orders o JOIN items i ON o.id = i.order_id",
	)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d: %v", len(tables), tables)
	}
	if tables[0].schema != "public" || tables[0].name != "orders" {
		t.Errorf("table 0: %+v", tables[0])
	}
	if tables[1].schema != "public" || tables[1].name != "items" {
		t.Errorf("table 1: %+v", tables[1])
	}
}

func TestExtractTables_SchemaQualified(t *testing.T) {
	tables := extractTables(
		"SELECT * FROM app.orders WHERE id IN " +
			"(SELECT order_id FROM app.items)",
	)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
	if tables[0].schema != "app" || tables[0].name != "orders" {
		t.Errorf("table 0: %+v", tables[0])
	}
	if tables[1].schema != "app" || tables[1].name != "items" {
		t.Errorf("table 1: %+v", tables[1])
	}
}

func TestExtractTables_FiltersSageAndPg(t *testing.T) {
	tables := extractTables(
		"SELECT * FROM pg_stat_statements " +
			"JOIN sage.findings ON true " +
			"JOIN hint_plan.hints ON true " +
			"JOIN app.users ON true",
	)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %v", len(tables), tables)
	}
	if tables[0].name != "users" {
		t.Errorf("expected users, got %+v", tables[0])
	}
}

func TestExtractTables_Dedup(t *testing.T) {
	tables := extractTables(
		"SELECT * FROM orders JOIN orders ON true",
	)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table (deduped), got %d", len(tables))
	}
}

func TestExtractTables_Update(t *testing.T) {
	tables := extractTables("UPDATE orders SET status = 'done'")
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if tables[0].name != "orders" {
		t.Errorf("got %+v", tables[0])
	}
}

func TestExtractTables_Insert(t *testing.T) {
	tables := extractTables(
		"INSERT INTO orders (id) VALUES (1)",
	)
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
}

func TestExtractTables_LimitsToMax(t *testing.T) {
	// extractTables doesn't limit, but buildQueryContext does
	query := "SELECT * FROM t1 JOIN t2 ON true " +
		"JOIN t3 ON true JOIN t4 ON true " +
		"JOIN t5 ON true JOIN t6 ON true"
	tables := extractTables(query)
	if len(tables) != 6 {
		t.Fatalf("expected 6 tables, got %d", len(tables))
	}
}

func TestParseIntSetting(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"100", 100},
		{"4", 4},
		{"", 0},
		{"invalid", 0},
	}
	for _, tc := range cases {
		got := parseIntSetting(tc.input)
		if got != tc.want {
			t.Errorf("parseIntSetting(%q) = %d, want %d",
				tc.input, got, tc.want)
		}
	}
}
