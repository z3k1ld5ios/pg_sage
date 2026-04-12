package tuner

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"users", `"users"`},
		{"", `""`},
		{`weird"name`, `"weird""name"`},
		{`a"b"c`, `"a""b""c"`},
		{"schema_1", `"schema_1"`},
	}
	for _, tt := range tests {
		got := QuoteIdentifier(tt.in)
		if got != tt.want {
			t.Errorf("QuoteIdentifier(%q) = %q, want %q",
				tt.in, got, tt.want)
		}
	}
}

func TestQuoteQualified(t *testing.T) {
	tests := []struct {
		schema, table, want string
	}{
		{"public", "users", `"public"."users"`},
		{"", "orders", `"public"."orders"`},
		{"sales", "line_items", `"sales"."line_items"`},
		{"", `evil"name`, `"public"."evil""name"`},
	}
	for _, tt := range tests {
		got := QuoteQualified(tt.schema, tt.table)
		if got != tt.want {
			t.Errorf(
				"QuoteQualified(%q,%q) = %q, want %q",
				tt.schema, tt.table, got, tt.want,
			)
		}
	}
}

func TestSplitCanonical(t *testing.T) {
	tests := []struct {
		in, wantSchema, wantTable string
	}{
		{"public.users", "public", "users"},
		{"sales.orders", "sales", "orders"},
		{"orders", "public", "orders"},
		{"  public.users  ", "public", "users"},
		{"", "public", ""},
		{"a.b.c", "a", "b.c"},
	}
	for _, tt := range tests {
		s, table := SplitCanonical(tt.in)
		if s != tt.wantSchema || table != tt.wantTable {
			t.Errorf(
				"SplitCanonical(%q) = (%q,%q), want (%q,%q)",
				tt.in, s, table, tt.wantSchema, tt.wantTable,
			)
		}
	}
}
