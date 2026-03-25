package optimizer

import "testing"

func TestExtractTotalCost(t *testing.T) {
	tests := []struct {
		name string
		json string
		want float64
	}{
		{
			name: "valid plan",
			json: `[{"Plan":{"Total Cost":42.5}}]`,
			want: 42.5,
		},
		{
			name: "invalid JSON",
			json: `not json`,
			want: 0,
		},
		{
			name: "empty array",
			json: `[]`,
			want: 0,
		},
		{
			name: "zero cost",
			json: `[{"Plan":{"Total Cost":0}}]`,
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractTotalCost([]byte(tt.json))
			if got != tt.want {
				t.Errorf("extractTotalCost() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsExplainable(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "WITH CTE query",
			query: "WITH cte AS (SELECT 1) SELECT * FROM cte",
			want:  true,
		},
		{
			name:  "DELETE statement",
			query: "DELETE FROM orders",
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isExplainable(tt.query)
			if got != tt.want {
				t.Errorf("isExplainable(%q) = %v, want %v",
					tt.query, got, tt.want)
			}
		})
	}
}
