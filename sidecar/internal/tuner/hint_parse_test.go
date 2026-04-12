package tuner

import (
	"testing"
)

func TestParseHintText_AllDirectives(t *testing.T) {
	tests := []struct {
		name            string
		in              string
		wantKinds       []string
		wantUnparseable int
	}{
		{
			name:      "HashJoin two aliases",
			in:        "HashJoin(a b)",
			wantKinds: []string{"HashJoin"},
		},
		{
			name:      "NestLoop alias list",
			in:        "NestLoop(t1 t2 t3)",
			wantKinds: []string{"NestLoop"},
		},
		{
			name:      "MergeJoin two aliases",
			in:        "MergeJoin(x y)",
			wantKinds: []string{"MergeJoin"},
		},
		{
			name:      "IndexScan with one index",
			in:        "IndexScan(orders orders_customer_idx)",
			wantKinds: []string{"IndexScan"},
		},
		{
			name:      "IndexScan with multiple indexes",
			in:        "IndexScan(t idx1 idx2 idx3)",
			wantKinds: []string{"IndexScan"},
		},
		{
			name:      "NoIndexScan single alias",
			in:        "NoIndexScan(t)",
			wantKinds: []string{"NoIndexScan"},
		},
		{
			name:      "BitmapScan with index",
			in:        "BitmapScan(t idx_foo)",
			wantKinds: []string{"BitmapScan"},
		},
		{
			name:      "SeqScan",
			in:        "SeqScan(t)",
			wantKinds: []string{"SeqScan"},
		},
		{
			name:      "NoSeqScan",
			in:        "NoSeqScan(t)",
			wantKinds: []string{"NoSeqScan"},
		},
		{
			name:      "Leading nested parens",
			in:        "Leading((t1 t2 t3))",
			wantKinds: []string{"Leading"},
		},
		{
			name:      "Set work_mem",
			in:        `Set(work_mem "256MB")`,
			wantKinds: []string{"Set"},
		},
		{
			name:      "Set single quotes",
			in:        `Set(enable_hashjoin 'off')`,
			wantKinds: []string{"Set"},
		},
		{
			name:      "Rows absolute count",
			in:        "Rows(a b #10000)",
			wantKinds: []string{"Rows"},
		},
		{
			name:      "Rows additive delta",
			in:        "Rows(a b +5)",
			wantKinds: []string{"Rows"},
		},
		{
			name:      "Multiple directives",
			in:        "HashJoin(a b) IndexScan(a a_idx)",
			wantKinds: []string{"HashJoin", "IndexScan"},
		},
		{
			name:            "Unknown head",
			in:              "BogusHint(x)",
			wantKinds:       nil,
			wantUnparseable: 1,
		},
		{
			name:            "Empty string",
			in:              "",
			wantKinds:       nil,
			wantUnparseable: 0,
		},
		{
			name:            "Whitespace only",
			in:              "   \n\t  ",
			wantKinds:       nil,
			wantUnparseable: 0,
		},
		{
			name:            "Malformed — no parens",
			in:              "HashJoin",
			wantKinds:       nil,
			wantUnparseable: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, unparseable := ParseHintText(tc.in)
			if len(parsed) != len(tc.wantKinds) {
				t.Fatalf("parsed = %d, want %d (%v)",
					len(parsed), len(tc.wantKinds), parsed)
			}
			for i, k := range tc.wantKinds {
				if parsed[i].Kind != k {
					t.Errorf("parsed[%d].Kind = %q, want %q",
						i, parsed[i].Kind, k)
				}
			}
			if len(unparseable) != tc.wantUnparseable {
				t.Errorf("unparseable = %d (%v), want %d",
					len(unparseable), unparseable, tc.wantUnparseable)
			}
		})
	}
}

func TestParseHintText_SetFields(t *testing.T) {
	parsed, _ := ParseHintText(`Set(work_mem "256MB")`)
	if len(parsed) != 1 {
		t.Fatalf("parsed = %d, want 1", len(parsed))
	}
	d := parsed[0]
	if d.ParamName != "work_mem" {
		t.Errorf("ParamName = %q, want work_mem", d.ParamName)
	}
	if d.ParamValue != "256MB" {
		t.Errorf("ParamValue = %q, want 256MB", d.ParamValue)
	}
}

func TestParseHintText_IndexScanFields(t *testing.T) {
	parsed, _ := ParseHintText("IndexScan(orders orders_cust_idx orders_date_idx)")
	if len(parsed) != 1 {
		t.Fatalf("parsed = %d, want 1", len(parsed))
	}
	d := parsed[0]
	if len(d.Aliases) != 1 || d.Aliases[0] != "orders" {
		t.Errorf("Aliases = %v, want [orders]", d.Aliases)
	}
	if len(d.IndexNames) != 2 {
		t.Fatalf("IndexNames len = %d, want 2 (%v)",
			len(d.IndexNames), d.IndexNames)
	}
	if d.IndexNames[0] != "orders_cust_idx" ||
		d.IndexNames[1] != "orders_date_idx" {
		t.Errorf("IndexNames = %v", d.IndexNames)
	}
}

func TestParseHintText_LeadingStripsNestedParens(t *testing.T) {
	parsed, _ := ParseHintText("Leading((a b c))")
	if len(parsed) != 1 {
		t.Fatalf("parsed = %d, want 1", len(parsed))
	}
	d := parsed[0]
	if len(d.Aliases) != 3 {
		t.Errorf("Aliases = %v, want 3", d.Aliases)
	}
	if d.Aliases[0] != "a" || d.Aliases[1] != "b" || d.Aliases[2] != "c" {
		t.Errorf("Aliases = %v", d.Aliases)
	}
}

func TestParseHintText_RowsDirective(t *testing.T) {
	parsed, _ := ParseHintText("Rows(a b c #1000)")
	if len(parsed) != 1 {
		t.Fatalf("parsed = %d, want 1", len(parsed))
	}
	d := parsed[0]
	if d.RowsDirective != "#1000" {
		t.Errorf("RowsDirective = %q, want #1000", d.RowsDirective)
	}
	if len(d.Aliases) != 3 {
		t.Errorf("Aliases = %v, want 3", d.Aliases)
	}
}

func TestParseHintText_MixedKnownAndUnknown(t *testing.T) {
	parsed, unparseable := ParseHintText(
		"HashJoin(a b) BogusHint(x) IndexScan(a a_idx)")
	if len(parsed) != 2 {
		t.Errorf("parsed count = %d, want 2", len(parsed))
	}
	if len(unparseable) != 1 {
		t.Errorf("unparseable count = %d, want 1", len(unparseable))
	}
}

// TestUnparseableDirectiveNotMarkedBroken verifies that a fabricated
// unknown directive surfaces as unparseable AND returns no parsed
// directives for that head — the revalidation loop must NOT mark
// such a hint broken (parser limitation != hint problem) per CHECK-R16.
func TestUnparseableDirectiveNotMarkedBroken(t *testing.T) {
	parsed, unparseable := ParseHintText("BogusHint(x)")
	if len(parsed) != 0 {
		t.Errorf("parsed = %v, want empty for unknown head", parsed)
	}
	if !HasUnparseable(unparseable) {
		t.Error("HasUnparseable should be true for BogusHint(x)")
	}
}
