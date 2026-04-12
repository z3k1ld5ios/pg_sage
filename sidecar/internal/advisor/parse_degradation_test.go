package advisor

import (
	"strings"
	"testing"
)

// --- parseLLMFindings degradation tests ---
// These test the parse function with inputs that simulate real LLM
// failure modes: garbage output, truncated JSON, valid JSON with
// missing required fields, and partial JSON that stripToJSON cannot
// recover.

func TestParseLLMFindings_CompleteGarbage(t *testing.T) {
	// LLM returned something completely non-JSON (e.g., an error
	// message, a stack trace, or HTML from a proxy).
	cases := []struct {
		name  string
		input string
	}{
		{"binary_garbage", "\x00\x01\x02\xff\xfe"},
		{"html_error", "<html><body>502 Bad Gateway</body></html>"},
		{"plain_error", "Error: rate limit exceeded"},
		{"python_traceback",
			"Traceback (most recent call last):\n  File ..."},
		{"number", "42"},
		{"boolean", "true"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			findings := parseLLMFindings(tc.input, "test", noopLog)
			if findings != nil {
				t.Errorf("expected nil findings for garbage input %q, "+
					"got %d findings", tc.name, len(findings))
			}
		})
	}
}

func TestParseLLMFindings_TruncatedJSON_NoClosingBracket(t *testing.T) {
	// LLM was cut off mid-response. stripToJSON finds '[' but no ']',
	// so it falls through to stripMarkdownFences which returns garbage.
	input := `[{"object_identifier":"public.orders","severity":"warning","rationale":"bloat is`
	findings := parseLLMFindings(input, "test", noopLog)
	if findings != nil {
		t.Errorf("expected nil for truncated JSON without ], got %d",
			len(findings))
	}
}

func TestParseLLMFindings_PartialJSON_BracketsMismatch(t *testing.T) {
	// stripToJSON extracts between [ and ] but the content is broken.
	input := `thinking...[{"object_identifier":"pub` +
		`lic.t1","sev] more text`
	findings := parseLLMFindings(input, "test", noopLog)
	if findings != nil {
		t.Errorf("expected nil for partial JSON, got %d",
			len(findings))
	}
}

func TestParseLLMFindings_EmptyObject(t *testing.T) {
	// LLM returned an array with a completely empty object.
	input := `[{}]`
	findings := parseLLMFindings(input, "test", noopLog)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	// Verify defaults are applied for missing fields.
	f := findings[0]
	if f.ObjectIdentifier != "instance" {
		t.Errorf("expected default ObjectIdentifier 'instance', "+
			"got %q", f.ObjectIdentifier)
	}
	if f.Severity != "info" {
		t.Errorf("expected default severity 'info', got %q",
			f.Severity)
	}
	if f.Recommendation != "" {
		t.Errorf("expected empty recommendation, got %q",
			f.Recommendation)
	}
	if f.ActionRisk != "safe" {
		t.Errorf("expected default ActionRisk 'safe', got %q",
			f.ActionRisk)
	}
}

func TestParseLLMFindings_ValidJSONWrongShape(t *testing.T) {
	// Valid JSON but not an array of objects.
	cases := []struct {
		name  string
		input string
	}{
		{"string_array", `["foo","bar"]`},
		{"number_array", `[1,2,3]`},
		{"nested_arrays", `[[1],[2]]`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			findings := parseLLMFindings(tc.input, "test", noopLog)
			// These should either return nil or empty findings since
			// the type assertions for object_identifier etc. fail
			// gracefully.
			for i, f := range findings {
				if f.ObjectIdentifier != "instance" {
					t.Errorf("finding[%d]: expected default "+
						"ObjectIdentifier, got %q",
						i, f.ObjectIdentifier)
				}
			}
		})
	}
}

func TestParseLLMFindings_ActionRisk_AlterSystem(t *testing.T) {
	raw := `[{"object_identifier":"instance",` +
		`"recommended_sql":"ALTER SYSTEM SET work_mem = '256MB'"}]`
	findings := parseLLMFindings(raw, "test", noopLog)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].ActionRisk != "moderate" {
		t.Errorf("ALTER SYSTEM should be moderate risk, got %q",
			findings[0].ActionRisk)
	}
}

func TestParseLLMFindings_ActionRisk_DropIndex(t *testing.T) {
	raw := `[{"object_identifier":"public.idx_foo",` +
		`"recommended_sql":"DROP INDEX CONCURRENTLY IF EXISTS idx_foo"}]`
	findings := parseLLMFindings(raw, "test", noopLog)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].ActionRisk != "moderate" {
		t.Errorf("DROP INDEX should be moderate risk, got %q",
			findings[0].ActionRisk)
	}
}

func TestParseLLMFindings_MarkdownWrappedGarbage(t *testing.T) {
	// Markdown fences around non-JSON content.
	input := "```json\nSorry, I cannot help with that.\n```"
	findings := parseLLMFindings(input, "test", noopLog)
	if findings != nil {
		t.Errorf("expected nil for markdown-wrapped garbage, "+
			"got %d findings", len(findings))
	}
}

func TestParseLLMFindings_ThinkingTokensWithNoJSON(t *testing.T) {
	// Model emitted only thinking tokens with no actual JSON payload.
	input := "Let me analyze the vacuum configuration.\n\n" +
		"The current settings appear adequate. " +
		"No changes are recommended at this time."
	findings := parseLLMFindings(input, "test", noopLog)
	if findings != nil {
		t.Errorf("expected nil for thinking-only response, "+
			"got %d findings", len(findings))
	}
}

func TestParseLLMFindings_VeryLargePayload(t *testing.T) {
	// Simulate a response with many findings to verify no panic.
	var b strings.Builder
	b.WriteString("[")
	for i := 0; i < 100; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"table":"t` + string(rune('0'+i%10)) + `"}`)
	}
	b.WriteString("]")
	findings := parseLLMFindings(b.String(), "test", noopLog)
	if len(findings) != 100 {
		t.Errorf("expected 100 findings, got %d", len(findings))
	}
}
