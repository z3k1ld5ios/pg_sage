package alerting

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// groupBySeverity
// ---------------------------------------------------------------------------

func TestGroupBySeverity_Empty(t *testing.T) {
	got := groupBySeverity([]AlertFinding{})
	if got == nil {
		t.Fatal("expected non-nil map for empty input")
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(got))
	}
}

func TestGroupBySeverity_Nil(t *testing.T) {
	got := groupBySeverity(nil)
	if got == nil {
		t.Fatal("expected non-nil map for nil input")
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(got))
	}
}

func TestGroupBySeverity_SingleFinding(t *testing.T) {
	f := AlertFinding{
		ID:       1,
		Severity: "critical",
		Title:    "Unused index",
	}
	got := groupBySeverity([]AlertFinding{f})

	if len(got) != 1 {
		t.Fatalf("expected 1 group, got %d", len(got))
	}
	crit, ok := got["critical"]
	if !ok {
		t.Fatal("expected 'critical' key in map")
	}
	if len(crit) != 1 {
		t.Fatalf("expected 1 finding in critical, got %d",
			len(crit))
	}
	if crit[0].ID != 1 {
		t.Fatalf("expected finding ID=1, got %d", crit[0].ID)
	}
	if crit[0].Title != "Unused index" {
		t.Fatalf("expected title 'Unused index', got %q",
			crit[0].Title)
	}
}

func TestGroupBySeverity_SameSeverity(t *testing.T) {
	findings := []AlertFinding{
		{ID: 1, Severity: "warning", Title: "A"},
		{ID: 2, Severity: "warning", Title: "B"},
		{ID: 3, Severity: "warning", Title: "C"},
	}
	got := groupBySeverity(findings)

	if len(got) != 1 {
		t.Fatalf("expected 1 group, got %d", len(got))
	}
	warns := got["warning"]
	if len(warns) != 3 {
		t.Fatalf("expected 3 findings, got %d", len(warns))
	}
	// Verify order is preserved.
	for i, f := range warns {
		if f.ID != int64(i+1) {
			t.Errorf("finding[%d]: expected ID=%d, got %d",
				i, i+1, f.ID)
		}
	}
}

func TestGroupBySeverity_MultipleSeverities(t *testing.T) {
	findings := []AlertFinding{
		{ID: 1, Severity: "critical", Title: "Seq exhaustion"},
		{ID: 2, Severity: "warning", Title: "Unused index"},
		{ID: 3, Severity: "critical", Title: "Replication lag"},
		{ID: 4, Severity: "info", Title: "Vacuum suggested"},
		{ID: 5, Severity: "warning", Title: "Bloated table"},
	}
	got := groupBySeverity(findings)

	if len(got) != 3 {
		t.Fatalf("expected 3 groups, got %d", len(got))
	}

	crits := got["critical"]
	if len(crits) != 2 {
		t.Fatalf("expected 2 critical, got %d", len(crits))
	}
	if crits[0].ID != 1 || crits[1].ID != 3 {
		t.Fatalf("critical IDs wrong: got %d, %d",
			crits[0].ID, crits[1].ID)
	}

	warns := got["warning"]
	if len(warns) != 2 {
		t.Fatalf("expected 2 warning, got %d", len(warns))
	}
	if warns[0].ID != 2 || warns[1].ID != 5 {
		t.Fatalf("warning IDs wrong: got %d, %d",
			warns[0].ID, warns[1].ID)
	}

	infos := got["info"]
	if len(infos) != 1 {
		t.Fatalf("expected 1 info, got %d", len(infos))
	}
	if infos[0].ID != 4 {
		t.Fatalf("info ID wrong: got %d", infos[0].ID)
	}
}

func TestGroupBySeverity_PreservesAllFields(t *testing.T) {
	now := time.Now()
	f := AlertFinding{
		ID:               42,
		Category:         "index_unused",
		Severity:         "warning",
		Title:            "Unused index idx_foo",
		ObjectType:       "index",
		ObjectIdentifier: "idx_foo",
		OccurrenceCount:  7,
		Recommendation:   "DROP INDEX idx_foo",
		FirstSeen:        now.Add(-24 * time.Hour),
		LastSeen:         now,
	}
	got := groupBySeverity([]AlertFinding{f})
	result := got["warning"][0]

	if result.ID != 42 {
		t.Errorf("ID: got %d, want 42", result.ID)
	}
	if result.Category != "index_unused" {
		t.Errorf("Category: got %q, want 'index_unused'",
			result.Category)
	}
	if result.Title != "Unused index idx_foo" {
		t.Errorf("Title: got %q", result.Title)
	}
	if result.ObjectType != "index" {
		t.Errorf("ObjectType: got %q", result.ObjectType)
	}
	if result.ObjectIdentifier != "idx_foo" {
		t.Errorf("ObjectIdentifier: got %q",
			result.ObjectIdentifier)
	}
	if result.OccurrenceCount != 7 {
		t.Errorf("OccurrenceCount: got %d, want 7",
			result.OccurrenceCount)
	}
	if result.Recommendation != "DROP INDEX idx_foo" {
		t.Errorf("Recommendation: got %q",
			result.Recommendation)
	}
	if !result.FirstSeen.Equal(f.FirstSeen) {
		t.Errorf("FirstSeen mismatch")
	}
	if !result.LastSeen.Equal(f.LastSeen) {
		t.Errorf("LastSeen mismatch")
	}
}

func TestGroupBySeverity_EmptySeverityString(t *testing.T) {
	findings := []AlertFinding{
		{ID: 1, Severity: ""},
		{ID: 2, Severity: ""},
	}
	got := groupBySeverity(findings)
	if len(got) != 1 {
		t.Fatalf("expected 1 group, got %d", len(got))
	}
	empty := got[""]
	if len(empty) != 2 {
		t.Fatalf("expected 2 findings with empty severity, got %d",
			len(empty))
	}
}

// ---------------------------------------------------------------------------
// parseHour
// ---------------------------------------------------------------------------

func TestParseHour_ValidWithMinutes(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"22:00", 22},
		{"00:30", 0},
		{"23:59", 23},
		{"0:00", 0},
		{"12:00", 12},
		{"1:30", 1},
	}
	for _, tc := range cases {
		got := parseHour(tc.input)
		if got != tc.want {
			t.Errorf("parseHour(%q) = %d, want %d",
				tc.input, got, tc.want)
		}
	}
}

func TestParseHour_Empty(t *testing.T) {
	got := parseHour("")
	if got != -1 {
		t.Errorf("parseHour(\"\") = %d, want -1", got)
	}
}

func TestParseHour_Invalid(t *testing.T) {
	cases := []struct {
		input string
		desc  string
	}{
		{"abc", "non-numeric"},
		{"25:00", "hour out of range high"},
		{"-1:00", "negative hour"},
		{"foo:bar", "all non-numeric"},
		{"24:00", "hour exactly 24"},
	}
	for _, tc := range cases {
		got := parseHour(tc.input)
		if got != -1 {
			t.Errorf("parseHour(%q) [%s] = %d, want -1",
				tc.input, tc.desc, got)
		}
	}
}

func TestParseHour_NoColon(t *testing.T) {
	// SplitN with ":" on "12" returns ["12"], so Atoi("12") = 12.
	got := parseHour("12")
	if got != 12 {
		t.Errorf("parseHour(\"12\") = %d, want 12", got)
	}
}

func TestParseHour_NoColon_Invalid(t *testing.T) {
	got := parseHour("99")
	if got != -1 {
		t.Errorf("parseHour(\"99\") = %d, want -1", got)
	}
}

func TestParseHour_Whitespace(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"  22:00  ", 22},
		{"\t06:00\t", 6},
		{" 0:00 ", 0},
	}
	for _, tc := range cases {
		got := parseHour(tc.input)
		if got != tc.want {
			t.Errorf("parseHour(%q) = %d, want %d",
				tc.input, got, tc.want)
		}
	}
}

func TestParseHour_WhitespaceOnly(t *testing.T) {
	got := parseHour("   ")
	if got != -1 {
		t.Errorf("parseHour(\"   \") = %d, want -1", got)
	}
}

func TestParseHour_Boundaries(t *testing.T) {
	// Hour 0 is valid.
	if got := parseHour("0:00"); got != 0 {
		t.Errorf("parseHour(\"0:00\") = %d, want 0", got)
	}
	// Hour 23 is valid.
	if got := parseHour("23:00"); got != 23 {
		t.Errorf("parseHour(\"23:00\") = %d, want 23", got)
	}
	// Hour 24 is invalid.
	if got := parseHour("24:00"); got != -1 {
		t.Errorf("parseHour(\"24:00\") = %d, want -1", got)
	}
}

// ---------------------------------------------------------------------------
// maxDuration
// ---------------------------------------------------------------------------

func TestMaxDuration_AGreater(t *testing.T) {
	a := 10 * time.Minute
	b := 5 * time.Minute
	got := maxDuration(a, b)
	if got != a {
		t.Errorf("maxDuration(%v, %v) = %v, want %v",
			a, b, got, a)
	}
}

func TestMaxDuration_BGreater(t *testing.T) {
	a := 5 * time.Minute
	b := 10 * time.Minute
	got := maxDuration(a, b)
	if got != b {
		t.Errorf("maxDuration(%v, %v) = %v, want %v",
			a, b, got, b)
	}
}

func TestMaxDuration_Equal(t *testing.T) {
	a := 7 * time.Minute
	b := 7 * time.Minute
	got := maxDuration(a, b)
	if got != a {
		t.Errorf("maxDuration(%v, %v) = %v, want %v",
			a, b, got, a)
	}
}

func TestMaxDuration_ZeroValues(t *testing.T) {
	got := maxDuration(0, 0)
	if got != 0 {
		t.Errorf("maxDuration(0, 0) = %v, want 0", got)
	}
}

func TestMaxDuration_OneZero(t *testing.T) {
	d := 5 * time.Second
	got := maxDuration(d, 0)
	if got != d {
		t.Errorf("maxDuration(%v, 0) = %v, want %v", d, got, d)
	}
	got = maxDuration(0, d)
	if got != d {
		t.Errorf("maxDuration(0, %v) = %v, want %v", d, got, d)
	}
}

func TestMaxDuration_Negative(t *testing.T) {
	a := -1 * time.Second
	b := -5 * time.Second
	got := maxDuration(a, b)
	if got != a {
		t.Errorf("maxDuration(%v, %v) = %v, want %v",
			a, b, got, a)
	}
}

func TestMaxDuration_LargeValues(t *testing.T) {
	a := 720 * time.Hour // 30 days
	b := 168 * time.Hour // 7 days
	got := maxDuration(a, b)
	if got != a {
		t.Errorf("maxDuration(%v, %v) = %v, want %v",
			a, b, got, a)
	}
}

// ---------------------------------------------------------------------------
// FormatDedupKey
// ---------------------------------------------------------------------------

func TestFormatDedupKey_Normal(t *testing.T) {
	got := FormatDedupKey("index_unused", "idx_users_email")
	want := "index_unused:idx_users_email"
	if got != want {
		t.Errorf("FormatDedupKey = %q, want %q", got, want)
	}
}

func TestFormatDedupKey_EmptyCategory(t *testing.T) {
	got := FormatDedupKey("", "idx_foo")
	want := ":idx_foo"
	if got != want {
		t.Errorf("FormatDedupKey = %q, want %q", got, want)
	}
}

func TestFormatDedupKey_EmptyObject(t *testing.T) {
	got := FormatDedupKey("index_unused", "")
	want := "index_unused:"
	if got != want {
		t.Errorf("FormatDedupKey = %q, want %q", got, want)
	}
}

func TestFormatDedupKey_BothEmpty(t *testing.T) {
	got := FormatDedupKey("", "")
	want := ":"
	if got != want {
		t.Errorf("FormatDedupKey = %q, want %q", got, want)
	}
}

func TestFormatDedupKey_SpecialCharacters(t *testing.T) {
	got := FormatDedupKey("seq:exhaustion", "public.users_id_seq")
	want := "seq:exhaustion:public.users_id_seq"
	if got != want {
		t.Errorf("FormatDedupKey = %q, want %q", got, want)
	}
}

func TestFormatDedupKey_Spaces(t *testing.T) {
	got := FormatDedupKey("my category", "my object")
	want := "my category:my object"
	if got != want {
		t.Errorf("FormatDedupKey = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// severityRank
// ---------------------------------------------------------------------------

func TestSeverityRank_Critical(t *testing.T) {
	got := severityRank("critical")
	if got != 0 {
		t.Errorf("severityRank(\"critical\") = %d, want 0", got)
	}
}

func TestSeverityRank_Warning(t *testing.T) {
	got := severityRank("warning")
	if got != 1 {
		t.Errorf("severityRank(\"warning\") = %d, want 1", got)
	}
}

func TestSeverityRank_Default(t *testing.T) {
	// "info" is not a named case, falls through to default.
	got := severityRank("info")
	if got != 2 {
		t.Errorf("severityRank(\"info\") = %d, want 2", got)
	}
}

func TestSeverityRank_Unknown(t *testing.T) {
	got := severityRank("unknown")
	if got != 2 {
		t.Errorf("severityRank(\"unknown\") = %d, want 2", got)
	}
}

func TestSeverityRank_Empty(t *testing.T) {
	got := severityRank("")
	if got != 2 {
		t.Errorf("severityRank(\"\") = %d, want 2", got)
	}
}

func TestSeverityRank_CaseSensitive(t *testing.T) {
	// "Critical" (uppercase C) should NOT match "critical".
	got := severityRank("Critical")
	if got != 2 {
		t.Errorf("severityRank(\"Critical\") = %d, want 2 "+
			"(case-sensitive, should not match)", got)
	}

	got = severityRank("WARNING")
	if got != 2 {
		t.Errorf("severityRank(\"WARNING\") = %d, want 2 "+
			"(case-sensitive, should not match)", got)
	}
}

func TestSeverityRank_Ordering(t *testing.T) {
	crit := severityRank("critical")
	warn := severityRank("warning")
	info := severityRank("info")

	if crit >= warn {
		t.Errorf("critical (%d) should rank lower than "+
			"warning (%d)", crit, warn)
	}
	if warn >= info {
		t.Errorf("warning (%d) should rank lower than "+
			"info (%d)", warn, info)
	}
}

// ---------------------------------------------------------------------------
// severityEmoji
// ---------------------------------------------------------------------------

func TestSeverityEmoji_Critical(t *testing.T) {
	got := severityEmoji("critical")
	want := "\xf0\x9f\x94\xb4" // red circle
	if got != want {
		t.Errorf("severityEmoji(\"critical\") = %q, want %q",
			got, want)
	}
}

func TestSeverityEmoji_Warning(t *testing.T) {
	got := severityEmoji("warning")
	want := "\xe2\x9a\xa0\xef\xb8\x8f" // warning sign
	if got != want {
		t.Errorf("severityEmoji(\"warning\") = %q, want %q",
			got, want)
	}
}

func TestSeverityEmoji_Info(t *testing.T) {
	got := severityEmoji("info")
	want := "\xe2\x84\xb9\xef\xb8\x8f" // info
	if got != want {
		t.Errorf("severityEmoji(\"info\") = %q, want %q",
			got, want)
	}
}

func TestSeverityEmoji_Unknown(t *testing.T) {
	// Unknown values should fall through to the default (info emoji).
	got := severityEmoji("banana")
	want := "\xe2\x84\xb9\xef\xb8\x8f"
	if got != want {
		t.Errorf("severityEmoji(\"banana\") = %q, want %q",
			got, want)
	}
}

func TestSeverityEmoji_Empty(t *testing.T) {
	got := severityEmoji("")
	want := "\xe2\x84\xb9\xef\xb8\x8f"
	if got != want {
		t.Errorf("severityEmoji(\"\") = %q, want %q",
			got, want)
	}
}

func TestSeverityEmoji_DistinctValues(t *testing.T) {
	crit := severityEmoji("critical")
	warn := severityEmoji("warning")
	info := severityEmoji("info")

	if crit == warn {
		t.Error("critical and warning emojis should differ")
	}
	if crit == info {
		t.Error("critical and info emojis should differ")
	}
	if warn == info {
		t.Error("warning and info emojis should differ")
	}
}

func TestSeverityEmoji_CaseSensitive(t *testing.T) {
	// "Critical" should not match "critical" switch case.
	got := severityEmoji("Critical")
	defaultEmoji := severityEmoji("anything")
	if got != defaultEmoji {
		t.Errorf("severityEmoji(\"Critical\") = %q, "+
			"expected default %q (case-sensitive)",
			got, defaultEmoji)
	}
}
