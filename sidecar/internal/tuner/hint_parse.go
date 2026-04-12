package tuner

import (
	"regexp"
	"strings"
)

// HintDirective is one parsed pg_hint_plan directive extracted
// from a query_hints.hint_text blob. A single hint_text may
// contain many directives; ParseHintText returns them all.
//
// Kind is the directive head (HashJoin, IndexScan, Set, etc.).
// Aliases is the alias list (may be empty). IndexNames is the
// tail of IndexScan/BitmapScan directives (may be empty). For
// Set(param "value"), ParamName and ParamValue are populated
// instead. RowsDirective captures the raw suffix (`#N` / `+N`)
// for Rows(). Raw is the literal directive substring.
type HintDirective struct {
	Kind          string
	Aliases       []string
	IndexNames    []string
	ParamName     string
	ParamValue    string
	RowsDirective string
	Raw           string
}

// Known directive heads supported by v0.8.5 revalidation Check 1.
var knownHintKinds = map[string]struct{}{
	"HashJoin":    {},
	"NestLoop":    {},
	"MergeJoin":   {},
	"IndexScan":   {},
	"NoIndexScan": {},
	"BitmapScan":  {},
	"SeqScan":     {},
	"NoSeqScan":   {},
	"Leading":     {},
	"Set":         {},
	"Rows":        {},
}

// hintDirectiveRe captures a single directive head + parenthesised
// body. It is intentionally permissive on body contents because the
// per-directive parsers below re-validate their own arguments. The
// body capture must match balanced parentheses at one level — we do
// not support nested directives because pg_hint_plan itself does
// not nest (Leading's inner parens are handled as literal text).
var hintDirectiveRe = regexp.MustCompile(
	`([A-Za-z]+)\(([^()]*(?:\([^()]*\)[^()]*)*)\)`,
)

// ParseHintText splits a hint_text blob into individual
// directives. Unknown directive heads are surfaced via the
// second return value (unparseable) so callers can log them
// instead of silently dropping them.
func ParseHintText(text string) (parsed []HintDirective, unparseable []string) {
	matches := hintDirectiveRe.FindAllStringSubmatchIndex(text, -1)
	if matches == nil {
		trimmed := strings.TrimSpace(text)
		if trimmed != "" {
			unparseable = append(unparseable, trimmed)
		}
		return parsed, unparseable
	}
	for _, m := range matches {
		raw := text[m[0]:m[1]]
		head := text[m[2]:m[3]]
		body := text[m[4]:m[5]]
		if _, ok := knownHintKinds[head]; !ok {
			unparseable = append(unparseable, raw)
			continue
		}
		d := HintDirective{Kind: head, Raw: raw}
		switch head {
		case "Set":
			d.ParamName, d.ParamValue = parseSetBody(body)
			if d.ParamName == "" {
				unparseable = append(unparseable, raw)
				continue
			}
		case "Rows":
			parts := splitArgs(body)
			if len(parts) < 2 {
				unparseable = append(unparseable, raw)
				continue
			}
			last := parts[len(parts)-1]
			if !strings.HasPrefix(last, "#") && !strings.HasPrefix(last, "+") &&
				!strings.HasPrefix(last, "-") && !strings.HasPrefix(last, "*") {
				unparseable = append(unparseable, raw)
				continue
			}
			d.RowsDirective = last
			d.Aliases = parts[:len(parts)-1]
		case "IndexScan", "BitmapScan":
			parts := splitArgs(body)
			if len(parts) == 0 {
				unparseable = append(unparseable, raw)
				continue
			}
			d.Aliases = parts[:1]
			d.IndexNames = parts[1:]
		case "Leading":
			// Leading uses nested parens: Leading((t1 t2 t3)).
			// Strip one layer and split.
			trimmed := strings.TrimSpace(body)
			trimmed = strings.TrimPrefix(trimmed, "(")
			trimmed = strings.TrimSuffix(trimmed, ")")
			d.Aliases = splitArgs(trimmed)
		default:
			// HashJoin / NestLoop / MergeJoin / SeqScan /
			// NoSeqScan / NoIndexScan — all alias-only.
			d.Aliases = splitArgs(body)
		}
		parsed = append(parsed, d)
	}
	return parsed, unparseable
}

// parseSetBody extracts (param, value) from a Set directive body
// like `work_mem "256MB"`. Returns empty strings on malformed.
func parseSetBody(body string) (string, string) {
	body = strings.TrimSpace(body)
	if body == "" {
		return "", ""
	}
	// Split into [param, rest] on the first whitespace.
	fields := strings.Fields(body)
	if len(fields) < 2 {
		return "", ""
	}
	param := fields[0]
	rest := strings.Join(fields[1:], " ")
	rest = strings.TrimSpace(rest)
	// Strip surrounding quotes if present.
	if len(rest) >= 2 &&
		((rest[0] == '"' && rest[len(rest)-1] == '"') ||
			(rest[0] == '\'' && rest[len(rest)-1] == '\'')) {
		rest = rest[1 : len(rest)-1]
	}
	return param, rest
}

// splitArgs tokenises a directive body on whitespace, preserving
// tokens unchanged. Quoted identifiers and nested parens are left
// literal because v0.8.5 callers only inspect a small whitelist.
func splitArgs(body string) []string {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}
	return strings.Fields(body)
}

// HasUnparseable returns true if the hint text contained at least
// one directive the parser did not recognise. Used by revalidation
// Check 1 to log a hint_unparseable finding while continuing the
// cycle for the other checks.
func HasUnparseable(unparseable []string) bool {
	return len(unparseable) > 0
}
