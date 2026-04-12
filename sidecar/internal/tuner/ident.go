package tuner

import "strings"

// QuoteIdentifier wraps a PostgreSQL identifier in double quotes,
// doubling any embedded double quote. Safe for use in ANALYZE or
// other DDL where identifiers must be injection-free.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteQualified returns a quoted "schema"."table" identifier.
// Empty schema defaults to "public".
func QuoteQualified(schema, table string) string {
	if schema == "" {
		schema = "public"
	}
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
}

// SplitCanonical splits a canonical "schema.table" string into
// its parts. Unqualified input defaults to schema="public".
func SplitCanonical(canonical string) (schema, table string) {
	canonical = strings.TrimSpace(canonical)
	if idx := strings.Index(canonical, "."); idx >= 0 {
		return canonical[:idx], canonical[idx+1:]
	}
	return "public", canonical
}
