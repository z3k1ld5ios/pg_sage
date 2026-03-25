package optimizer

import (
	"regexp"
	"sort"
	"strings"
)

var (
	reWhitespace  = regexp.MustCompile(`\s+`)
	reInList      = regexp.MustCompile(`(?i)in\s*\([^)]+\)`)
	reNumericLit  = regexp.MustCompile(`\b\d+\.?\d*\b`)
	reParamMarker = regexp.MustCompile(`\$\d+`)
)

// FingerprintQuery normalizes a SQL query to a canonical form.
// Collapses IN-lists, normalizes whitespace, replaces numeric
// literals (but not $N params), and lowercases the result.
func FingerprintQuery(query string) string {
	s := query

	// Step 1: protect $N parameters by replacing with placeholders
	params := reParamMarker.FindAllString(s, -1)
	for i, p := range params {
		s = strings.Replace(s, p, placeholderToken(i), 1)
	}

	// Step 2: replace numeric literals with ?
	s = reNumericLit.ReplaceAllString(s, "?")

	// Step 3: restore $N parameters
	for i, p := range params {
		s = strings.Replace(s, placeholderToken(i), p, 1)
	}

	// Step 4: collapse IN-lists
	s = reInList.ReplaceAllString(s, "IN (...)")

	// Step 5: normalize whitespace
	s = reWhitespace.ReplaceAllString(s, " ")

	// Step 6: trim and lowercase
	return strings.ToLower(strings.TrimSpace(s))
}

func placeholderToken(i int) string {
	return "\x00PARAM" + string(rune('A'+i)) + "\x00"
}

// GroupByFingerprint groups queries by fingerprint and returns
// aggregated stats. The representative query (highest calls)
// is kept; Calls and TotalTimeMs are summed across the group.
func GroupByFingerprint(queries []QueryInfo) []QueryInfo {
	type group struct {
		rep         QueryInfo
		totalCalls  int64
		totalTimeMs float64
	}

	groups := make(map[string]*group)
	var order []string

	for _, q := range queries {
		fp := FingerprintQuery(q.Text)
		g, exists := groups[fp]
		if !exists {
			g = &group{rep: q}
			groups[fp] = g
			order = append(order, fp)
		}
		g.totalCalls += q.Calls
		g.totalTimeMs += q.TotalTimeMs
		if q.Calls > g.rep.Calls {
			g.rep = q
		}
	}

	result := make([]QueryInfo, 0, len(groups))
	for _, fp := range order {
		g := groups[fp]
		result = append(result, QueryInfo{
			QueryID:     g.rep.QueryID,
			Text:        g.rep.Text,
			Calls:       g.totalCalls,
			TotalTimeMs: g.totalTimeMs,
			MeanTimeMs:  g.totalTimeMs / float64(g.totalCalls),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TotalTimeMs > result[j].TotalTimeMs
	})

	return result
}
