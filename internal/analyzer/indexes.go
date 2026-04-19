// Package analyzer provides analysis of PostgreSQL database objects
// to identify potential performance issues and optimization opportunities.
package analyzer

import (
	"context"
	"fmt"
	"strings"

	"github.com/jasonmassie01/pg_sage/internal/db"
)

// IndexIssueType categorizes the kind of index problem detected.
type IndexIssueType string

const (
	IssueDuplicateIndex  IndexIssueType = "duplicate_index"
	IssueUnusedIndex     IndexIssueType = "unused_index"
	IssueRedundantIndex  IndexIssueType = "redundant_index"
)

// IndexIssue represents a detected problem with one or more indexes.
type IndexIssue struct {
	Type        IndexIssueType
	Severity    string
	TableName   string
	IndexNames  []string
	Description string
	Suggestion  string
}

// AnalyzeIndexes inspects the provided indexes and returns a list of issues.
func AnalyzeIndexes(indexes []db.Index) []IndexIssue {
	var issues []IndexIssue

	issues = append(issues, detectDuplicateIndexes(indexes)...)
	issues = append(issues, detectUnusedIndexes(indexes)...)

	return issues
}

// detectDuplicateIndexes finds indexes that cover the exact same columns
// on the same table, making one of them redundant.
func detectDuplicateIndexes(indexes []db.Index) []IndexIssue {
	type key struct {
		table   string
		columns string
	}

	seen := make(map[key][]string)

	for _, idx := range indexes {
		normalized := normalizeColumns(idx.Columns)
		k := key{table: idx.TableName, columns: normalized}
		seen[k] = append(seen[k], idx.IndexName)
	}

	var issues []IndexIssue
	for k, names := range seen {
		if len(names) < 2 {
			continue
		}
		issues = append(issues, IndexIssue{
			Type:       IssueDuplicateIndex,
			Severity:   "high",
			TableName:  k.table,
			IndexNames: names,
			Description: fmt.Sprintf(
				"Table %q has %d indexes covering the same columns (%s): %s",
				k.table, len(names), k.columns, strings.Join(names, ", "),
			),
			Suggestion: fmt.Sprintf("Drop all but one of: %s", strings.Join(names, ", ")),
		})
	}

	return issues
}

// detectUnusedIndexes flags indexes that have never been scanned.
// These waste space and slow down writes without benefiting reads.
func detectUnusedIndexes(indexes []db.Index) []IndexIssue {
	var issues []IndexIssue

	for _, idx := range indexes {
		// Primary keys and unique constraints should never be flagged as unused.
		if idx.IsPrimary || idx.IsUnique {
			continue
		}
		if idx.IndexScans == 0 {
			issues = append(issues, IndexIssue{
				Type:       IssueUnusedIndex,
				Severity:   "medium",
				TableName:  idx.TableName,
				IndexNames: []string{idx.IndexName},
				Description: fmt.Sprintf(
					"Index %q on table %q has never been used (0 scans since last stats reset)",
					idx.IndexName, idx.TableName,
				),
				Suggestion: fmt.Sprintf("Consider dropping index %q if the workload is representative", idx.IndexName),
			})
		}
	}

	return issues
}

// RunIndexAnalysis fetches index data from the database and returns detected issues.
func RunIndexAnalysis(ctx context.Context, pool db.Querier) ([]IndexIssue, error) {
	indexes, err := db.GetIndexes(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("fetching indexes: %w", err)
	}
	return AnalyzeIndexes(indexes), nil
}

// normalizeColumns sorts and trims column lists so that (a, b) and (b, a)
// are treated as equivalent for duplicate detection purposes.
func normalizeColumns(columns []string) string {
	copy_ := make([]string, len(columns))
	copy(copy_, columns)
	// simple alphabetical sort without importing sort for brevity
	for i := 0; i < len(copy_); i++ {
		for j := i + 1; j < len(copy_); j++ {
			if copy_[i] > copy_[j] {
				copy_[i], copy_[j] = copy_[j], copy_[i]
			}
		}
	}
	return strings.Join(copy_, ",")
}
