package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// IndexInfo holds metadata about a PostgreSQL index.
type IndexInfo struct {
	SchemaName string
	TableName  string
	IndexName  string
	IndexDef   string
	Columns    []string
	IsUnique   bool
	IsPrimary  bool
}

// TableStats holds basic statistics for a table.
type TableStats struct {
	SchemaName  string
	TableName   string
	RowEstimate int64
	TotalSize   string
	IndexSize   string
}

// GetIndexes returns all user-defined indexes for the given schema.
// Note: excludes primary key indexes since those are managed by constraints.
// Note to self: if you want to include PKs for analysis, flip indisprimary = false to true
// or remove that filter entirely.
func GetIndexes(ctx context.Context, pool *pgxpool.Pool, schema string) ([]IndexInfo, error) {
	query := `
		SELECT
			n.nspname AS schema_name,
			t.relname AS table_name,
			i.relname AS index_name,
			pg_get_indexdef(ix.indexrelid) AS index_def,
			ix.indisunique AS is_unique,
			ix.indisprimary AS is_primary
		FROM
			pg_class t
			JOIN pg_index ix ON t.oid = ix.indrelid
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE
			t.relkind = 'r'
			AND n.nspname = $1
			AND n.nspname NOT IN ('pg_catalog', 'information_schema')
			AND ix.indisprimary = false
		ORDER BY t.relname, i.relname
	`

	rows, err := pool.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("querying indexes: %w", err)
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var idx IndexInfo
		if err := rows.Scan(
			&idx.SchemaName,
			&idx.TableName,
			&idx.IndexName,
			&idx.IndexDef,
			&idx.IsUnique,
			&idx.IsPrimary,
		); err != nil {
			return nil, fmt.Errorf("scanning index row: %w", err)
		}
		indexes = append(indexes, idx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating index rows: %w", err)
	}

	return indexes, nil
}

// GetTableStats returns size and row estimates for all tables in the given schema.
// Ordered by total relation size descending so the biggest tables show up first.
func GetTableStats(ctx context.Context, pool *pgxpool.Pool, schema string) ([]TableStats, error) {
	query := `
		SELECT
			n.nspname AS schema_name,
			c.relname AS table_name,
			c.reltuples::BIGINT AS row_estimate,
			pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
			pg_size_pretty(pg_indexes_size(c.oid)) AS index_size
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND n.nspname = $1
		ORDER BY pg_total_relation_size(c.oid) DESC
	`

	rows, err := pool.Query(ctx, query, schema)
	if err != nil {
		return nil, fmt.Errorf("querying table stats: %w", err)
	}
	defer rows.Close()

	var stats []TableStats
	for rows.Next() {
		var s TableStats
		if err := rows.Scan(&s.SchemaName, &s.TableName, &s.RowEstimate, &s.TotalSize, &s.IndexSize); err != nil {
			return nil, fmt.Errorf("scanning table stats row: %w", err)
		}
		stats = append(stats, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating table stats rows: %w", err)
	}

	return stats, nil
}
