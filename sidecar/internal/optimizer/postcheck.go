package optimizer

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckIndexValid verifies that a newly created index is valid
// by checking pg_index.indisvalid. Returns false if the index
// is invalid (e.g., CONCURRENTLY build failed).
func CheckIndexValid(
	ctx context.Context,
	pool *pgxpool.Pool,
	indexName string,
) (bool, error) {
	var valid bool
	err := pool.QueryRow(ctx,
		`SELECT i.indisvalid
		 FROM pg_index i
		 JOIN pg_class c ON c.oid = i.indexrelid
		 WHERE c.relname = $1`,
		indexName,
	).Scan(&valid)
	if err != nil {
		return false, err
	}
	return valid, nil
}
