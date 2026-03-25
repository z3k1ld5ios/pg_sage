package optimizer

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckColdStart returns true if there are fewer than minSnapshots
// in sage.snapshots, indicating insufficient data for optimization.
func CheckColdStart(
	ctx context.Context,
	pool *pgxpool.Pool,
	minSnapshots int,
) (bool, error) {
	var count int
	err := pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM sage.snapshots",
	).Scan(&count)
	if err != nil {
		return true, err
	}
	return count < minSnapshots, nil
}
