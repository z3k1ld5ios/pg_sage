package executor

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// VerifyGrants checks that the connected PostgreSQL user has the
// privileges required for autonomous execution. It logs warnings
// with fix SQL for any missing grants.
func VerifyGrants(
	ctx context.Context,
	pool *pgxpool.Pool,
	user string,
	logFn func(string, string, ...any),
) {
	// Resolve actual connected user (handles DATABASE_URL override).
	var actual string
	if err := pool.QueryRow(ctx, "SELECT current_user").Scan(&actual); err == nil {
		user = actual
	}
	checkSchemaCreate(ctx, pool, user, logFn)
	checkSignalBackend(ctx, pool, user, logFn)
}

func checkSchemaCreate(
	ctx context.Context,
	pool *pgxpool.Pool,
	user string,
	logFn func(string, string, ...any),
) {
	var hasCreate bool
	err := pool.QueryRow(ctx,
		"SELECT has_schema_privilege($1, 'public', 'CREATE')",
		user,
	).Scan(&hasCreate)
	if err != nil {
		logFn("grants",
			"could not check CREATE privilege on public schema: %v", err,
		)
		return
	}
	if !hasCreate {
		logFn("grants",
			"WARNING: user %q lacks CREATE on public schema; "+
				"fix with: GRANT CREATE ON SCHEMA public TO %s",
			user, user,
		)
	}
}

func checkSignalBackend(
	ctx context.Context,
	pool *pgxpool.Pool,
	user string,
	logFn func(string, string, ...any),
) {
	var hasMembership bool
	err := pool.QueryRow(ctx,
		"SELECT pg_has_role($1, 'pg_signal_backend', 'MEMBER')",
		user,
	).Scan(&hasMembership)
	if err != nil {
		logFn("grants",
			"could not check pg_signal_backend membership: %v", err,
		)
		return
	}
	if !hasMembership {
		logFn("grants",
			"WARNING: user %q is not a member of pg_signal_backend; "+
				"fix with: GRANT pg_signal_backend TO %s",
			user, user,
		)
	}
}
