#!/bin/bash
# Seed historical snapshots for forecaster testing
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
psql "$PG_URL" -c "
INSERT INTO sage.snapshots (database_name, collected_at, data)
SELECT 'testdb',
       now() - (i * interval '1 hour'),
       jsonb_build_object(
           'disk_size_bytes', 1000000000 + (i * 5000000),
           'active_connections', 20 + (i % 10),
           'cache_hit_ratio', 0.98 - (i * 0.0001),
           'total_query_calls', 100000 + (i * 500)
       )
FROM generate_series(1, 48) AS i;
"
echo "Seeded 48 hours of historical snapshot data"
