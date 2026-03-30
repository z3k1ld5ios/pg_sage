#!/bin/bash
# Bloat generator - creates dead tuples for table_bloat detection
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
psql "$PG_URL" -c "UPDATE bloat_target SET payload = repeat('z', 200) WHERE id <= 50000;"
echo "Bloat workload complete (50k rows updated, dead tuples created)"
