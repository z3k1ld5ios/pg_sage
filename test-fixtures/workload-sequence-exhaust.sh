#!/bin/bash
# Sequence exhaustion - advances almost_done_seq
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
psql "$PG_URL" -c "SELECT nextval('almost_done_seq') FROM generate_series(1, 30);"
echo "Sequence advanced by 30 values"
