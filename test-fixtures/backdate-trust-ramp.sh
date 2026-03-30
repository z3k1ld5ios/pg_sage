#!/bin/bash
# Backdate trust ramp start for executor testing
PG_URL="${PG_URL:-postgres://postgres:test@localhost:5433/testdb}"
DAYS="${1:-35}"
psql "$PG_URL" -c "
  INSERT INTO sage.config (key, value)
  VALUES ('trust_ramp_start', (now() - interval '${DAYS} days')::text)
  ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
"
echo "Trust ramp backdated to $DAYS days ago"
