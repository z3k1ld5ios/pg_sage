-- pg_sage integration tests
-- Run with: psql -f test/run_tests.sql

\set ON_ERROR_STOP on
\timing on

\echo '=== pg_sage Integration Tests ==='
\echo ''

-- Test 1: Extension is loaded
\echo 'TEST 1: Extension loaded'
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_sage';

-- Test 2: Schema exists
\echo 'TEST 2: Schema exists'
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sage';

-- Test 3: All tables exist
\echo 'TEST 3: All tables exist'
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'sage'
ORDER BY table_name;

-- Test 4: sage.status() works
\echo 'TEST 4: sage.status()'
SELECT sage.status();

-- Test 5: sage.briefing() works (should return structured output without LLM)
\echo 'TEST 5: sage.briefing()'
SELECT length(sage.briefing()) > 0 AS briefing_not_empty;

-- Test 6: sage.diagnose() works without LLM
\echo 'TEST 6: sage.diagnose() without LLM'
SELECT length(sage.diagnose('why is the database slow?')) > 0 AS diagnose_not_empty;

-- Test 7: sage.explain() works
\echo 'TEST 7: sage.explain()'
-- First generate some queries to appear in pg_stat_statements
SELECT count(*) FROM orders WHERE customer_id = 1;
SELECT count(*) FROM orders WHERE customer_id = 1;
SELECT count(*) FROM orders WHERE customer_id = 1;

-- Get a queryid and try to explain it
DO $$
DECLARE
    qid bigint;
    result text;
BEGIN
    SELECT queryid INTO qid FROM pg_stat_statements
    WHERE query LIKE '%orders%customer_id%' LIMIT 1;

    IF qid IS NOT NULL THEN
        result := sage.explain(qid);
        RAISE NOTICE 'EXPLAIN result length: %', length(result);
    ELSE
        RAISE NOTICE 'No matching queryid found (pg_stat_statements may need more data)';
    END IF;
END $$;

-- Test 8: Emergency stop / resume
\echo 'TEST 8: Emergency stop and resume'
SELECT sage.emergency_stop();
SELECT (sage.status()->>'emergency_stopped')::boolean AS stopped;
SELECT sage.resume();
SELECT (sage.status()->>'emergency_stopped')::boolean AS stopped;

-- Test 9: Suppress a finding (if any exist)
\echo 'TEST 9: Finding suppression'
DO $$
DECLARE
    fid bigint;
BEGIN
    SELECT id INTO fid FROM sage.findings WHERE status = 'open' LIMIT 1;
    IF fid IS NOT NULL THEN
        PERFORM sage.suppress(fid, 'test suppression', 7);
        RAISE NOTICE 'Suppressed finding %', fid;
    ELSE
        RAISE NOTICE 'No open findings to suppress yet';
    END IF;
END $$;

-- Test 10: Check snapshots are being collected
\echo 'TEST 10: Snapshot collection'
-- Wait a moment for collector to run
SELECT pg_sleep(5);
SELECT category, count(*) FROM sage.snapshots GROUP BY category ORDER BY category;

-- Test 11: Config table has defaults
\echo 'TEST 11: Config defaults'
SELECT key, value FROM sage.config ORDER BY key;

-- Test 12: Check GUC settings are visible
\echo 'TEST 12: GUC settings'
SHOW sage.enabled;
SHOW sage.collector_interval;
SHOW sage.slow_query_threshold;
SHOW sage.trust_level;

-- Test 13: Verify duplicate index detection (wait for analyzer)
\echo 'TEST 13: Checking for findings (may need to wait for analyzer cycle)'
SELECT pg_sleep(10);
SELECT id, category, severity, title, object_identifier
FROM sage.findings
WHERE status = 'open'
ORDER BY severity DESC, category
LIMIT 20;

-- Test 14: Self-monitoring snapshot
\echo 'TEST 14: Self-monitoring'
SELECT data FROM sage.snapshots
WHERE category = 'sage_health'
ORDER BY collected_at DESC
LIMIT 1;

\echo ''
\echo '=== Tests Complete ==='
