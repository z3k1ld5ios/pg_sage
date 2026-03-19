-- pg_sage regression test suite
-- This tests individual components in isolation
-- Run with: psql -f test/regression.sql -v ON_ERROR_STOP=1

\set ON_ERROR_STOP on

DO $$
DECLARE
    pass_count integer := 0;
    fail_count integer := 0;
    total_count integer := 0;
    test_name text;
    test_result boolean;
BEGIN
    RAISE NOTICE '=== pg_sage Regression Tests ===';
    RAISE NOTICE '';

    -- ============================================================
    -- SCHEMA TESTS
    -- ============================================================

    -- T1: Extension installed
    test_name := 'Extension installed';
    total_count := total_count + 1;
    SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_sage') INTO test_result;
    IF test_result THEN
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    ELSE
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: %', test_name;
    END IF;

    -- T2: sage schema exists
    test_name := 'sage schema exists';
    total_count := total_count + 1;
    SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'sage') INTO test_result;
    IF test_result THEN
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    ELSE
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: %', test_name;
    END IF;

    -- T3: All required tables exist
    FOR test_name IN SELECT unnest(ARRAY['snapshots','findings','action_log','explain_cache','briefings','config'])
    LOOP
        total_count := total_count + 1;
        SELECT EXISTS(
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'sage' AND table_name = test_name
        ) INTO test_result;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: Table sage.% exists', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: Table sage.% missing', test_name;
        END IF;
    END LOOP;

    -- T4: All required indexes exist
    FOR test_name IN SELECT unnest(ARRAY[
        'idx_snapshots_time', 'idx_snapshots_category',
        'idx_findings_status', 'idx_findings_object',
        'idx_action_log_time', 'idx_explain_queryid'
    ])
    LOOP
        total_count := total_count + 1;
        SELECT EXISTS(
            SELECT 1 FROM pg_indexes WHERE indexname = test_name
        ) INTO test_result;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: Index % exists', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: Index % missing', test_name;
        END IF;
    END LOOP;

    -- ============================================================
    -- FUNCTION TESTS
    -- ============================================================

    -- T5: sage.status() returns valid JSONB
    test_name := 'sage.status() returns JSONB';
    total_count := total_count + 1;
    BEGIN
        PERFORM sage.status();
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- T6: sage.status() has required keys
    test_name := 'sage.status() has version key';
    total_count := total_count + 1;
    BEGIN
        SELECT (sage.status() ? 'version') INTO test_result;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: %', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: %', test_name;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- T7: sage.emergency_stop() works
    test_name := 'sage.emergency_stop()';
    total_count := total_count + 1;
    BEGIN
        PERFORM sage.emergency_stop();
        SELECT (sage.status()->>'emergency_stopped')::boolean INTO test_result;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: %', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: %', test_name;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- T8: sage.resume() works
    test_name := 'sage.resume()';
    total_count := total_count + 1;
    BEGIN
        PERFORM sage.resume();
        SELECT NOT (sage.status()->>'emergency_stopped')::boolean INTO test_result;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: %', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: %', test_name;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- T9: sage.briefing() returns text
    test_name := 'sage.briefing() returns text';
    total_count := total_count + 1;
    BEGIN
        SELECT length(sage.briefing()) > 0 INTO test_result;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: %', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: % — empty result', test_name;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- T10: sage.diagnose() returns text
    test_name := 'sage.diagnose() returns text';
    total_count := total_count + 1;
    BEGIN
        SELECT length(sage.diagnose('show me slow queries')) > 0 INTO test_result;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: %', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: % — empty result', test_name;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- ============================================================
    -- GUC TESTS
    -- ============================================================

    -- T11: GUCs are registered
    test_name := 'GUC sage.enabled exists';
    total_count := total_count + 1;
    SELECT EXISTS(SELECT 1 FROM pg_settings WHERE name = 'sage.enabled') INTO test_result;
    IF test_result THEN
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    ELSE
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: %', test_name;
    END IF;

    test_name := 'GUC sage.collector_interval exists';
    total_count := total_count + 1;
    SELECT EXISTS(SELECT 1 FROM pg_settings WHERE name = 'sage.collector_interval') INTO test_result;
    IF test_result THEN
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    ELSE
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: %', test_name;
    END IF;

    test_name := 'GUC sage.trust_level exists';
    total_count := total_count + 1;
    SELECT EXISTS(SELECT 1 FROM pg_settings WHERE name = 'sage.trust_level') INTO test_result;
    IF test_result THEN
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    ELSE
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: %', test_name;
    END IF;

    -- ============================================================
    -- DATA COLLECTION TESTS (require waiting)
    -- ============================================================

    -- T12: Config table has defaults
    test_name := 'Config table has defaults';
    total_count := total_count + 1;
    SELECT count(*) > 0 INTO test_result FROM sage.config;
    IF test_result THEN
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    ELSE
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: %', test_name;
    END IF;

    -- ============================================================
    -- FINDINGS ENGINE TESTS
    -- ============================================================

    -- T13: Insert a test finding
    test_name := 'Insert test finding';
    total_count := total_count + 1;
    BEGIN
        INSERT INTO sage.findings (category, severity, object_type, object_identifier, title, detail)
        VALUES ('test', 'info', 'table', 'test.test_table', 'Test finding',
                '{"test": true}'::jsonb);
        pass_count := pass_count + 1;
        RAISE NOTICE 'PASS: %', test_name;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- T14: Suppress the test finding
    test_name := 'Suppress test finding';
    total_count := total_count + 1;
    BEGIN
        PERFORM sage.suppress(
            (SELECT id FROM sage.findings WHERE category = 'test' AND status = 'open' LIMIT 1),
            'test suppression',
            7
        );
        SELECT status = 'suppressed' INTO test_result
        FROM sage.findings WHERE category = 'test' LIMIT 1;
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: %', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: %', test_name;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- T15: Dedup key constraint
    test_name := 'Finding dedup constraint';
    total_count := total_count + 1;
    BEGIN
        -- Insert a finding with same dedup key (but different status)
        INSERT INTO sage.findings (category, severity, object_type, object_identifier, title, detail, status)
        VALUES ('test', 'info', 'table', 'test.dedup_table', 'Dedup test 1', '{"n": 1}'::jsonb, 'open');
        INSERT INTO sage.findings (category, severity, object_type, object_identifier, title, detail, status)
        VALUES ('test', 'info', 'table', 'test.dedup_table', 'Dedup test 2', '{"n": 2}'::jsonb, 'resolved');
        -- Both should exist since status differs
        SELECT count(*) = 2 INTO test_result
        FROM sage.findings WHERE category = 'test' AND object_identifier = 'test.dedup_table';
        IF test_result THEN
            pass_count := pass_count + 1;
            RAISE NOTICE 'PASS: %', test_name;
        ELSE
            fail_count := fail_count + 1;
            RAISE WARNING 'FAIL: %', test_name;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        fail_count := fail_count + 1;
        RAISE WARNING 'FAIL: % — %', test_name, SQLERRM;
    END;

    -- Cleanup test data
    DELETE FROM sage.findings WHERE category = 'test';

    -- ============================================================
    -- SUMMARY
    -- ============================================================
    RAISE NOTICE '';
    RAISE NOTICE '=== Results: % passed, % failed, % total ===',
        pass_count, fail_count, total_count;

    IF fail_count > 0 THEN
        RAISE EXCEPTION '% test(s) failed', fail_count;
    END IF;
END $$;
