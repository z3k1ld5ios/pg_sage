/*
 * self_monitor.c — pg_sage watches itself
 *
 * Collects schema size, collector/analyzer performance, circuit breaker
 * health, and finding volume. Generates findings against itself when
 * thresholds are exceeded.
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include <string.h>
#include "access/xact.h"
#include "utils/snapmgr.h"

/* ----------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------- */

/*
 * Parse a human-readable size string like "1GB", "512MB", "100KB" to bytes.
 * Returns 0 on parse failure.
 */
static int64
sage_parse_size_bytes(const char *size_str)
{
    double  val;
    char    unit[8];
    int     n;

    if (size_str == NULL || size_str[0] == '\0')
        return 0;

    memset(unit, 0, sizeof(unit));
    n = sscanf(size_str, "%lf%7s", &val, unit);

    if (n < 1 || val < 0)
        return 0;

    if (n == 1)
        return (int64)val;

    if (pg_strcasecmp(unit, "KB") == 0 || pg_strcasecmp(unit, "K") == 0)
        return (int64)(val * 1024);
    if (pg_strcasecmp(unit, "MB") == 0 || pg_strcasecmp(unit, "M") == 0)
        return (int64)(val * 1024 * 1024);
    if (pg_strcasecmp(unit, "GB") == 0 || pg_strcasecmp(unit, "G") == 0)
        return (int64)(val * 1024 * 1024 * 1024);
    if (pg_strcasecmp(unit, "TB") == 0 || pg_strcasecmp(unit, "T") == 0)
        return (int64)(val * 1024LL * 1024 * 1024 * 1024);

    /* Unknown unit, treat as raw bytes */
    return (int64)val;
}

/*
 * Get the circuit state name as a string.
 */
static const char *
sage_circuit_state_name(SageCircuitState state)
{
    switch (state)
    {
        case SAGE_CIRCUIT_CLOSED:   return "closed";
        case SAGE_CIRCUIT_OPEN:     return "open";
        case SAGE_CIRCUIT_DORMANT:  return "dormant";
        default:                    return "unknown";
    }
}

/*
 * Get the trust level name as a string.
 */
static const char *
sage_trust_level_name(SageTrustLevel level)
{
    switch (level)
    {
        case SAGE_TRUST_OBSERVATION:    return "observation";
        case SAGE_TRUST_ADVISORY:       return "advisory";
        case SAGE_TRUST_AUTONOMOUS:     return "autonomous";
        default:                        return "unknown";
    }
}

/* ----------------------------------------------------------------
 * sage_self_monitor_collect
 *
 * The main self-monitoring collection function. Called periodically
 * by the collector worker to assess pg_sage's own health.
 * ---------------------------------------------------------------- */
void
sage_self_monitor_collect(void)
{
    int64               schema_bytes = 0;
    double              collector_ms = 0.0;
    double              analyzer_ms  = 0.0;
    int                 skips = 0;
    bool                stopped = false;
    bool                in_recovery = false;
    int                 tokens_today = 0;
    SageCircuitState    circuit;
    SageTrustLevel      trust;
    int                 trust_day;
    int                 open_findings = 0;
    int64               max_schema_bytes;
    int                 ret;
    StringInfoData      json_buf;
    volatile bool       err;

    if (!sage_state)
        return;

    /* ----------------------------------------------------------
     * Step 1: Collect schema size
     * ---------------------------------------------------------- */
    err = false;
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_connect();

    PG_TRY();
    {
        ret = SPI_execute(
            "SELECT pg_total_relation_size('sage.snapshots') + "
            "       pg_total_relation_size('sage.findings') + "
            "       pg_total_relation_size('sage.action_log') + "
            "       pg_total_relation_size('sage.explain_cache') + "
            "       pg_total_relation_size('sage.briefings') + "
            "       pg_total_relation_size('sage.config') AS total_bytes",
            true, 0);

        if (ret == SPI_OK_SELECT && SPI_processed > 0 && !sage_spi_isnull(0, 0))
            schema_bytes = sage_spi_getval_int64(0, 0);
    }
    PG_CATCH();
    {
        FlushErrorState();
        err = true;
        schema_bytes = 0;
    }
    PG_END_TRY();

    SPI_finish();
    if (err)
        AbortCurrentTransaction();
    else
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    /* Store schema size in shared state */
    LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);
    sage_state->sage_schema_bytes = schema_bytes;
    LWLockRelease(sage_state->lock);

    /* ----------------------------------------------------------
     * Step 2: Read shared state snapshot
     * ---------------------------------------------------------- */
    LWLockAcquire(sage_state->lock, LW_SHARED);
    collector_ms = sage_state->last_collector_duration_ms;
    analyzer_ms  = sage_state->last_analyzer_duration_ms;
    skips        = sage_state->consecutive_skips;
    stopped      = sage_state->emergency_stopped;
    in_recovery  = sage_state->is_in_recovery;
    tokens_today = sage_state->llm_tokens_used_today;
    circuit      = sage_state->circuit_state;
    LWLockRelease(sage_state->lock);

    trust     = sage_get_trust_level();
    trust_day = sage_get_trust_day();

    /* ----------------------------------------------------------
     * Step 3: Check schema size against max
     * Step 4: Check collector and analyzer durations
     * Step 5: Check circuit breaker engagement
     *
     * All upsert/resolve calls need an active transaction + SPI.
     * ---------------------------------------------------------- */
    max_schema_bytes = sage_parse_size_bytes(sage_max_schema_size);

    err = false;
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_connect();

    PG_TRY();
    {
        /* Step 3: Schema size */
        if (max_schema_bytes > 0 && schema_bytes > max_schema_bytes)
        {
            char    detail[256];

            snprintf(detail, sizeof(detail),
                     "{\"schema_bytes\": %lld, \"max_bytes\": %lld}",
                     (long long)schema_bytes, (long long)max_schema_bytes);

            sage_upsert_finding(
                "sage_health", "warning", "schema", "sage.schema_size",
                "pg_sage schema size exceeds configured maximum",
                detail,
                "Run sage.run_retention_cleanup() or reduce retention settings",
                "SELECT sage.run_retention_cleanup()",
                NULL);
        }

        /* Step 4: Collector duration */
        if (collector_ms > 5000.0)
        {
            char    detail[256];

            snprintf(detail, sizeof(detail),
                     "{\"collector_duration_ms\": %.1f, \"threshold_ms\": 5000}",
                     collector_ms);

            sage_upsert_finding(
                "sage_health", "warning", "worker", "sage.collector_duration",
                "pg_sage collector cycle is taking too long",
                detail,
                "Check for table bloat in sage schema or reduce collector_batch_size",
                NULL, NULL);
        }

        /* Step 4: Analyzer duration */
        if (analyzer_ms > 30000.0)
        {
            char    detail[256];

            snprintf(detail, sizeof(detail),
                     "{\"analyzer_duration_ms\": %.1f, \"threshold_ms\": 30000}",
                     analyzer_ms);

            sage_upsert_finding(
                "sage_health", "warning", "worker", "sage.analyzer_duration",
                "pg_sage analyzer cycle is taking too long",
                detail,
                "Review analyzer rules and snapshot volume",
                NULL, NULL);
        }

        /* Step 5: Circuit breaker */
        if (skips > 0)
        {
            char    detail[256];

            snprintf(detail, sizeof(detail),
                     "{\"consecutive_skips\": %d, \"circuit_state\": \"%s\"}",
                     skips, sage_circuit_state_name(circuit));

            sage_upsert_finding(
                "sage_health", "info", "circuit_breaker", "sage.circuit_breaker",
                "pg_sage circuit breaker is engaged",
                detail,
                "Investigate system load or disk pressure causing skips",
                NULL, NULL);
        }
        else
        {
            sage_resolve_finding("sage_health", "sage.circuit_breaker");
        }
    }
    PG_CATCH();
    {
        FlushErrorState();
        err = true;
        elog(WARNING, "pg_sage: error in self-monitor findings (steps 3-5)");
    }
    PG_END_TRY();

    SPI_finish();
    if (err)
        AbortCurrentTransaction();
    else
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    /* ----------------------------------------------------------
     * Step 6: Check finding volume
     * ---------------------------------------------------------- */
    err = false;
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_connect();

    PG_TRY();
    {
        ret = SPI_execute(
            "SELECT count(*) FROM sage.findings WHERE status = 'open'",
            true, 0);

        if (ret == SPI_OK_SELECT && SPI_processed > 0 && !sage_spi_isnull(0, 0))
            open_findings = (int)sage_spi_getval_int64(0, 0);

        if (open_findings > 100)
        {
            char    detail[256];

            snprintf(detail, sizeof(detail),
                     "{\"open_findings_count\": %d}", open_findings);

            sage_upsert_finding(
                "sage_health", "warning", "findings", "sage.findings_flood",
                "pg_sage has an unusually high number of open findings",
                detail,
                "Review and suppress or resolve low-priority findings",
                NULL, NULL);
        }
        else
        {
            sage_resolve_finding("sage_health", "sage.findings_flood");
        }
    }
    PG_CATCH();
    {
        FlushErrorState();
        err = true;
        open_findings = 0;
    }
    PG_END_TRY();

    SPI_finish();
    if (err)
        AbortCurrentTransaction();
    else
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    /* ----------------------------------------------------------
     * Step 7: Generate self-health snapshot
     * ---------------------------------------------------------- */
    initStringInfo(&json_buf);
    appendStringInfo(&json_buf,
        "{"
        "\"schema_bytes\": %lld, "
        "\"collector_duration_ms\": %.1f, "
        "\"analyzer_duration_ms\": %.1f, "
        "\"circuit_state\": \"%s\", "
        "\"consecutive_skips\": %d, "
        "\"trust_level\": \"%s\", "
        "\"trust_day\": %d, "
        "\"is_in_recovery\": %s, "
        "\"emergency_stopped\": %s, "
        "\"open_findings_count\": %d, "
        "\"llm_tokens_today\": %d"
        "}",
        (long long)schema_bytes,
        collector_ms,
        analyzer_ms,
        sage_circuit_state_name(circuit),
        skips,
        sage_trust_level_name(trust),
        trust_day,
        in_recovery ? "true" : "false",
        stopped ? "true" : "false",
        open_findings,
        tokens_today);

    err = false;
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_connect();

    PG_TRY();
    {
        static const char *insert_sql =
            "INSERT INTO sage.snapshots (category, data) "
            "VALUES ($1, $2::jsonb)";

        Oid     argtypes[2] = {TEXTOID, TEXTOID};
        Datum   values[2];
        char    nulls[2] = {' ', ' '};

        values[0] = CStringGetTextDatum("sage_health");
        values[1] = CStringGetTextDatum(json_buf.data);

        ret = SPI_execute_with_args(insert_sql, 2, argtypes, values, nulls,
                                    false, 0);

        if (ret != SPI_OK_INSERT)
            elog(WARNING, "pg_sage: failed to insert self-health snapshot (SPI returned %d)", ret);
    }
    PG_CATCH();
    {
        FlushErrorState();
        err = true;
        elog(WARNING, "pg_sage: error inserting self-health snapshot");
    }
    PG_END_TRY();

    SPI_finish();
    if (err)
        AbortCurrentTransaction();
    else
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
    }

    pfree(json_buf.data);

    elog(DEBUG1, "pg_sage: self-monitor collected — schema=%lld bytes, findings=%d, circuit=%s",
         (long long)schema_bytes, open_findings, sage_circuit_state_name(circuit));
}
