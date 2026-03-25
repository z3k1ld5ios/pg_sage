/*
 * findings.c — Findings engine for pg_sage
 *
 * Implements upsert, resolve, suppress, escalate logic for the
 * sage.findings table.
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "funcapi.h"
#include "catalog/pg_type.h"

/* SQL-callable function declarations */
PG_FUNCTION_INFO_V1(sage_suppress);

/* ----------------------------------------------------------------
 * sage_upsert_finding
 *
 * Core dedup-aware insert/update for findings.
 * Dedup key: (category, object_identifier) WHERE status = 'open'
 * Escalation: info -> warning after 7 days, warning -> critical after 14 days.
 * ---------------------------------------------------------------- */
void
sage_upsert_finding(const char *category, const char *severity,
                    const char *object_type, const char *object_id,
                    const char *title, const char *detail_json,
                    const char *recommendation,
                    const char *recommended_sql,
                    const char *rollback_sql)
{
    static const char *upsert_sql =
        "INSERT INTO sage.findings "
        "(category, severity, object_type, object_identifier, title, "
        " detail, recommendation, recommended_sql, rollback_sql) "
        "VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9) "
        "ON CONFLICT (category, object_identifier) "
        "WHERE status = 'open' "
        "DO UPDATE SET "
        "  last_seen = now(), "
        "  occurrence_count = sage.findings.occurrence_count + 1, "
        "  severity = CASE "
        "    WHEN sage.findings.created_at < now() - interval '7 days' "
        "         AND sage.findings.severity = 'info' THEN 'warning' "
        "    WHEN sage.findings.created_at < now() - interval '14 days' "
        "         AND sage.findings.severity = 'warning' THEN 'critical' "
        "    ELSE EXCLUDED.severity "
        "  END, "
        "  detail = EXCLUDED.detail, "
        "  recommendation = EXCLUDED.recommendation, "
        "  recommended_sql = EXCLUDED.recommended_sql, "
        "  rollback_sql = EXCLUDED.rollback_sql";

    Oid         argtypes[9];
    Datum       values[9];
    char        nulls[9];
    int         ret;
    int         i;

    /*
     * Save caller's SPI result set.  SPI_execute_with_args overwrites
     * the global SPI_tuptable / SPI_processed, which would corrupt any
     * outer loop that is iterating over a previous query's results.
     */
    SPITupleTable  *saved_tuptable  = SPI_tuptable;
    uint64          saved_processed = SPI_processed;

    /* All parameters are text */
    for (i = 0; i < 9; i++)
        argtypes[i] = TEXTOID;

    /* Build parameter values, handling NULLs */
    memset(nulls, ' ', sizeof(nulls));

    values[0] = CStringGetTextDatum(category ? category : "");
    values[1] = CStringGetTextDatum(severity ? severity : "info");
    values[2] = CStringGetTextDatum(object_type ? object_type : "");
    values[3] = CStringGetTextDatum(object_id ? object_id : "");
    values[4] = CStringGetTextDatum(title ? title : "");

    /* detail is NOT NULL — provide empty object as fallback */
    values[5] = CStringGetTextDatum(detail_json ? detail_json : "{}");

    if (recommendation != NULL)
        values[6] = CStringGetTextDatum(recommendation);
    else
    {
        values[6] = (Datum) 0;
        nulls[6] = 'n';
    }

    if (recommended_sql != NULL)
        values[7] = CStringGetTextDatum(recommended_sql);
    else
    {
        values[7] = (Datum) 0;
        nulls[7] = 'n';
    }

    if (rollback_sql != NULL)
        values[8] = CStringGetTextDatum(rollback_sql);
    else
    {
        values[8] = (Datum) 0;
        nulls[8] = 'n';
    }

    ret = SPI_execute_with_args(upsert_sql, 9, argtypes, values, nulls,
                                false, 0);

    if (ret != SPI_OK_INSERT)
    {
        elog(WARNING, "pg_sage: sage_upsert_finding failed for category=%s "
             "object=%s (SPI returned %d)",
             category ? category : "(null)",
             object_id ? object_id : "(null)",
             ret);
    }

    /* Restore caller's SPI result set */
    SPI_tuptable = saved_tuptable;
    SPI_processed = saved_processed;
}

/* ----------------------------------------------------------------
 * sage_resolve_finding
 *
 * Mark an open finding as resolved when the condition clears.
 * ---------------------------------------------------------------- */
void
sage_resolve_finding(const char *category, const char *object_id)
{
    static const char *resolve_sql =
        "UPDATE sage.findings "
        "SET status = 'resolved', resolved_at = now() "
        "WHERE category = $1 AND object_identifier = $2 AND status = 'open'";

    Oid     argtypes[2] = {TEXTOID, TEXTOID};
    Datum   values[2];
    char    nulls[2] = {' ', ' '};
    int     ret;

    /* Save caller's SPI result set (same rationale as upsert) */
    SPITupleTable  *saved_tuptable  = SPI_tuptable;
    uint64          saved_processed = SPI_processed;

    values[0] = CStringGetTextDatum(category ? category : "");
    values[1] = CStringGetTextDatum(object_id ? object_id : "");

    ret = SPI_execute_with_args(resolve_sql, 2, argtypes, values, nulls,
                                false, 0);

    if (ret != SPI_OK_UPDATE)
    {
        elog(WARNING, "pg_sage: sage_resolve_finding failed for category=%s "
             "object=%s (SPI returned %d)",
             category ? category : "(null)",
             object_id ? object_id : "(null)",
             ret);
    }

    /* Restore caller's SPI result set */
    SPI_tuptable = saved_tuptable;
    SPI_processed = saved_processed;
}

/* ----------------------------------------------------------------
 * sage_suppress — SQL-callable
 *
 * Suppress a finding for a given number of days, with a reason.
 *
 * sage_suppress(finding_id int, reason text, duration_days int)
 *   returns void
 * ---------------------------------------------------------------- */
Datum
sage_suppress(PG_FUNCTION_ARGS)
{
    int64       finding_id;
    char       *reason;
    int32       duration_days;
    int         ret;

    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                 errmsg("finding_id must not be NULL")));

    finding_id = PG_GETARG_INT64(0);
    reason = PG_ARGISNULL(1) ? pstrdup("no reason given")
                              : text_to_cstring(PG_GETARG_TEXT_PP(1));
    duration_days = PG_ARGISNULL(2) ? 30 : PG_GETARG_INT32(2);

    SPI_connect();

    /* Update status and suppressed_until */
    {
        static const char *suppress_sql =
            "UPDATE sage.findings "
            "SET status = 'suppressed', "
            "    suppressed_until = now() + make_interval(days => $2) "
            "WHERE id = $1";

        Oid     argtypes[2] = {INT8OID, INT4OID};
        Datum   values[2];
        char    nulls[2] = {' ', ' '};

        values[0] = Int64GetDatum(finding_id);
        values[1] = Int32GetDatum(duration_days);

        ret = SPI_execute_with_args(suppress_sql, 2, argtypes, values, nulls,
                                    false, 0);
        if (ret != SPI_OK_UPDATE || SPI_processed == 0)
        {
            SPI_finish();
            ereport(WARNING,
                    (errmsg("pg_sage: suppress failed or finding_id " INT64_FORMAT " not found",
                            finding_id)));
            PG_RETURN_VOID();
        }
    }

    /* Merge suppression_reason into the detail JSONB column */
    {
        Oid     argtypes2[2] = {INT8OID, TEXTOID};
        Datum   values2[2];
        char    nulls2[2] = {' ', ' '};

        values2[0] = Int64GetDatum(finding_id);
        values2[1] = CStringGetTextDatum(reason);

        ret = SPI_execute_with_args(
            "UPDATE sage.findings "
            "SET detail = COALESCE(detail, '{}'::jsonb) || "
            "  jsonb_build_object('suppression_reason', $2::text) "
            "WHERE id = $1",
            2, argtypes2, values2, nulls2, false, 0);

        if (ret != SPI_OK_UPDATE)
            elog(WARNING,
                 "pg_sage: failed to record suppression_reason for "
                 "finding_id " INT64_FORMAT, finding_id);
    }

    SPI_finish();

    elog(NOTICE, "pg_sage: finding " INT64_FORMAT " suppressed for %d days — %s",
         finding_id, duration_days, reason);

    pfree(reason);

    PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 * sage_check_suppressions
 *
 * Auto-unsuppress findings whose suppression has expired.
 * Called at the start of each analyzer cycle.
 * ---------------------------------------------------------------- */
void
sage_check_suppressions(void)
{
    static const char *unsuppress_sql =
        "UPDATE sage.findings "
        "SET status = 'open', suppressed_until = NULL "
        "WHERE status = 'suppressed' AND suppressed_until < now()";

    int ret;
    uint64 count;

    SPI_connect();

    ret = SPI_execute(unsuppress_sql, false, 0);

    if (ret != SPI_OK_UPDATE)
    {
        elog(WARNING, "pg_sage: sage_check_suppressions failed (SPI returned %d)",
             ret);
        SPI_finish();
        return;
    }

    count = SPI_processed;
    if (count > 0)
        elog(LOG, "pg_sage: unsuppressed " UINT64_FORMAT " expired findings", count);

    SPI_finish();
}
