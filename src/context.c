/*
 * context.c — Context Assembly Pipeline
 *
 * Gathers schema, query, plan, and finding data for LLM interactions.
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include <string.h>
#include "lib/stringinfo.h"
#include "executor/spi.h"
#include "utils/builtins.h"

/* ----------------------------------------------------------------
 * Token budget helpers
 *
 * Estimate ~4 chars per token.  We truncate lower-priority sections
 * first to stay within sage_llm_context_budget.
 * ---------------------------------------------------------------- */
#define CHARS_PER_TOKEN     4

static int
remaining_budget(int budget_tokens, int chars_used)
{
    int max_chars = budget_tokens * CHARS_PER_TOKEN;
    int left = max_chars - chars_used;

    return (left > 0) ? left : 0;
}

/*
 * Append a section to the context buffer, truncating if needed.
 * Returns true if the section was appended (even partially).
 */
static bool
append_section(StringInfo ctx, const char *header, const char *body,
               int budget_tokens)
{
    int     budget_left;
    int     header_len;
    int     body_len;

    if (body == NULL || body[0] == '\0')
        return false;

    budget_left = remaining_budget(budget_tokens, ctx->len);
    if (budget_left <= 0)
        return false;

    header_len = strlen(header);
    body_len = strlen(body);

    /* Need at least room for the header + some body */
    if (budget_left < header_len + 20)
        return false;

    appendStringInfoString(ctx, header);
    appendStringInfoChar(ctx, '\n');

    if (body_len + header_len + 1 <= budget_left)
    {
        appendStringInfoString(ctx, body);
    }
    else
    {
        /* Truncate body to fit budget */
        int trunc_len = budget_left - header_len - 20;

        if (trunc_len > 0)
        {
            appendBinaryStringInfo(ctx, body, trunc_len);
            appendStringInfoString(ctx, "\n... [truncated]");
        }
    }
    appendStringInfoChar(ctx, '\n');
    appendStringInfoChar(ctx, '\n');

    return true;
}

/* ----------------------------------------------------------------
 * SPI helper: execute a query with a text param and collect results
 * into a string.  Returns palloc'd string.
 * ---------------------------------------------------------------- */
static char *
spi_query_text_param(const char *sql, const char *param)
{
    StringInfoData  result;
    int             ret;
    int             i, j;
    Oid             argtypes[1] = { TEXTOID };
    Datum           values[1];
    char            nulls[1] = { ' ' };

    initStringInfo(&result);

    values[0] = CStringGetTextDatum(param);

    ret = SPI_execute_with_args(sql, 1, argtypes, values, nulls, true, 0);
    if (ret != SPI_OK_SELECT || SPI_tuptable == NULL)
    {
        pfree(DatumGetPointer(values[0]));
        return result.data;
    }

    for (i = 0; i < (int) SPI_processed; i++)
    {
        for (j = 0; j < SPI_tuptable->tupdesc->natts; j++)
        {
            bool    isnull;
            Datum   val;
            char   *str;

            val = SPI_getbinval(SPI_tuptable->vals[i],
                                SPI_tuptable->tupdesc,
                                j + 1, &isnull);
            if (isnull)
            {
                appendStringInfoString(&result, "NULL");
            }
            else
            {
                str = SPI_getvalue(SPI_tuptable->vals[i],
                                   SPI_tuptable->tupdesc,
                                   j + 1);
                if (str != NULL)
                    appendStringInfoString(&result, str);
                else
                    appendStringInfoString(&result, "NULL");
            }

            if (j < SPI_tuptable->tupdesc->natts - 1)
                appendStringInfoString(&result, " | ");
        }
        appendStringInfoChar(&result, '\n');
    }

    pfree(DatumGetPointer(values[0]));
    return result.data;
}

/* ----------------------------------------------------------------
 * SPI helper: execute a plain SQL query, return all results as text.
 * ---------------------------------------------------------------- */
static char *
spi_query_plain(const char *sql)
{
    StringInfoData  result;
    int             ret;
    int             i, j;

    initStringInfo(&result);

    ret = SPI_execute(sql, true, 0);
    if (ret != SPI_OK_SELECT || SPI_tuptable == NULL)
        return result.data;

    for (i = 0; i < (int) SPI_processed; i++)
    {
        for (j = 0; j < SPI_tuptable->tupdesc->natts; j++)
        {
            char *str = SPI_getvalue(SPI_tuptable->vals[i],
                                     SPI_tuptable->tupdesc,
                                     j + 1);
            if (str != NULL)
                appendStringInfoString(&result, str);
            else
                appendStringInfoString(&result, "NULL");

            if (j < SPI_tuptable->tupdesc->natts - 1)
                appendStringInfoString(&result, " | ");
        }
        appendStringInfoChar(&result, '\n');
    }

    return result.data;
}

/* ----------------------------------------------------------------
 * sage_assemble_context_named
 *
 * Assemble context for a specific named object (table).
 * ---------------------------------------------------------------- */
char *
sage_assemble_context_named(const char *object_name)
{
    StringInfoData  ctx;
    char           *section;
    int             budget = sage_llm_context_budget;
    int             connected;

    if (object_name == NULL || object_name[0] == '\0')
        return pstrdup("");

    initStringInfo(&ctx);

    connected = SPI_connect();
    if (connected != SPI_OK_CONNECT)
    {
        elog(WARNING, "pg_sage: SPI_connect failed in context assembly");
        return pstrdup("[Error: unable to query database]");
    }

    PG_TRY();
    {
        /* 1. DDL / schema definition */
        section = spi_query_text_param(
            "SELECT 'CREATE TABLE ' || c.table_schema || '.' || c.table_name || ' (' || "
            "       string_agg(c.column_name || ' ' || c.data_type || "
            "                  CASE WHEN c.is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END, ', ') || ')' "
            "FROM information_schema.columns c "
            "JOIN information_schema.tables t "
            "  ON t.table_name = c.table_name AND t.table_schema = c.table_schema "
            "WHERE c.table_name = $1 AND t.table_type = 'BASE TABLE' "
            "GROUP BY c.table_schema, c.table_name",
            object_name);
        append_section(&ctx, "[SCHEMA]", section, budget);
        pfree(section);

        /* 2. FK-connected tables (1 hop) */
        section = spi_query_text_param(
            "SELECT DISTINCT ccu.table_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON tc.constraint_name = ccu.constraint_name "
            "WHERE tc.table_name = $1 AND tc.constraint_type = 'FOREIGN KEY' "
            "UNION "
            "SELECT DISTINCT tc.table_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON tc.constraint_name = ccu.constraint_name "
            "WHERE ccu.table_name = $1 AND tc.constraint_type = 'FOREIGN KEY'",
            object_name);
        if (section[0] != '\0')
            append_section(&ctx, "[RELATED_TABLES]", section, budget);
        pfree(section);

        /* 3. Index definitions */
        section = spi_query_text_param(
            "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = $1",
            object_name);
        append_section(&ctx, "[INDEXES]", section, budget);
        pfree(section);

        /* 4. Top 10 queries touching the table */
        {
            StringInfoData  sql;

            initStringInfo(&sql);
            appendStringInfo(&sql,
                "SELECT queryid, query, calls, mean_exec_time, total_exec_time "
                "FROM pg_stat_statements s "
                "JOIN pg_database d ON d.oid = s.dbid "
                "WHERE d.datname = current_database() "
                "  AND query ILIKE '%%' || $1 || '%%' "
                "ORDER BY total_exec_time DESC LIMIT 10");

            section = spi_query_text_param(sql.data, object_name);
            append_section(&ctx, "[QUERIES]", section, budget);
            pfree(section);
            pfree(sql.data);
        }

        /* 5. Recent EXPLAIN plans from cache */
        {
            StringInfoData  sql;

            initStringInfo(&sql);
            appendStringInfo(&sql,
                "SELECT ec.queryid, ec.plan_json, ec.captured_at "
                "FROM sage.explain_cache ec "
                "WHERE ec.queryid IN ("
                "  SELECT s.queryid FROM pg_stat_statements s "
                "  JOIN pg_database d ON d.oid = s.dbid "
                "  WHERE d.datname = current_database() "
                "    AND s.query ILIKE '%%' || $1 || '%%'"
                ") "
                "ORDER BY ec.captured_at DESC LIMIT 5");

            section = spi_query_text_param(sql.data, object_name);
            if (section[0] != '\0')
                append_section(&ctx, "[PLANS]", section, budget);
            pfree(section);
            pfree(sql.data);
        }

        /* 6. Bloat / dead tuple stats */
        section = spi_query_text_param(
            "SELECT n_live_tup, n_dead_tup, last_vacuum, last_autovacuum, "
            "       pg_total_relation_size(relid) as total_bytes "
            "FROM pg_stat_user_tables WHERE relname = $1",
            object_name);
        append_section(&ctx, "[STATS]", section, budget);
        pfree(section);

        /* 7. Active locks */
        section = spi_query_text_param(
            "SELECT l.mode, l.granted, a.query, a.state "
            "FROM pg_locks l "
            "JOIN pg_class c ON c.oid = l.relation "
            "LEFT JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE c.relname = $1",
            object_name);
        if (section[0] != '\0')
            append_section(&ctx, "[LOCKS]", section, budget);
        pfree(section);

        /* 8. Open findings */
        section = spi_query_text_param(
            "SELECT title, severity, recommendation "
            "FROM sage.findings "
            "WHERE object_identifier LIKE '%%' || $1 || '%%' "
            "  AND status = 'open'",
            object_name);
        if (section[0] != '\0')
            append_section(&ctx, "[FINDINGS]", section, budget);
        pfree(section);
    }
    PG_CATCH();
    {
        SPI_finish();
        PG_RE_THROW();
    }
    PG_END_TRY();

    SPI_finish();

    return ctx.data;
}

/* ----------------------------------------------------------------
 * sage_assemble_context_system
 *
 * Assemble context for system-wide / vague questions.
 * ---------------------------------------------------------------- */
char *
sage_assemble_context_system(void)
{
    StringInfoData  ctx;
    char           *section;
    int             budget = sage_llm_context_budget;
    int             connected;

    initStringInfo(&ctx);

    connected = SPI_connect();
    if (connected != SPI_OK_CONNECT)
    {
        elog(WARNING, "pg_sage: SPI_connect failed in system context assembly");
        return pstrdup("[Error: unable to query database]");
    }

    PG_TRY();
    {
        /* 1. System health metrics */
        section = spi_query_plain(
            "SELECT "
            "  (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_backends, "
            "  (SELECT count(*) FROM pg_stat_activity) AS total_backends, "
            "  (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections, "
            "  (SELECT round(100.0 * sum(blks_hit) / NULLIF(sum(blks_hit) + sum(blks_read), 0), 2) "
            "   FROM pg_stat_database WHERE datname = current_database()) AS cache_hit_ratio, "
#if PG_VERSION_NUM >= 170000
            "  (SELECT num_timed + num_requested FROM pg_stat_checkpointer) AS total_checkpoints, "
#else
            "  (SELECT checkpoints_timed + checkpoints_req FROM pg_stat_bgwriter) AS total_checkpoints, "
#endif
            "  pg_postmaster_start_time() AS uptime_since, "
            "  pg_database_size(current_database()) AS db_size_bytes");
        append_section(&ctx, "[SYSTEM_HEALTH]", section, budget);
        pfree(section);

        /* 2. Critical and warning findings */
        section = spi_query_plain(
            "SELECT category, severity, title, object_identifier, recommendation "
            "FROM sage.findings "
            "WHERE status = 'open' AND severity IN ('critical', 'warning') "
            "ORDER BY "
            "  CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, "
            "  created_at DESC "
            "LIMIT 20");
        append_section(&ctx, "[CRITICAL_FINDINGS]", section, budget);
        pfree(section);

        /* 3. Recent actions (last 24h) */
        section = spi_query_plain(
            "SELECT action_type, sql_executed, outcome, executed_at "
            "FROM sage.action_log "
            "WHERE executed_at > now() - interval '1 day' "
            "ORDER BY executed_at DESC "
            "LIMIT 10");
        if (section[0] != '\0')
            append_section(&ctx, "[RECENT_ACTIONS]", section, budget);
        pfree(section);

        /* 4. Top 5 regressed queries */
        section = spi_query_plain(
            "SELECT f.title, f.object_identifier, f.recommendation "
            "FROM sage.findings f "
            "WHERE f.category = 'query_regression' AND f.status = 'open' "
            "ORDER BY f.created_at DESC "
            "LIMIT 5");
        if (section[0] != '\0')
            append_section(&ctx, "[TOP_QUERIES]", section, budget);
        pfree(section);

        /* 5. Replication lag if applicable */
        section = spi_query_plain(
            "SELECT client_addr, state, "
            "       pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replay_lag_bytes, "
            "       replay_lag "
            "FROM pg_stat_replication");
        if (section[0] != '\0')
            append_section(&ctx, "[REPLICATION]", section, budget);
        pfree(section);
    }
    PG_CATCH();
    {
        SPI_finish();
        PG_RE_THROW();
    }
    PG_END_TRY();

    SPI_finish();

    return ctx.data;
}
