/*
 * tier2_extra.c — Tier 2 extra analysis features for pg_sage
 *
 * Implements cost attribution (2.5), migration review (2.6),
 * and schema design review (2.7).
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include <math.h>
#include <string.h>

#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

/* Default cloud storage cost estimate: $0.10 per GB per month */
#define DEFAULT_STORAGE_COST_PER_GB_MONTH   0.10

/* ----------------------------------------------------------------
 * Helper: query a single config value from sage.config.
 * Returns the value as a double, or default_val if not found.
 * Caller must already have an SPI connection open.
 * ---------------------------------------------------------------- */
static double
get_config_double(const char *key, double default_val)
{
    StringInfoData sql;
    int             ret;
    double          result = default_val;

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT value FROM sage.config WHERE key = '%s'",
        key);

    SPI_execute("SAVEPOINT _sage_config_read", false, 0);

    PG_TRY();
    {
        ret = sage_spi_exec(sql.data, 0);
        if (ret > 0 && !sage_spi_isnull(0, 0))
        {
            char *val_str = sage_spi_getval_str(0, 0);
            if (val_str)
            {
                result = strtod(val_str, NULL);
            }
        }
        SPI_execute("RELEASE SAVEPOINT _sage_config_read", false, 0);
    }
    PG_CATCH();
    {
        FlushErrorState();
        SPI_execute("ROLLBACK TO SAVEPOINT _sage_config_read", false, 0);
        SPI_execute("RELEASE SAVEPOINT _sage_config_read", false, 0);
    }
    PG_END_TRY();

    pfree(sql.data);
    return result;
}

/* ----------------------------------------------------------------
 * sage_analyze_cost_attribution — Tier 2.5
 *
 * Annotates existing findings with estimated monthly cost in USD
 * and produces a cost_summary finding.
 * ---------------------------------------------------------------- */
void
sage_analyze_cost_attribution(void)
{
    double  cost_per_cpu_hour;
    double  cost_per_iops;
    double  total_waste_usd = 0.0;
    int     ret;
    int     i;
    int     findings_annotated = 0;

    /* Skip if cloud provider is not configured */
    if (sage_cloud_provider == NULL || sage_cloud_provider[0] == '\0')
        return;

    SPI_connect();
    sage_spi_exec("SET LOCAL statement_timeout = '500ms'", 0);

    /* Fetch config values */
    cost_per_cpu_hour = get_config_double("cost_per_cpu_hour_usd", 0.0);
    cost_per_iops     = get_config_double("cost_per_iops", 0.0);

    /* ----------------------------------------------------------------
     * Part 1: Annotate unused-index findings with storage cost
     * ---------------------------------------------------------------- */
    {
        static const char *unused_idx_sql =
            "SELECT f.id, f.object_identifier "
            "FROM sage.findings f "
            "WHERE f.category = 'unused_index' "
            "  AND f.status = 'open' "
            "LIMIT 200";

        SPI_execute("SAVEPOINT _sage_cost_unused", false, 0);

        PG_TRY();
        {
            ret = sage_spi_exec(unused_idx_sql, 0);

            if (ret > 0)
            {
                int         nrows = (int) SPI_processed;
                int64      *ids = palloc(sizeof(int64) * nrows);
                char      **obj_ids = palloc(sizeof(char *) * nrows);

                /* Copy results before re-entering SPI */
                for (i = 0; i < nrows; i++)
                {
                    ids[i] = sage_spi_getval_int64(i, 0);
                    obj_ids[i] = pstrdup(sage_spi_getval_str(i, 1));
                }

                for (i = 0; i < nrows; i++)
                {
                    StringInfoData size_sql;
                    double         monthly_cost;

                    initStringInfo(&size_sql);
                    appendStringInfo(&size_sql,
                        "SELECT pg_relation_size('%s'::regclass)",
                        obj_ids[i]);

                    SPI_execute("SAVEPOINT _sage_cost_size", false, 0);

                    PG_TRY();
                    {
                        int sret = sage_spi_exec(size_sql.data, 0);
                        if (sret > 0 && !sage_spi_isnull(0, 0))
                        {
                            int64   size_bytes = sage_spi_getval_int64(0, 0);
                            double  size_gb = (double) size_bytes / (1024.0 * 1024.0 * 1024.0);
                            StringInfoData upd;

                            monthly_cost = size_gb * DEFAULT_STORAGE_COST_PER_GB_MONTH;
                            total_waste_usd += monthly_cost;

                            initStringInfo(&upd);
                            appendStringInfo(&upd,
                                "UPDATE sage.findings "
                                "SET estimated_cost_usd = %.6f "
                                "WHERE id = " INT64_FORMAT,
                                monthly_cost, ids[i]);
                            sage_spi_exec(upd.data, 0);
                            pfree(upd.data);
                            findings_annotated++;
                        }
                        SPI_execute("RELEASE SAVEPOINT _sage_cost_size", false, 0);
                    }
                    PG_CATCH();
                    {
                        FlushErrorState();
                        SPI_execute("ROLLBACK TO SAVEPOINT _sage_cost_size", false, 0);
                        SPI_execute("RELEASE SAVEPOINT _sage_cost_size", false, 0);
                        /* Index may have been dropped — skip */
                    }
                    PG_END_TRY();

                    pfree(size_sql.data);
                    pfree(obj_ids[i]);
                }

                pfree(ids);
                pfree(obj_ids);
            }

            SPI_execute("RELEASE SAVEPOINT _sage_cost_unused", false, 0);
        }
        PG_CATCH();
        {
            FlushErrorState();
            SPI_execute("ROLLBACK TO SAVEPOINT _sage_cost_unused", false, 0);
            SPI_execute("RELEASE SAVEPOINT _sage_cost_unused", false, 0);
        }
        PG_END_TRY();
    }

    /* ----------------------------------------------------------------
     * Part 2: Annotate missing-index findings with IOPS cost
     * ---------------------------------------------------------------- */
    if (cost_per_iops > 0.0)
    {
        static const char *missing_idx_sql =
            "SELECT f.id, f.object_identifier, "
            "       f.detail->>'table' as tblname "
            "FROM sage.findings f "
            "WHERE f.category = 'missing_index' "
            "  AND f.status = 'open' "
            "LIMIT 200";

        SPI_execute("SAVEPOINT _sage_cost_missing", false, 0);

        PG_TRY();
        {
            ret = sage_spi_exec(missing_idx_sql, 0);

            if (ret > 0)
            {
                int         nrows = (int) SPI_processed;
                int64      *ids = palloc(sizeof(int64) * nrows);
                char      **tblnames = palloc(sizeof(char *) * nrows);

                for (i = 0; i < nrows; i++)
                {
                    ids[i] = sage_spi_getval_int64(i, 0);
                    if (!sage_spi_isnull(i, 2))
                        tblnames[i] = pstrdup(sage_spi_getval_str(i, 2));
                    else
                        tblnames[i] = NULL;
                }

                for (i = 0; i < nrows; i++)
                {
                    if (tblnames[i] == NULL)
                        continue;

                    {
                        StringInfoData stat_sql;
                        initStringInfo(&stat_sql);
                        appendStringInfo(&stat_sql,
                            "SELECT seq_scan, seq_tup_read "
                            "FROM pg_stat_user_tables "
                            "WHERE schemaname || '.' || relname = '%s' "
                            "LIMIT 1",
                            tblnames[i]);

                        SPI_execute("SAVEPOINT _sage_cost_iops", false, 0);

                        PG_TRY();
                        {
                            int sret = sage_spi_exec(stat_sql.data, 0);
                            if (sret > 0 && !sage_spi_isnull(0, 0) && !sage_spi_isnull(0, 1))
                            {
                                int64   seq_scan     = sage_spi_getval_int64(0, 0);
                                int64   seq_tup_read = sage_spi_getval_int64(0, 1);
                                double  monthly_cost;
                                StringInfoData upd;

                                /*
                                 * Estimate: each seq scan that reads >1000 tuples
                                 * represents excess IOPS.  Rough model:
                                 * excess_reads_per_month ~= seq_tup_read * 30 (daily)
                                 * cost = excess_reads * cost_per_iops
                                 *
                                 * We scale by seq_scan to avoid double-counting.
                                 * Assume stats represent roughly 1 day of activity.
                                 */
                                if (seq_scan > 0 && seq_tup_read > 1000)
                                {
                                    double excess_monthly = (double) seq_tup_read * 30.0;
                                    monthly_cost = excess_monthly * cost_per_iops;
                                    total_waste_usd += monthly_cost;

                                    initStringInfo(&upd);
                                    appendStringInfo(&upd,
                                        "UPDATE sage.findings "
                                        "SET estimated_cost_usd = %.6f "
                                        "WHERE id = " INT64_FORMAT,
                                        monthly_cost, ids[i]);
                                    sage_spi_exec(upd.data, 0);
                                    pfree(upd.data);
                                    findings_annotated++;
                                }
                            }
                            SPI_execute("RELEASE SAVEPOINT _sage_cost_iops", false, 0);
                        }
                        PG_CATCH();
                        {
                            FlushErrorState();
                            SPI_execute("ROLLBACK TO SAVEPOINT _sage_cost_iops", false, 0);
                            SPI_execute("RELEASE SAVEPOINT _sage_cost_iops", false, 0);
                        }
                        PG_END_TRY();

                        pfree(stat_sql.data);
                    }

                    pfree(tblnames[i]);
                }

                pfree(ids);
                pfree(tblnames);
            }

            SPI_execute("RELEASE SAVEPOINT _sage_cost_missing", false, 0);
        }
        PG_CATCH();
        {
            FlushErrorState();
            SPI_execute("ROLLBACK TO SAVEPOINT _sage_cost_missing", false, 0);
            SPI_execute("RELEASE SAVEPOINT _sage_cost_missing", false, 0);
        }
        PG_END_TRY();
    }

    /* ----------------------------------------------------------------
     * Part 3: Create cost_summary finding
     * ---------------------------------------------------------------- */
    if (findings_annotated > 0)
    {
        StringInfoData detail;
        StringInfoData title;
        StringInfoData rec;
        char *provider_esc = sage_escape_json_string(sage_cloud_provider);
        char *instance_esc = sage_escape_json_string(
            sage_instance_type ? sage_instance_type : "unknown");

        initStringInfo(&detail);
        initStringInfo(&title);
        initStringInfo(&rec);

        appendStringInfo(&detail,
            "{\"cloud_provider\": \"%s\", "
            "\"instance_type\": \"%s\", "
            "\"total_monthly_waste_usd\": %.2f, "
            "\"findings_annotated\": %d}",
            provider_esc,
            instance_esc,
            total_waste_usd,
            findings_annotated);

        appendStringInfo(&title,
            "Estimated monthly waste: $%.2f across %d findings (%s)",
            total_waste_usd, findings_annotated, sage_cloud_provider);

        appendStringInfo(&rec,
            "Review the %d cost-annotated findings. "
            "Addressing unused indexes and adding missing indexes could "
            "save an estimated $%.2f/month on %s (%s).",
            findings_annotated, total_waste_usd,
            sage_cloud_provider,
            sage_instance_type ? sage_instance_type : "unknown");

        sage_upsert_finding(
            "cost_summary",
            total_waste_usd > 50.0 ? "warning" : "info",
            "database",
            "cost_summary",
            title.data,
            detail.data,
            rec.data,
            NULL,   /* no recommended_sql */
            NULL);  /* no rollback_sql */

        pfree(detail.data);
        pfree(title.data);
        pfree(rec.data);
        pfree(provider_esc);
        pfree(instance_esc);
    }
    else
    {
        /* No findings to annotate — resolve any stale cost_summary */
        sage_resolve_finding("cost_summary", "cost_summary");
    }

    SPI_finish();
}

/* ----------------------------------------------------------------
 * sage_analyze_migration_review — Tier 2.6
 *
 * Detect long-running DDL operations and produce warning findings.
 * ---------------------------------------------------------------- */
void
sage_analyze_migration_review(void)
{
    static const char *ddl_sql =
        "SELECT pid, query, "
        "       extract(epoch from now() - query_start)::int as duration_secs "
        "FROM pg_stat_activity "
        "WHERE query ~* '^\\s*(CREATE|ALTER|DROP|REINDEX)' "
        "  AND state = 'active' "
        "  AND now() - query_start > interval '5 minutes' "
        "ORDER BY query_start "
        "LIMIT 20";

    int     ret;
    int     i;

    SPI_connect();
    sage_spi_exec("SET LOCAL statement_timeout = '500ms'", 0);

    SPI_execute("SAVEPOINT _sage_migration", false, 0);

    PG_TRY();
    {
        ret = sage_spi_exec(ddl_sql, 0);

        if (ret > 0)
        {
            int nrows = (int) SPI_processed;

            /* Copy all results before re-entering SPI with upsert_finding */
            int    *pids = palloc(sizeof(int) * nrows);
            char  **queries = palloc(sizeof(char *) * nrows);
            int    *durations = palloc(sizeof(int) * nrows);

            for (i = 0; i < nrows; i++)
            {
                pids[i] = (int) sage_spi_getval_int64(i, 0);
                queries[i] = sage_spi_isnull(i, 1) ? pstrdup("(unknown)")
                                                     : pstrdup(sage_spi_getval_str(i, 1));
                durations[i] = sage_spi_isnull(i, 2) ? 0
                                                       : (int) sage_spi_getval_int64(i, 2);
            }

            for (i = 0; i < nrows; i++)
            {
                StringInfoData detail;
                StringInfoData title;
                StringInfoData rec;
                StringInfoData object_id;
                char *query_esc;

                /* Truncate query for display */
                char query_preview[256];
                int qlen = strlen(queries[i]);
                if (qlen > 200)
                {
                    memcpy(query_preview, queries[i], 200);
                    query_preview[200] = '\0';
                    strcat(query_preview, "...");
                }
                else
                {
                    memcpy(query_preview, queries[i], qlen + 1);
                }

                query_esc = sage_escape_json_string(query_preview);

                initStringInfo(&detail);
                initStringInfo(&title);
                initStringInfo(&rec);
                initStringInfo(&object_id);

                appendStringInfo(&object_id, "ddl_pid_%d", pids[i]);

                appendStringInfo(&detail,
                    "{\"pid\": %d, "
                    "\"query\": \"%s\", "
                    "\"duration_seconds\": %d}",
                    pids[i], query_esc, durations[i]);

                appendStringInfo(&title,
                    "Long-running DDL detected (pid %d, %d min %d sec)",
                    pids[i], durations[i] / 60, durations[i] % 60);

                appendStringInfo(&rec,
                    "PID %d has been running DDL for %d minutes. "
                    "Long-running DDL can hold AccessExclusiveLock, blocking all "
                    "concurrent queries. Consider: (1) check if the operation is "
                    "still making progress, (2) use CONCURRENTLY variants where "
                    "possible, (3) schedule DDL during maintenance windows, "
                    "(4) set a lock_timeout before DDL to fail fast.",
                    pids[i], durations[i] / 60);

                sage_upsert_finding(
                    "migration_review",
                    durations[i] > 1800 ? "critical" : "warning",
                    "session",
                    object_id.data,
                    title.data,
                    detail.data,
                    rec.data,
                    NULL,   /* no recommended_sql */
                    NULL);  /* no rollback_sql */

                pfree(detail.data);
                pfree(title.data);
                pfree(rec.data);
                pfree(object_id.data);
                pfree(query_esc);
                pfree(queries[i]);
            }

            pfree(pids);
            pfree(queries);
            pfree(durations);
        }

        SPI_execute("RELEASE SAVEPOINT _sage_migration", false, 0);
    }
    PG_CATCH();
    {
        FlushErrorState();
        SPI_execute("ROLLBACK TO SAVEPOINT _sage_migration", false, 0);
        SPI_execute("RELEASE SAVEPOINT _sage_migration", false, 0);
    }
    PG_END_TRY();

    SPI_finish();
}

/* ----------------------------------------------------------------
 * sage_analyze_schema_design — Tier 2.7
 *
 * Structural schema quality checks:
 *   - Timezone-naive timestamps
 *   - Missing primary keys
 *   - Wide tables (> 50 columns)
 *   - Missing foreign key coverage for _id columns
 *   - Naming convention issues (mixed case, spaces)
 * ---------------------------------------------------------------- */
void
sage_analyze_schema_design(void)
{
    int     ret;
    int     i;

    SPI_connect();
    sage_spi_exec("SET LOCAL statement_timeout = '500ms'", 0);

    /* ----------------------------------------------------------------
     * Check 1: Timezone-naive timestamps
     * ---------------------------------------------------------------- */
    {
        static const char *tz_naive_sql =
            "SELECT table_schema, table_name, column_name "
            "FROM information_schema.columns "
            "WHERE data_type = 'timestamp without time zone' "
            "  AND table_schema NOT IN ('pg_catalog', 'information_schema', 'sage') "
            "ORDER BY table_schema, table_name, column_name "
            "LIMIT 100";

        SPI_execute("SAVEPOINT _sage_schema_tz", false, 0);

        PG_TRY();
        {
            ret = sage_spi_exec(tz_naive_sql, 0);

            if (ret > 0)
            {
                int nrows = (int) SPI_processed;
                char **schemas = palloc(sizeof(char *) * nrows);
                char **tables  = palloc(sizeof(char *) * nrows);
                char **columns = palloc(sizeof(char *) * nrows);

                for (i = 0; i < nrows; i++)
                {
                    schemas[i] = pstrdup(sage_spi_getval_str(i, 0));
                    tables[i]  = pstrdup(sage_spi_getval_str(i, 1));
                    columns[i] = pstrdup(sage_spi_getval_str(i, 2));
                }

                for (i = 0; i < nrows; i++)
                {
                    StringInfoData detail;
                    StringInfoData object_id;
                    StringInfoData title;
                    StringInfoData rec;
                    StringInfoData rec_sql;
                    char *schema_esc = sage_escape_json_string(schemas[i]);
                    char *table_esc  = sage_escape_json_string(tables[i]);
                    char *col_esc    = sage_escape_json_string(columns[i]);

                    initStringInfo(&detail);
                    initStringInfo(&object_id);
                    initStringInfo(&title);
                    initStringInfo(&rec);
                    initStringInfo(&rec_sql);

                    appendStringInfo(&object_id, "%s.%s.%s",
                        schemas[i], tables[i], columns[i]);

                    appendStringInfo(&detail,
                        "{\"schema\": \"%s\", \"table\": \"%s\", "
                        "\"column\": \"%s\", \"current_type\": "
                        "\"timestamp without time zone\"}",
                        schema_esc, table_esc, col_esc);

                    appendStringInfo(&title,
                        "Timezone-naive timestamp: %s.%s.%s",
                        schemas[i], tables[i], columns[i]);

                    appendStringInfo(&rec,
                        "Column %s.%s.%s uses 'timestamp without time zone'. "
                        "This can cause subtle bugs with timezone conversions. "
                        "Consider migrating to 'timestamptz' "
                        "(timestamp with time zone).",
                        schemas[i], tables[i], columns[i]);

                    appendStringInfo(&rec_sql,
                        "ALTER TABLE %s.%s "
                        "ALTER COLUMN %s TYPE timestamptz "
                        "USING %s AT TIME ZONE 'UTC';",
                        schemas[i], tables[i], columns[i], columns[i]);

                    sage_upsert_finding(
                        "schema_design",
                        "info",
                        "column",
                        object_id.data,
                        title.data,
                        detail.data,
                        rec.data,
                        rec_sql.data,
                        NULL);

                    pfree(detail.data);
                    pfree(object_id.data);
                    pfree(title.data);
                    pfree(rec.data);
                    pfree(rec_sql.data);
                    pfree(schema_esc);
                    pfree(table_esc);
                    pfree(col_esc);
                    pfree(schemas[i]);
                    pfree(tables[i]);
                    pfree(columns[i]);
                }

                pfree(schemas);
                pfree(tables);
                pfree(columns);
            }

            SPI_execute("RELEASE SAVEPOINT _sage_schema_tz", false, 0);
        }
        PG_CATCH();
        {
            FlushErrorState();
            SPI_execute("ROLLBACK TO SAVEPOINT _sage_schema_tz", false, 0);
            SPI_execute("RELEASE SAVEPOINT _sage_schema_tz", false, 0);
        }
        PG_END_TRY();
    }

    /* ----------------------------------------------------------------
     * Check 2: Missing primary keys
     * ---------------------------------------------------------------- */
    {
        static const char *no_pk_sql =
            "SELECT n.nspname, c.relname "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind = 'r' "
            "  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sage') "
            "  AND NOT EXISTS ( "
            "      SELECT 1 FROM pg_constraint con "
            "      WHERE con.conrelid = c.oid AND con.contype = 'p' "
            "  ) "
            "ORDER BY n.nspname, c.relname "
            "LIMIT 100";

        SPI_execute("SAVEPOINT _sage_schema_pk", false, 0);

        PG_TRY();
        {
            ret = sage_spi_exec(no_pk_sql, 0);

            if (ret > 0)
            {
                int nrows = (int) SPI_processed;
                char **schemas = palloc(sizeof(char *) * nrows);
                char **tables  = palloc(sizeof(char *) * nrows);

                for (i = 0; i < nrows; i++)
                {
                    schemas[i] = pstrdup(sage_spi_getval_str(i, 0));
                    tables[i]  = pstrdup(sage_spi_getval_str(i, 1));
                }

                for (i = 0; i < nrows; i++)
                {
                    StringInfoData detail;
                    StringInfoData object_id;
                    StringInfoData title;
                    char *schema_esc = sage_escape_json_string(schemas[i]);
                    char *table_esc  = sage_escape_json_string(tables[i]);

                    initStringInfo(&detail);
                    initStringInfo(&object_id);
                    initStringInfo(&title);

                    appendStringInfo(&object_id, "%s.%s", schemas[i], tables[i]);

                    appendStringInfo(&detail,
                        "{\"schema\": \"%s\", \"table\": \"%s\"}",
                        schema_esc, table_esc);

                    appendStringInfo(&title,
                        "Missing primary key: %s.%s",
                        schemas[i], tables[i]);

                    sage_upsert_finding(
                        "schema_design",
                        "warning",
                        "table",
                        object_id.data,
                        title.data,
                        detail.data,
                        "Tables without primary keys can cause issues with "
                        "logical replication, ORMs, and query optimization. "
                        "Consider adding a primary key.",
                        NULL,
                        NULL);

                    pfree(detail.data);
                    pfree(object_id.data);
                    pfree(title.data);
                    pfree(schema_esc);
                    pfree(table_esc);
                    pfree(schemas[i]);
                    pfree(tables[i]);
                }

                pfree(schemas);
                pfree(tables);
            }

            SPI_execute("RELEASE SAVEPOINT _sage_schema_pk", false, 0);
        }
        PG_CATCH();
        {
            FlushErrorState();
            SPI_execute("ROLLBACK TO SAVEPOINT _sage_schema_pk", false, 0);
            SPI_execute("RELEASE SAVEPOINT _sage_schema_pk", false, 0);
        }
        PG_END_TRY();
    }

    /* ----------------------------------------------------------------
     * Check 3: Wide tables (> 50 columns)
     * ---------------------------------------------------------------- */
    {
        static const char *wide_sql =
            "SELECT table_schema, table_name, count(*) as col_count "
            "FROM information_schema.columns "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'sage') "
            "GROUP BY table_schema, table_name "
            "HAVING count(*) > 50 "
            "ORDER BY count(*) DESC "
            "LIMIT 50";

        SPI_execute("SAVEPOINT _sage_schema_wide", false, 0);

        PG_TRY();
        {
            ret = sage_spi_exec(wide_sql, 0);

            if (ret > 0)
            {
                int nrows = (int) SPI_processed;
                char **schemas   = palloc(sizeof(char *) * nrows);
                char **tables    = palloc(sizeof(char *) * nrows);
                int64 *col_counts = palloc(sizeof(int64) * nrows);

                for (i = 0; i < nrows; i++)
                {
                    schemas[i]    = pstrdup(sage_spi_getval_str(i, 0));
                    tables[i]     = pstrdup(sage_spi_getval_str(i, 1));
                    col_counts[i] = sage_spi_getval_int64(i, 2);
                }

                for (i = 0; i < nrows; i++)
                {
                    StringInfoData detail;
                    StringInfoData object_id;
                    StringInfoData title;
                    StringInfoData rec;
                    char *schema_esc = sage_escape_json_string(schemas[i]);
                    char *table_esc  = sage_escape_json_string(tables[i]);

                    initStringInfo(&detail);
                    initStringInfo(&object_id);
                    initStringInfo(&title);
                    initStringInfo(&rec);

                    appendStringInfo(&object_id, "%s.%s", schemas[i], tables[i]);

                    appendStringInfo(&detail,
                        "{\"schema\": \"%s\", \"table\": \"%s\", "
                        "\"column_count\": " INT64_FORMAT "}",
                        schema_esc, table_esc, col_counts[i]);

                    appendStringInfo(&title,
                        "Wide table: %s.%s has " INT64_FORMAT " columns",
                        schemas[i], tables[i], col_counts[i]);

                    appendStringInfo(&rec,
                        "Table %s.%s has " INT64_FORMAT " columns. Wide tables "
                        "can indicate denormalization that hurts cache "
                        "efficiency, increases TOAST overhead, and makes "
                        "queries harder to optimize. Consider splitting into "
                        "related tables via normalization.",
                        schemas[i], tables[i], col_counts[i]);

                    sage_upsert_finding(
                        "schema_design",
                        "info",
                        "table",
                        object_id.data,
                        title.data,
                        detail.data,
                        rec.data,
                        NULL,
                        NULL);

                    pfree(detail.data);
                    pfree(object_id.data);
                    pfree(title.data);
                    pfree(rec.data);
                    pfree(schema_esc);
                    pfree(table_esc);
                    pfree(schemas[i]);
                    pfree(tables[i]);
                }

                pfree(schemas);
                pfree(tables);
                pfree(col_counts);
            }

            SPI_execute("RELEASE SAVEPOINT _sage_schema_wide", false, 0);
        }
        PG_CATCH();
        {
            FlushErrorState();
            SPI_execute("ROLLBACK TO SAVEPOINT _sage_schema_wide", false, 0);
            SPI_execute("RELEASE SAVEPOINT _sage_schema_wide", false, 0);
        }
        PG_END_TRY();
    }

    /* ----------------------------------------------------------------
     * Check 4: Missing foreign key coverage for _id columns
     * ---------------------------------------------------------------- */
    {
        static const char *fk_sql =
            "SELECT c.table_schema, c.table_name, c.column_name "
            "FROM information_schema.columns c "
            "WHERE c.column_name LIKE '%%\\_id' "
            "  AND c.table_schema NOT IN ('pg_catalog', 'information_schema', 'sage') "
            "  AND NOT EXISTS ( "
            "      SELECT 1 "
            "      FROM information_schema.key_column_usage kcu "
            "      JOIN information_schema.table_constraints tc "
            "        ON tc.constraint_name = kcu.constraint_name "
            "       AND tc.table_schema = kcu.table_schema "
            "      WHERE tc.constraint_type = 'FOREIGN KEY' "
            "        AND kcu.table_schema = c.table_schema "
            "        AND kcu.table_name = c.table_name "
            "        AND kcu.column_name = c.column_name "
            "  ) "
            "ORDER BY c.table_schema, c.table_name, c.column_name "
            "LIMIT 100";

        SPI_execute("SAVEPOINT _sage_schema_fk", false, 0);

        PG_TRY();
        {
            ret = sage_spi_exec(fk_sql, 0);

            if (ret > 0)
            {
                int nrows = (int) SPI_processed;
                char **schemas = palloc(sizeof(char *) * nrows);
                char **tables  = palloc(sizeof(char *) * nrows);
                char **columns = palloc(sizeof(char *) * nrows);

                for (i = 0; i < nrows; i++)
                {
                    schemas[i] = pstrdup(sage_spi_getval_str(i, 0));
                    tables[i]  = pstrdup(sage_spi_getval_str(i, 1));
                    columns[i] = pstrdup(sage_spi_getval_str(i, 2));
                }

                for (i = 0; i < nrows; i++)
                {
                    StringInfoData detail;
                    StringInfoData object_id;
                    StringInfoData title;
                    StringInfoData rec;
                    char *schema_esc = sage_escape_json_string(schemas[i]);
                    char *table_esc  = sage_escape_json_string(tables[i]);
                    char *col_esc    = sage_escape_json_string(columns[i]);

                    initStringInfo(&detail);
                    initStringInfo(&object_id);
                    initStringInfo(&title);
                    initStringInfo(&rec);

                    appendStringInfo(&object_id, "%s.%s.%s",
                        schemas[i], tables[i], columns[i]);

                    appendStringInfo(&detail,
                        "{\"schema\": \"%s\", \"table\": \"%s\", "
                        "\"column\": \"%s\"}",
                        schema_esc, table_esc, col_esc);

                    appendStringInfo(&title,
                        "Possible missing FK: %s.%s.%s",
                        schemas[i], tables[i], columns[i]);

                    appendStringInfo(&rec,
                        "Column %s.%s.%s is named with an '_id' suffix but has "
                        "no foreign key constraint. If this references another "
                        "table, adding a FK constraint enforces referential "
                        "integrity and can help the query planner.",
                        schemas[i], tables[i], columns[i]);

                    sage_upsert_finding(
                        "schema_design",
                        "info",
                        "column",
                        object_id.data,
                        title.data,
                        detail.data,
                        rec.data,
                        NULL,
                        NULL);

                    pfree(detail.data);
                    pfree(object_id.data);
                    pfree(title.data);
                    pfree(rec.data);
                    pfree(schema_esc);
                    pfree(table_esc);
                    pfree(col_esc);
                    pfree(schemas[i]);
                    pfree(tables[i]);
                    pfree(columns[i]);
                }

                pfree(schemas);
                pfree(tables);
                pfree(columns);
            }

            SPI_execute("RELEASE SAVEPOINT _sage_schema_fk", false, 0);
        }
        PG_CATCH();
        {
            FlushErrorState();
            SPI_execute("ROLLBACK TO SAVEPOINT _sage_schema_fk", false, 0);
            SPI_execute("RELEASE SAVEPOINT _sage_schema_fk", false, 0);
        }
        PG_END_TRY();
    }

    /* ----------------------------------------------------------------
     * Check 5: Naming convention issues (mixed case, spaces)
     * ---------------------------------------------------------------- */
    {
        /*
         * Find tables or columns whose names contain uppercase letters
         * or spaces, which require quoting and are error-prone.
         */
        static const char *naming_sql =
            "SELECT n.nspname, c.relname, 'table' as obj_type, "
            "       NULL as column_name "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('r', 'v', 'm') "
            "  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sage') "
            "  AND (c.relname ~ '[A-Z]' OR c.relname ~ ' ') "
            "UNION ALL "
            "SELECT table_schema, table_name, 'column', column_name "
            "FROM information_schema.columns "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'sage') "
            "  AND (column_name ~ '[A-Z]' OR column_name ~ ' ') "
            "ORDER BY 1, 2 "
            "LIMIT 100";

        SPI_execute("SAVEPOINT _sage_schema_naming", false, 0);

        PG_TRY();
        {
            ret = sage_spi_exec(naming_sql, 0);

            if (ret > 0)
            {
                int nrows = (int) SPI_processed;
                char **schemas   = palloc(sizeof(char *) * nrows);
                char **names     = palloc(sizeof(char *) * nrows);
                char **obj_types = palloc(sizeof(char *) * nrows);
                char **col_names = palloc(sizeof(char *) * nrows);

                for (i = 0; i < nrows; i++)
                {
                    schemas[i]   = pstrdup(sage_spi_getval_str(i, 0));
                    names[i]     = pstrdup(sage_spi_getval_str(i, 1));
                    obj_types[i] = pstrdup(sage_spi_getval_str(i, 2));
                    col_names[i] = sage_spi_isnull(i, 3)
                                   ? NULL
                                   : pstrdup(sage_spi_getval_str(i, 3));
                }

                for (i = 0; i < nrows; i++)
                {
                    StringInfoData detail;
                    StringInfoData object_id;
                    StringInfoData title;
                    char *schema_esc = sage_escape_json_string(schemas[i]);
                    char *name_esc   = sage_escape_json_string(names[i]);
                    char *col_esc    = col_names[i]
                                       ? sage_escape_json_string(col_names[i])
                                       : NULL;

                    initStringInfo(&detail);
                    initStringInfo(&object_id);
                    initStringInfo(&title);

                    if (col_names[i] != NULL)
                    {
                        appendStringInfo(&object_id, "naming:%s.%s.%s",
                            schemas[i], names[i], col_names[i]);
                        appendStringInfo(&detail,
                            "{\"schema\": \"%s\", \"table\": \"%s\", "
                            "\"column\": \"%s\", \"issue\": \"mixed_case_or_spaces\"}",
                            schema_esc, name_esc, col_esc);
                        appendStringInfo(&title,
                            "Naming convention: column %s.%s.%s uses "
                            "mixed case or spaces",
                            schemas[i], names[i], col_names[i]);
                    }
                    else
                    {
                        appendStringInfo(&object_id, "naming:%s.%s",
                            schemas[i], names[i]);
                        appendStringInfo(&detail,
                            "{\"schema\": \"%s\", \"%s\": \"%s\", "
                            "\"issue\": \"mixed_case_or_spaces\"}",
                            schema_esc, obj_types[i], name_esc);
                        appendStringInfo(&title,
                            "Naming convention: %s %s.%s uses "
                            "mixed case or spaces",
                            obj_types[i], schemas[i], names[i]);
                    }

                    sage_upsert_finding(
                        "schema_design",
                        "info",
                        obj_types[i],
                        object_id.data,
                        title.data,
                        detail.data,
                        "Identifiers with uppercase letters or spaces require "
                        "double-quoting in SQL and are a common source of errors. "
                        "Prefer lowercase snake_case naming.",
                        NULL,
                        NULL);

                    pfree(detail.data);
                    pfree(object_id.data);
                    pfree(title.data);
                    pfree(schema_esc);
                    pfree(name_esc);
                    if (col_esc)
                        pfree(col_esc);
                    pfree(schemas[i]);
                    pfree(names[i]);
                    pfree(obj_types[i]);
                    if (col_names[i])
                        pfree(col_names[i]);
                }

                pfree(schemas);
                pfree(names);
                pfree(obj_types);
                pfree(col_names);
            }

            SPI_execute("RELEASE SAVEPOINT _sage_schema_naming", false, 0);
        }
        PG_CATCH();
        {
            FlushErrorState();
            SPI_execute("ROLLBACK TO SAVEPOINT _sage_schema_naming", false, 0);
            SPI_execute("RELEASE SAVEPOINT _sage_schema_naming", false, 0);
        }
        PG_END_TRY();
    }

    SPI_finish();
}
