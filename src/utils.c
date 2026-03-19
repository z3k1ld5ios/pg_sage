/*
 * utils.c — Shared utility functions for pg_sage
 *
 * SPI helpers, parsing, JSON escaping, timestamp helpers.
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include <stdarg.h>

/* forward declaration for internal helper */
static void append_json_escaped(StringInfo buf, const char *str);

/* ----------------------------------------------------------------
 * sage_spi_exec
 *
 * Execute SQL via SPI and return the processed row count.
 * Returns -1 on failure (logs WARNING).
 * ---------------------------------------------------------------- */
int
sage_spi_exec(const char *sql, int expected)
{
    int ret;

    ret = SPI_execute(sql, (expected == SPI_OK_SELECT), 0);
    if (expected == 0)
    {
        /* Caller doesn't care about specific return code — accept any success */
        if (ret < 0)
        {
            elog(WARNING, "pg_sage: SPI_execute failed: %.128s (returned %d)",
                 sql, ret);
            return -1;
        }
    }
    else if (ret != expected)
    {
        elog(WARNING, "pg_sage: SPI_execute unexpected result: %.128s "
             "(expected %d, got %d)", sql, expected, ret);
        return -1;
    }
    return (int) SPI_processed;
}

/* ----------------------------------------------------------------
 * sage_spi_getval_str
 *
 * Get a text value from the current SPI result set as a palloc'd
 * C string.  Returns NULL if the tuple table is empty, the row is
 * out of range, or the column value is SQL NULL.
 *
 * col is 0-based; internally converted to 1-based for SPI.
 * ---------------------------------------------------------------- */
char *
sage_spi_getval_str(int row, int col)
{
    char   *val;

    if (SPI_tuptable == NULL || row >= (int) SPI_processed)
        return NULL;

    /* Use SPI_getvalue which handles any type via output function */
    val = SPI_getvalue(SPI_tuptable->vals[row],
                       SPI_tuptable->tupdesc,
                       col + 1);

    return val;  /* NULL if SQL NULL; palloc'd in SPI context */
}

/* ----------------------------------------------------------------
 * sage_spi_getval_int64
 *
 * Get an int64 value from the current SPI result set.
 * Returns 0 on NULL or out-of-range.
 * ---------------------------------------------------------------- */
int64
sage_spi_getval_int64(int row, int col)
{
    bool    isnull;
    Datum   val;

    if (SPI_tuptable == NULL || row >= (int) SPI_processed)
        return 0;

    val = SPI_getbinval(SPI_tuptable->vals[row],
                        SPI_tuptable->tupdesc,
                        col + 1,
                        &isnull);
    if (isnull)
        return 0;

    return DatumGetInt64(val);
}

/* ----------------------------------------------------------------
 * sage_spi_getval_float
 *
 * Get a float8 value from the current SPI result set.
 * Returns 0.0 on NULL or out-of-range.
 * ---------------------------------------------------------------- */
double
sage_spi_getval_float(int row, int col)
{
    bool    isnull;
    Datum   val;

    if (SPI_tuptable == NULL || row >= (int) SPI_processed)
        return 0.0;

    val = SPI_getbinval(SPI_tuptable->vals[row],
                        SPI_tuptable->tupdesc,
                        col + 1,
                        &isnull);
    if (isnull)
        return 0.0;

    return DatumGetFloat8(val);
}

/* ----------------------------------------------------------------
 * sage_spi_isnull
 *
 * Check whether a column in the current SPI result is NULL.
 * Returns true if NULL, tuptable is missing, or row out of range.
 * ---------------------------------------------------------------- */
bool
sage_spi_isnull(int row, int col)
{
    bool    isnull;

    if (SPI_tuptable == NULL || row >= (int) SPI_processed)
        return true;

    (void) SPI_getbinval(SPI_tuptable->vals[row],
                         SPI_tuptable->tupdesc,
                         col + 1,
                         &isnull);
    return isnull;
}

/* ----------------------------------------------------------------
 * sage_parse_interval_days
 *
 * Parse simple interval strings into integer days.
 * Supported formats: "7d", "30d", "90d", "7 days", "30 days".
 * Returns 30 as a safe default if parsing fails.
 * ---------------------------------------------------------------- */
int
sage_parse_interval_days(const char *interval_str)
{
    int days = 0;

    if (interval_str == NULL || interval_str[0] == '\0')
        return 30;

    /* Try "Nd" format */
    if (sscanf(interval_str, "%dd", &days) == 1 && days > 0)
        return days;

    /* Try "N days" format */
    if (sscanf(interval_str, "%d days", &days) == 1 && days > 0)
        return days;

    /* Try plain integer */
    if (sscanf(interval_str, "%d", &days) == 1 && days > 0)
        return days;

    return 30; /* default fallback */
}

/* ----------------------------------------------------------------
 * sage_now
 *
 * Return the current timestamp (TimestampTz).
 * ---------------------------------------------------------------- */
TimestampTz
sage_now(void)
{
    return GetCurrentTimestamp();
}

/* ----------------------------------------------------------------
 * append_json_escaped  (static helper)
 *
 * Append str to buf with JSON-required escaping applied.
 * ---------------------------------------------------------------- */
static void
append_json_escaped(StringInfo buf, const char *str)
{
    const char *p;

    for (p = str; *p != '\0'; p++)
    {
        switch (*p)
        {
            case '"':
                appendStringInfoString(buf, "\\\"");
                break;
            case '\\':
                appendStringInfoString(buf, "\\\\");
                break;
            case '\n':
                appendStringInfoString(buf, "\\n");
                break;
            case '\r':
                appendStringInfoString(buf, "\\r");
                break;
            case '\t':
                appendStringInfoString(buf, "\\t");
                break;
            case '\b':
                appendStringInfoString(buf, "\\b");
                break;
            case '\f':
                appendStringInfoString(buf, "\\f");
                break;
            default:
                /* Escape control characters as \u00XX */
                if ((unsigned char) *p < 0x20)
                {
                    appendStringInfo(buf, "\\u%04x", (unsigned int)(unsigned char) *p);
                }
                else
                {
                    appendStringInfoChar(buf, *p);
                }
                break;
        }
    }
}

/* ----------------------------------------------------------------
 * sage_escape_json_string
 *
 * Return a palloc'd copy of str with JSON special characters
 * escaped (backslash, double-quote, newline, tab, etc.).
 *
 * The caller is responsible for pfree'ing the result.
 * ---------------------------------------------------------------- */
char *
sage_escape_json_string(const char *str)
{
    StringInfoData buf;

    if (str == NULL)
        return pstrdup("");

    initStringInfo(&buf);
    append_json_escaped(&buf, str);

    return buf.data;
}

/* ----------------------------------------------------------------
 * sage_format_jsonb_object
 *
 * Build a JSONB-compatible object string from key/value pairs.
 *
 * Accepts a variable number of (const char *key, const char *value)
 * pairs.  The list MUST be terminated with a NULL key.  Values may
 * be NULL, in which case the JSON null literal is emitted.
 *
 * All string values are properly JSON-escaped.
 *
 * Example:
 *   sage_format_jsonb_object(
 *       "table", "public.users",
 *       "rows",  "12345",
 *       "note",  NULL,
 *       NULL);
 *   => {"table": "public.users", "rows": "12345", "note": null}
 *
 * Returns a palloc'd string.  Caller must pfree.
 * ---------------------------------------------------------------- */
char *
sage_format_jsonb_object(const char *first_key, ...)
{
    StringInfoData  buf;
    va_list         args;
    const char     *key;
    const char     *value;
    bool            first = true;

    initStringInfo(&buf);
    appendStringInfoChar(&buf, '{');

    if (first_key == NULL)
    {
        appendStringInfoChar(&buf, '}');
        return buf.data;
    }

    va_start(args, first_key);

    key = first_key;
    while (key != NULL)
    {
        value = va_arg(args, const char *);

        if (!first)
            appendStringInfoString(&buf, ", ");
        first = false;

        /* Key — always a string */
        appendStringInfoChar(&buf, '"');
        append_json_escaped(&buf, key);
        appendStringInfoString(&buf, "\": ");

        /* Value — NULL becomes JSON null, otherwise quoted string */
        if (value == NULL)
        {
            appendStringInfoString(&buf, "null");
        }
        else
        {
            appendStringInfoChar(&buf, '"');
            append_json_escaped(&buf, value);
            appendStringInfoChar(&buf, '"');
        }

        key = va_arg(args, const char *);
    }

    va_end(args);

    appendStringInfoChar(&buf, '}');
    return buf.data;
}
