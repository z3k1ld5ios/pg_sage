/*
 * circuit_breaker.c — Observer-effect protection for pg_sage
 *
 * pg_sage must never become the incident. The circuit breaker monitors
 * system load, disk pressure, and its own health to automatically back
 * off when the database is under stress.
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include <math.h>
#include <time.h>
#include "access/xact.h"
#include "utils/snapmgr.h"

#ifdef __linux__
#include <sys/statvfs.h>
#endif

/* ------------------------------------------------------------
 * Internal helpers
 * ------------------------------------------------------------ */

/*
 * Parse /proc/stat to estimate CPU utilization.
 * Returns a value in [0.0, 100.0], or -1.0 if unavailable.
 *
 * We read two samples 100ms apart and compute delta.
 * On non-Linux this simply returns -1.
 */
static double
sage_read_cpu_load(void)
{
#ifdef __linux__
    FILE       *fp;
    long long   user1, nice1, sys1, idle1, iowait1, irq1, softirq1, steal1;
    long long   user2, nice2, sys2, idle2, iowait2, irq2, softirq2, steal2;
    long long   total1, total2, idle_total1, idle_total2;

    fp = fopen("/proc/stat", "r");
    if (!fp)
        return -1.0;

    if (fscanf(fp, "cpu %lld %lld %lld %lld %lld %lld %lld %lld",
               &user1, &nice1, &sys1, &idle1,
               &iowait1, &irq1, &softirq1, &steal1) != 8)
    {
        fclose(fp);
        return -1.0;
    }
    fclose(fp);

    /* Brief sleep for delta measurement */
    pg_usleep(100000);  /* 100ms */

    fp = fopen("/proc/stat", "r");
    if (!fp)
        return -1.0;

    if (fscanf(fp, "cpu %lld %lld %lld %lld %lld %lld %lld %lld",
               &user2, &nice2, &sys2, &idle2,
               &iowait2, &irq2, &softirq2, &steal2) != 8)
    {
        fclose(fp);
        return -1.0;
    }
    fclose(fp);

    idle_total1 = idle1 + iowait1;
    idle_total2 = idle2 + iowait2;

    total1 = user1 + nice1 + sys1 + idle1 + iowait1 + irq1 + softirq1 + steal1;
    total2 = user2 + nice2 + sys2 + idle2 + iowait2 + irq2 + softirq2 + steal2;

    if ((total2 - total1) == 0)
        return 0.0;

    return 100.0 * (1.0 - ((double)(idle_total2 - idle_total1) /
                            (double)(total2 - total1)));
#else
    return -1.0;
#endif
}

/*
 * Estimate CPU pressure using active backends / max_connections as a proxy.
 * Returns a ratio in [0.0, 1.0].
 */
static double
sage_backend_load_ratio(void)
{
    int             ret;
    double          ratio = 0.0;
    volatile bool   err = false;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_connect();

    PG_TRY();
    {
        ret = SPI_execute(
            "SELECT count(*) FILTER (WHERE state = 'active')::float / "
            "       greatest(current_setting('max_connections')::int, 1) "
            "FROM pg_stat_activity",
            true, 0);

        if (ret == SPI_OK_SELECT && SPI_processed > 0 && !sage_spi_isnull(0, 0))
            ratio = sage_spi_getval_float(0, 0);
    }
    PG_CATCH();
    {
        FlushErrorState();
        err = true;
        ratio = 0.0;
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
    return ratio;
}

/*
 * Check disk pressure by querying default tablespace size and comparing
 * with sage_disk_pressure_threshold.
 *
 * Returns true if disk pressure is too high (should back off).
 */
static bool
sage_disk_pressure_exceeded(void)
{
    int             ret;
    bool            exceeded = false;
    volatile bool   err = false;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_connect();

    PG_TRY();
    {
        ret = SPI_execute(
            "SELECT pg_tablespace_size('pg_default') AS ts_size",
            true, 0);

        if (ret == SPI_OK_SELECT && SPI_processed > 0 && !sage_spi_isnull(0, 0))
        {
            int64   ts_size = sage_spi_getval_int64(0, 0);

#ifdef __linux__
            {
                struct statvfs  svfs;
                const char     *datadir = GetConfigOptionByName("data_directory", NULL, false);

                if (datadir && statvfs(datadir, &svfs) == 0)
                {
                    uint64  total_bytes = (uint64)svfs.f_blocks * svfs.f_frsize;
                    uint64  free_bytes  = (uint64)svfs.f_bavail * svfs.f_frsize;
                    double  free_pct    = 0.0;

                    if (total_bytes > 0)
                        free_pct = 100.0 * ((double)free_bytes / (double)total_bytes);

                    if (free_pct < (double)sage_disk_pressure_threshold)
                        exceeded = true;
                }
            }
#else
            (void) ts_size;
#endif
        }
    }
    PG_CATCH();
    {
        FlushErrorState();
        err = true;
        exceeded = false;
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
    return exceeded;
}

/* ----------------------------------------------------------------
 * sage_circuit_check
 *
 * Returns true if it is safe for pg_sage to proceed with work,
 * false if the circuit is tripped and we should back off.
 * ---------------------------------------------------------------- */
bool
sage_circuit_check(void)
{
    SageCircuitState    state;
    bool                stopped;
    TimestampTz         last_collect;
    int                 skips;
    TimestampTz         now;
    double              cpu;

    if (!sage_state)
        return false;

    now = sage_now();

    /* Step 1: Read shared state under lock */
    LWLockAcquire(sage_state->lock, LW_SHARED);
    stopped      = sage_state->emergency_stopped;
    state        = sage_state->circuit_state;
    skips        = sage_state->consecutive_skips;
    last_collect = sage_state->last_collect_time;
    LWLockRelease(sage_state->lock);

    /* Step 2: Emergency stop is absolute */
    if (stopped)
        return false;

    /* Step 3: OPEN circuit — exponential backoff with cap */
    if (state == SAGE_CIRCUIT_OPEN)
    {
        long    cooldown_secs;
        long    elapsed_secs;

        /* 30s * consecutive_skips, capped at 600s */
        cooldown_secs = 30L * (skips > 0 ? skips : 1);
        if (cooldown_secs > 600)
            cooldown_secs = 600;

        elapsed_secs = (long)((now - last_collect) / USECS_PER_SEC);

        if (elapsed_secs >= cooldown_secs)
        {
            /* Cooldown expired — transition to CLOSED */
            LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);
            sage_state->circuit_state = SAGE_CIRCUIT_CLOSED;
            sage_state->last_circuit_change = now;
            LWLockRelease(sage_state->lock);

            elog(LOG, "pg_sage: circuit breaker transitioning OPEN -> CLOSED after %ld s cooldown",
                 cooldown_secs);
            return true;
        }

        return false;
    }

    /* Step 4: DORMANT — only 1 collection per 10 minutes */
    if (state == SAGE_CIRCUIT_DORMANT)
    {
        long    elapsed_secs;

        elapsed_secs = (long)((now - last_collect) / USECS_PER_SEC);

        if (elapsed_secs < 600)     /* 10 minutes */
            return false;

        /* Allow one attempt */
        return true;
    }

    /* Steps 5-6 only apply in CLOSED state */

    /* Step 5: CPU load check */
    cpu = sage_read_cpu_load();

    if (cpu >= 0.0)
    {
        /* Direct measurement available */
        if (cpu > 90.0)
        {
            elog(LOG, "pg_sage: CPU utilization %.1f%% exceeds 90%%, skipping cycle", cpu);
            return false;
        }
    }
    else
    {
        /* Fallback: active backends ratio */
        double ratio = sage_backend_load_ratio();

        if (ratio > 0.9)
        {
            elog(LOG, "pg_sage: active backends ratio %.2f exceeds 0.9, skipping cycle", ratio);
            return false;
        }
    }

    /* Step 6: Disk pressure */
    if (sage_disk_pressure_exceeded())
    {
        elog(LOG, "pg_sage: disk pressure exceeds threshold (%d%% free required), suspending writes",
             sage_disk_pressure_threshold);
        return false;
    }

    /* Step 7: All clear */
    return true;
}

/* ----------------------------------------------------------------
 * sage_circuit_record_success
 *
 * Record a successful collection cycle.
 * ---------------------------------------------------------------- */
void
sage_circuit_record_success(void)
{
    SageCircuitState    old_state;

    if (!sage_state)
        return;

    LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);

    old_state = sage_state->circuit_state;

    sage_state->consecutive_skips = 0;
    sage_state->consecutive_successes++;

    if (old_state == SAGE_CIRCUIT_DORMANT &&
        sage_state->consecutive_successes >= 3)
    {
        sage_state->circuit_state = SAGE_CIRCUIT_CLOSED;
        sage_state->last_circuit_change = sage_now();
    }
    else if (old_state == SAGE_CIRCUIT_OPEN)
    {
        sage_state->circuit_state = SAGE_CIRCUIT_CLOSED;
        sage_state->last_circuit_change = sage_now();
    }

    LWLockRelease(sage_state->lock);

    if (old_state != SAGE_CIRCUIT_CLOSED)
    {
        SageCircuitState new_state;

        LWLockAcquire(sage_state->lock, LW_SHARED);
        new_state = sage_state->circuit_state;
        LWLockRelease(sage_state->lock);

        if (new_state == SAGE_CIRCUIT_CLOSED)
            elog(LOG, "pg_sage: circuit breaker recovered to CLOSED from %s",
                 old_state == SAGE_CIRCUIT_OPEN ? "OPEN" : "DORMANT");
    }
}

/* ----------------------------------------------------------------
 * sage_circuit_record_skip
 *
 * Record a skipped collection cycle (circuit engaged or error).
 * ---------------------------------------------------------------- */
void
sage_circuit_record_skip(void)
{
    SageCircuitState    old_state;
    SageCircuitState    new_state;

    if (!sage_state)
        return;

    LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);

    old_state = sage_state->circuit_state;
    sage_state->consecutive_successes = 0;
    sage_state->consecutive_skips++;

    new_state = old_state;

    if (sage_state->consecutive_skips >= 6 && old_state == SAGE_CIRCUIT_OPEN)
    {
        sage_state->circuit_state = SAGE_CIRCUIT_DORMANT;
        sage_state->last_circuit_change = sage_now();
        new_state = SAGE_CIRCUIT_DORMANT;
    }
    else if (sage_state->consecutive_skips >= 3 && old_state == SAGE_CIRCUIT_CLOSED)
    {
        sage_state->circuit_state = SAGE_CIRCUIT_OPEN;
        sage_state->last_circuit_change = sage_now();
        new_state = SAGE_CIRCUIT_OPEN;
    }

    LWLockRelease(sage_state->lock);

    if (old_state != new_state)
    {
        const char *old_name, *new_name;
        int         cur_skips;

        LWLockAcquire(sage_state->lock, LW_SHARED);
        cur_skips = sage_state->consecutive_skips;
        LWLockRelease(sage_state->lock);

        switch (old_state)
        {
            case SAGE_CIRCUIT_CLOSED:  old_name = "CLOSED";  break;
            case SAGE_CIRCUIT_OPEN:    old_name = "OPEN";    break;
            case SAGE_CIRCUIT_DORMANT: old_name = "DORMANT"; break;
            default:                   old_name = "UNKNOWN"; break;
        }
        switch (new_state)
        {
            case SAGE_CIRCUIT_CLOSED:  new_name = "CLOSED";  break;
            case SAGE_CIRCUIT_OPEN:    new_name = "OPEN";    break;
            case SAGE_CIRCUIT_DORMANT: new_name = "DORMANT"; break;
            default:                   new_name = "UNKNOWN"; break;
        }

        elog(WARNING, "pg_sage: circuit breaker state %s -> %s (consecutive_skips=%d)",
             old_name, new_name, cur_skips);
    }
}

/* ----------------------------------------------------------------
 * sage_llm_circuit_check
 *
 * Returns true if LLM calls are currently allowed.
 * ---------------------------------------------------------------- */
bool
sage_llm_circuit_check(void)
{
    SageCircuitState    llm_state;
    TimestampTz         opened_at;
    int                 tokens_today;
    int                 day_of_year;
    TimestampTz         now;
    long                elapsed_secs;
    struct pg_tm        tm;
    fsec_t              fsec;
    int                 current_doy;

    if (!sage_state)
        return false;

    now = sage_now();

    /* Get current day-of-year for budget reset */
    timestamp2tm(now, NULL, &tm, &fsec, NULL, NULL);
    current_doy = tm.tm_yday;

    LWLockAcquire(sage_state->lock, LW_SHARED);
    llm_state    = sage_state->llm_circuit_state;
    opened_at    = sage_state->llm_circuit_opened_at;
    tokens_today = sage_state->llm_tokens_used_today;
    day_of_year  = sage_state->llm_day_of_year;
    LWLockRelease(sage_state->lock);

    /* Reset daily token counter on new day */
    if (current_doy != day_of_year)
    {
        LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);
        if (sage_state->llm_day_of_year != current_doy)
        {
            sage_state->llm_tokens_used_today = 0;
            sage_state->llm_day_of_year = current_doy;
            tokens_today = 0;
        }
        LWLockRelease(sage_state->lock);
    }

    /* Check if circuit is OPEN */
    if (llm_state == SAGE_CIRCUIT_OPEN)
    {
        elapsed_secs = (long)((now - opened_at) / USECS_PER_SEC);

        if (elapsed_secs < sage_llm_cooldown)
        {
            elog(DEBUG1, "pg_sage: LLM circuit OPEN, %ld s remaining in cooldown",
                 sage_llm_cooldown - elapsed_secs);
            return false;
        }

        /* Cooldown expired, reset to CLOSED */
        LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);
        sage_state->llm_circuit_state = SAGE_CIRCUIT_CLOSED;
        sage_state->llm_consecutive_failures = 0;
        LWLockRelease(sage_state->lock);

        elog(LOG, "pg_sage: LLM circuit breaker reset to CLOSED after cooldown");
    }

    /* Check daily token budget */
    if (tokens_today >= sage_llm_token_budget)
    {
        elog(DEBUG1, "pg_sage: LLM daily token budget exhausted (%d/%d)",
             tokens_today, sage_llm_token_budget);
        return false;
    }

    return true;
}

/* ----------------------------------------------------------------
 * sage_llm_circuit_record_failure
 *
 * Record a failed LLM call. Opens circuit after 3 consecutive failures.
 * ---------------------------------------------------------------- */
void
sage_llm_circuit_record_failure(void)
{
    if (!sage_state)
        return;

    LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);

    sage_state->llm_consecutive_failures++;

    if (sage_state->llm_consecutive_failures >= 3 &&
        sage_state->llm_circuit_state != SAGE_CIRCUIT_OPEN)
    {
        sage_state->llm_circuit_state = SAGE_CIRCUIT_OPEN;
        sage_state->llm_circuit_opened_at = sage_now();

        elog(WARNING, "pg_sage: LLM circuit breaker OPENED after %d consecutive failures",
             sage_state->llm_consecutive_failures);
    }

    LWLockRelease(sage_state->lock);
}

/* ----------------------------------------------------------------
 * sage_llm_circuit_record_success
 *
 * Record a successful LLM call. Resets failure counter.
 * ---------------------------------------------------------------- */
void
sage_llm_circuit_record_success(void)
{
    if (!sage_state)
        return;

    LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);

    sage_state->llm_consecutive_failures = 0;

    if (sage_state->llm_circuit_state != SAGE_CIRCUIT_CLOSED)
    {
        sage_state->llm_circuit_state = SAGE_CIRCUIT_CLOSED;
        elog(LOG, "pg_sage: LLM circuit breaker reset to CLOSED on success");
    }

    LWLockRelease(sage_state->lock);
}
