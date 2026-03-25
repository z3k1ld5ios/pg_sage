/*
 * ha.c — HA/failover awareness and trust ramp for pg_sage
 *
 * Detects recovery mode, role flips, split-brain scenarios, and
 * manages the trust escalation ramp from observation to autonomous.
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include <string.h>
#include "access/xact.h"
#include "utils/snapmgr.h"

/* ----------------------------------------------------------------
 * sage_check_recovery_mode
 *
 * Query pg_is_in_recovery() and update shared state.
 * Detects role flips and enters safe mode if flapping.
 * Returns true if currently in recovery (standby).
 * ---------------------------------------------------------------- */
bool
sage_check_recovery_mode(void)
{
    bool    in_recovery = false;
    bool    prev_recovery;
    int     ret;

    if (!sage_state)
        return false;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    SPI_connect();

    PG_TRY();
    {
        ret = SPI_execute("SELECT pg_is_in_recovery()", true, 0);

        if (ret == SPI_OK_SELECT && SPI_processed > 0 && !sage_spi_isnull(0, 0))
        {
            char   *val = sage_spi_getval_str(0, 0);

            in_recovery = (val != NULL && (val[0] == 't' || val[0] == 'T'));
        }
    }
    PG_CATCH();
    {
        FlushErrorState();
        AbortCurrentTransaction();
        return false;
    }
    PG_END_TRY();

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* Update shared state and detect role flips */
    LWLockAcquire(sage_state->lock, LW_EXCLUSIVE);

    prev_recovery = sage_state->is_in_recovery;
    sage_state->is_in_recovery = in_recovery;

    if (prev_recovery != in_recovery)
    {
        sage_state->role_flip_count++;

        elog(WARNING, "pg_sage: role flip detected (%s -> %s), flip_count=%d",
             prev_recovery ? "standby" : "primary",
             in_recovery ? "standby" : "primary",
             sage_state->role_flip_count);

        /* If flapping excessively, enter safe mode (emergency stop) */
        if (sage_state->role_flip_count > 5)
        {
            sage_state->emergency_stopped = true;
            elog(WARNING, "pg_sage: excessive role flips (%d), entering emergency safe mode",
                 sage_state->role_flip_count);
        }
    }
    else
    {
        /*
         * Stable reading — decay flip counter gradually.
         * Only decrement if we've been stable for at least one check.
         */
        if (sage_state->role_flip_count > 0)
            sage_state->role_flip_count--;
    }

    LWLockRelease(sage_state->lock);

    return in_recovery;
}

/* ----------------------------------------------------------------
 * sage_is_safe_for_writes
 *
 * Returns true if pg_sage is allowed to perform write operations
 * (INSERT/UPDATE/DELETE into sage schema tables).
 * ---------------------------------------------------------------- */
bool
sage_is_safe_for_writes(void)
{
    bool    in_recovery;
    bool    stopped;
    int     flips;
    SageCircuitState circuit;

    if (!sage_state)
        return false;

    LWLockAcquire(sage_state->lock, LW_SHARED);
    in_recovery = sage_state->is_in_recovery;
    stopped     = sage_state->emergency_stopped;
    circuit     = sage_state->circuit_state;
    flips       = sage_state->role_flip_count;
    LWLockRelease(sage_state->lock);

    /* Must not be in recovery (standby) */
    if (in_recovery)
        return false;

    /* Must not be emergency stopped */
    if (stopped)
        return false;

    /* Circuit breaker must not be open or dormant */
    if (circuit != SAGE_CIRCUIT_CLOSED)
        return false;

    /* Split-brain protection: too many role flips */
    if (flips >= 5)
        return false;

    return true;
}

/* ----------------------------------------------------------------
 * sage_get_trust_level
 *
 * Determine the current trust level from the GUC string or the
 * automatic trust ramp based on installation age.
 *
 * GUC sage_trust_level_str can be:
 *   'autonomous' — explicit full trust
 *   'advisory'   — explicit advisory-only
 *   'observation' — explicit observation-only
 *   'auto' or '' — compute from trust_ramp_start date
 *
 * Auto ramp schedule:
 *   Day  0-7:  OBSERVATION
 *   Day  8-30: ADVISORY
 *   Day 31+:   AUTONOMOUS
 * ---------------------------------------------------------------- */
SageTrustLevel
sage_get_trust_level(void)
{
    int     day;

    if (sage_trust_level_str != NULL)
    {
        if (pg_strcasecmp(sage_trust_level_str, "autonomous") == 0)
            return SAGE_TRUST_AUTONOMOUS;
        if (pg_strcasecmp(sage_trust_level_str, "advisory") == 0)
            return SAGE_TRUST_ADVISORY;
        if (pg_strcasecmp(sage_trust_level_str, "observation") == 0)
            return SAGE_TRUST_OBSERVATION;
    }

    /* Auto ramp — compute from installation age */
    day = sage_get_trust_day();

    if (day <= 7)
        return SAGE_TRUST_OBSERVATION;
    else if (day <= 30)
        return SAGE_TRUST_ADVISORY;
    else
        return SAGE_TRUST_AUTONOMOUS;
}

/* ----------------------------------------------------------------
 * sage_get_trust_day
 *
 * Calculate the number of days since trust_ramp_start.
 * Returns 0 if trust_ramp_start has not been set.
 * ---------------------------------------------------------------- */
int
sage_get_trust_day(void)
{
    TimestampTz     ramp_start;
    TimestampTz     now;
    long            diff_secs;

    /* Allow test override via sage.trust_ramp_override_days */
    if (sage_trust_ramp_override_days > 0)
        return sage_trust_ramp_override_days;

    if (!sage_state)
        return 0;

    LWLockAcquire(sage_state->lock, LW_SHARED);
    ramp_start = sage_state->trust_ramp_start;
    LWLockRelease(sage_state->lock);

    /* If trust_ramp_start is zero/unset, treat as day 0 */
    if (ramp_start == 0)
        return 0;

    now = sage_now();
    diff_secs = (long)((now - ramp_start) / USECS_PER_SEC);

    if (diff_secs < 0)
        return 0;

    return (int)(diff_secs / 86400);    /* seconds per day */
}
