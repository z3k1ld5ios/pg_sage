/*
 * ddl_worker.c — Background worker for DDL execution via libpq
 *
 * Executes DDL statements (including CONCURRENTLY) outside any transaction
 * by connecting via libpq instead of SPI. Polls sage.action_log for pending
 * actions every 5 seconds.
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "libpq-fe.h"
#include <signal.h>

/* How often to poll for pending actions (seconds) */
#define DDL_WORKER_POLL_INTERVAL 5

static volatile sig_atomic_t got_sigterm = false;
static PGconn *ddl_conn = NULL;

static void
sage_ddl_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/*
 * ddl_connect — establish a libpq connection to the target database.
 */
static PGconn *
ddl_connect(void)
{
    PGconn *conn;
    char conninfo[512];

    snprintf(conninfo, sizeof(conninfo),
             "dbname=%s host=/var/run/postgresql port=5432 "
             "application_name=pg_sage_ddl_worker",
             sage_database);

    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        ereport(WARNING,
                (errmsg("pg_sage ddl_worker: connection failed: %s",
                        PQerrorMessage(conn))));
        PQfinish(conn);
        return NULL;
    }

    ereport(LOG, (errmsg("pg_sage ddl_worker: connected to %s",
                         sage_database)));
    return conn;
}

/*
 * ddl_execute_pending — fetch and execute pending DDL from action_log.
 */
static void
ddl_execute_pending(void)
{
    PGresult *res;
    int ntuples;
    int i;

    if (!ddl_conn || PQstatus(ddl_conn) != CONNECTION_OK)
    {
        if (ddl_conn)
            PQfinish(ddl_conn);
        ddl_conn = ddl_connect();
        if (!ddl_conn)
            return;
    }

    /* Find pending actions */
    res = PQexec(ddl_conn,
                 "SELECT id, sql_executed FROM sage.action_log "
                 "WHERE outcome = 'pending' "
                 "ORDER BY executed_at ASC LIMIT 5");

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        ereport(WARNING,
                (errmsg("pg_sage ddl_worker: poll query failed: %s",
                        PQerrorMessage(ddl_conn))));
        PQclear(res);
        PQfinish(ddl_conn);
        ddl_conn = NULL;
        return;
    }

    ntuples = PQntuples(res);

    if (ntuples == 0)
    {
        PQclear(res);
        return;
    }

    for (i = 0; i < ntuples; i++)
    {
        char *id_str = PQgetvalue(res, i, 0);
        char *sql = PQgetvalue(res, i, 1);
        PGresult *exec_res;
        PGresult *update_res;
        const char *params[2];

        ereport(LOG,
                (errmsg("pg_sage ddl_worker: executing action %s: %.128s",
                        id_str, sql)));

        /* Execute DDL outside any transaction — CONCURRENTLY works */
        exec_res = PQexec(ddl_conn, sql);

        if (PQresultStatus(exec_res) == PGRES_COMMAND_OK ||
            PQresultStatus(exec_res) == PGRES_TUPLES_OK)
        {
            params[0] = id_str;
            update_res = PQexecParams(ddl_conn,
                "UPDATE sage.action_log SET outcome = 'success', "
                "error_message = NULL WHERE id = $1",
                1, NULL, params, NULL, NULL, 0);

            ereport(LOG,
                    (errmsg("pg_sage ddl_worker: action %s succeeded",
                            id_str)));
            PQclear(update_res);
        }
        else
        {
            char *err_text = PQresultErrorMessage(exec_res);
            params[0] = err_text ? err_text : "unknown error";
            params[1] = id_str;

            update_res = PQexecParams(ddl_conn,
                "UPDATE sage.action_log SET outcome = 'failure', "
                "error_message = $1 WHERE id = $2",
                2, NULL, params, NULL, NULL, 0);

            ereport(WARNING,
                    (errmsg("pg_sage ddl_worker: action %s failed: %s",
                            id_str,
                            err_text ? err_text : "unknown")));
            PQclear(update_res);
        }

        PQclear(exec_res);

        if (got_sigterm)
            break;
    }

    PQclear(res);
}

/*
 * sage_ddl_worker_main — background worker entry point.
 */
void
sage_ddl_worker_main(Datum main_arg)
{
    pqsignal(SIGTERM, sage_ddl_sigterm);
    BackgroundWorkerUnblockSignals();

    ereport(LOG, (errmsg("pg_sage ddl_worker: starting")));

    ddl_conn = ddl_connect();

    while (!got_sigterm)
    {
        int rc;

        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                       DDL_WORKER_POLL_INTERVAL * 1000L,
                       PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);

        if (got_sigterm)
            break;

        if (rc & WL_TIMEOUT)
            ddl_execute_pending();
    }

    if (ddl_conn)
    {
        PQfinish(ddl_conn);
        ddl_conn = NULL;
    }

    ereport(LOG, (errmsg("pg_sage ddl_worker: shutting down")));
    proc_exit(0);
}
