/*
 * analyzer_extra.c — Additional analyzer rules for pg_sage
 *
 * Implements Tier 1.2 (vacuum/bloat), Tier 1.8 (security), and
 * Tier 1.9 (replication health) analysis functions.
 *
 * AGPL-3.0 License
 */

#include "pg_sage.h"

#include <math.h>
#include <string.h>

#include "access/xact.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/* ================================================================
 * 1. sage_analyze_vacuum_bloat  (Tier 1.2)
 * ================================================================ */
void
sage_analyze_vacuum_bloat(void)
{
	int		ret;
	int		i;

	SPI_connect();
	sage_spi_exec("SET LOCAL statement_timeout = '500ms'", 0);

	/* ----- Dead tuple ratio check ----- */
	ret = sage_spi_exec(
		"SELECT schemaname, relname, n_live_tup, n_dead_tup, "
		"       last_autovacuum, last_vacuum "
		"FROM pg_stat_user_tables "
		"WHERE n_live_tup > 0 "
		"ORDER BY n_dead_tup DESC "
		"LIMIT 200",
		0);

	if (ret >= 0 && SPI_processed > 0)
	{
		int nrows = (int) SPI_processed;

		/* Copy results before further SPI calls */
		typedef struct
		{
			char   *schemaname;
			char   *relname;
			int64	n_live_tup;
			int64	n_dead_tup;
			bool	has_autovacuum;
			char   *last_autovacuum;
			bool	has_vacuum;
			char   *last_vacuum;
		} TblInfo;

		TblInfo *tables = palloc(sizeof(TblInfo) * nrows);

		for (i = 0; i < nrows; i++)
		{
			tables[i].schemaname = pstrdup(sage_spi_getval_str(i, 0));
			tables[i].relname    = pstrdup(sage_spi_getval_str(i, 1));
			tables[i].n_live_tup = sage_spi_getval_int64(i, 2);
			tables[i].n_dead_tup = sage_spi_getval_int64(i, 3);
			tables[i].has_autovacuum = !sage_spi_isnull(i, 4);
			tables[i].last_autovacuum = tables[i].has_autovacuum
				? pstrdup(sage_spi_getval_str(i, 4)) : NULL;
			tables[i].has_vacuum = !sage_spi_isnull(i, 5);
			tables[i].last_vacuum = tables[i].has_vacuum
				? pstrdup(sage_spi_getval_str(i, 5)) : NULL;
		}

		for (i = 0; i < nrows; i++)
		{
			double ratio;
			const char *severity;
			StringInfoData detail;
			StringInfoData object_id;
			StringInfoData title_buf;
			StringInfoData rec_sql;
			StringInfoData rollback_sql;

			CHECK_FOR_INTERRUPTS();

			if (tables[i].n_live_tup == 0)
				continue;

			ratio = (double) tables[i].n_dead_tup / (double) tables[i].n_live_tup * 100.0;

			if (ratio <= 10.0)
			{
				/* Resolve any previous finding for this table */
				char *oid = psprintf("%s.%s", tables[i].schemaname, tables[i].relname);
				sage_resolve_finding("vacuum_bloat_dead_tuples", oid);
				pfree(oid);
				continue;
			}

			severity = (ratio > 30.0) ? "critical" : "warning";

			initStringInfo(&detail);
			initStringInfo(&object_id);
			initStringInfo(&title_buf);
			initStringInfo(&rec_sql);
			initStringInfo(&rollback_sql);

			appendStringInfo(&object_id, "%s.%s",
							 tables[i].schemaname, tables[i].relname);

			appendStringInfo(&title_buf,
				"High dead tuple ratio (%.1f%%) on %s.%s",
				ratio, tables[i].schemaname, tables[i].relname);

			{
				char *esc_schema = sage_escape_json_string(tables[i].schemaname);
				char *esc_rel    = sage_escape_json_string(tables[i].relname);

				appendStringInfo(&detail,
					"{\"dead_tup_ratio\": %.1f, \"table\": \"%s.%s\", "
					"\"n_live_tup\": " INT64_FORMAT ", \"n_dead_tup\": " INT64_FORMAT "}",
					ratio, esc_schema, esc_rel,
					tables[i].n_live_tup, tables[i].n_dead_tup);
				pfree(esc_schema);
				pfree(esc_rel);
			}

			appendStringInfo(&rec_sql,
				"ALTER TABLE %s.%s SET (autovacuum_vacuum_scale_factor = 0.05, "
				"autovacuum_vacuum_threshold = 50);",
				tables[i].schemaname, tables[i].relname);

			appendStringInfo(&rollback_sql,
				"ALTER TABLE %s.%s RESET (autovacuum_vacuum_scale_factor, "
				"autovacuum_vacuum_threshold);",
				tables[i].schemaname, tables[i].relname);

			sage_upsert_finding(
				"vacuum_bloat_dead_tuples", severity,
				"table", object_id.data,
				title_buf.data, detail.data,
				"Table has a high ratio of dead tuples to live tuples. "
				"Consider tuning autovacuum settings for this table or "
				"running VACUUM manually.",
				rec_sql.data, rollback_sql.data);

			pfree(detail.data);
			pfree(object_id.data);
			pfree(title_buf.data);
			pfree(rec_sql.data);
			pfree(rollback_sql.data);
		}

		/* Check autovacuum staleness: tables not vacuumed in 7+ days
		 * with significant dead tuples */
		for (i = 0; i < nrows; i++)
		{
			StringInfoData detail;
			StringInfoData object_id;
			StringInfoData title_buf;
			char *oid;

			CHECK_FOR_INTERRUPTS();

			/* Only flag if dead tuples > 1000 */
			if (tables[i].n_dead_tup < 1000)
				continue;

			/*
			 * We already have last_autovacuum/last_vacuum as strings.
			 * Check staleness with a query.
			 */
			{
				StringInfoData check_sql;
				int check_ret;
				bool stale = false;

				initStringInfo(&check_sql);
				appendStringInfo(&check_sql,
					"SELECT 1 FROM pg_stat_user_tables "
					"WHERE schemaname = '%s' AND relname = '%s' "
					"  AND COALESCE(last_autovacuum, last_vacuum, '1970-01-01'::timestamptz) "
					"      < now() - interval '7 days' "
					"  AND n_dead_tup >= 1000",
					tables[i].schemaname, tables[i].relname);

				check_ret = sage_spi_exec(check_sql.data, 0);
				pfree(check_sql.data);

				if (check_ret >= 0 && SPI_processed > 0)
					stale = true;

				oid = psprintf("%s.%s", tables[i].schemaname, tables[i].relname);

				if (!stale)
				{
					sage_resolve_finding("vacuum_staleness", oid);
					pfree(oid);
					continue;
				}

				initStringInfo(&detail);
				initStringInfo(&object_id);
				initStringInfo(&title_buf);

				appendStringInfoString(&object_id, oid);

				appendStringInfo(&title_buf,
					"Autovacuum stale on %s (>7 days, " INT64_FORMAT " dead tuples)",
					oid, tables[i].n_dead_tup);

				{
					char *esc_tbl = sage_escape_json_string(oid);
					appendStringInfo(&detail,
						"{\"table\": \"%s\", \"n_dead_tup\": " INT64_FORMAT ", "
						"\"days_since_vacuum\": \"7+\"}",
						esc_tbl, tables[i].n_dead_tup);
					pfree(esc_tbl);
				}

				sage_upsert_finding(
					"vacuum_staleness", "warning",
					"table", object_id.data,
					title_buf.data, detail.data,
					"This table has not been vacuumed in over 7 days and has "
					"significant dead tuples. Autovacuum may need tuning or "
					"a manual VACUUM may be needed.",
					psprintf("VACUUM ANALYZE %s;", oid),
					NULL);

				pfree(detail.data);
				pfree(object_id.data);
				pfree(title_buf.data);
				pfree(oid);
			}
		}

		/* Free table info */
		for (i = 0; i < nrows; i++)
		{
			pfree(tables[i].schemaname);
			pfree(tables[i].relname);
			if (tables[i].last_autovacuum)
				pfree(tables[i].last_autovacuum);
			if (tables[i].last_vacuum)
				pfree(tables[i].last_vacuum);
		}
		pfree(tables);
	}

	/* ----- XID wraparound check ----- */
	ret = sage_spi_exec(
		"SELECT datname, age(datfrozenxid) FROM pg_database "
		"WHERE datname = current_database()",
		0);

	if (ret >= 0 && SPI_processed > 0)
	{
		char   *datname = pstrdup(sage_spi_getval_str(0, 0));
		int64	xid_age = sage_spi_getval_int64(0, 1);

		if (xid_age > 500000000)
		{
			const char *severity = (xid_age > 1000000000) ? "critical" : "warning";
			StringInfoData detail;
			StringInfoData title_buf;
			char *esc_db = sage_escape_json_string(datname);

			initStringInfo(&detail);
			initStringInfo(&title_buf);

			appendStringInfo(&title_buf,
				"XID wraparound risk: age " INT64_FORMAT " on database %s",
				xid_age, datname);

			appendStringInfo(&detail,
				"{\"database\": \"%s\", \"xid_age\": " INT64_FORMAT ", "
				"\"threshold_warning\": 500000000, \"threshold_critical\": 1000000000}",
				esc_db, xid_age);

			sage_upsert_finding(
				"xid_wraparound", severity,
				"database", datname,
				title_buf.data, detail.data,
				"Transaction ID wraparound is approaching dangerous levels. "
				"Aggressive VACUUM FREEZE is recommended to prevent database "
				"shutdown.",
				"VACUUM FREEZE;",
				NULL);

			pfree(detail.data);
			pfree(title_buf.data);
			pfree(esc_db);
		}
		else
		{
			sage_resolve_finding("xid_wraparound", datname);
		}

		pfree(datname);
	}

	/* ----- Toast table bloat check ----- */
	ret = sage_spi_exec(
		"SELECT c.relname, n.nspname, "
		"       pg_total_relation_size(c.reltoastrelid) as toast_bytes, "
		"       pg_total_relation_size(c.oid) as total_bytes "
		"FROM pg_class c "
		"JOIN pg_namespace n ON n.oid = c.relnamespace "
		"WHERE c.reltoastrelid != 0 AND c.relkind = 'r' "
		"  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sage') "
		"  AND pg_total_relation_size(c.oid) > 0 "
		"ORDER BY pg_total_relation_size(c.reltoastrelid) DESC "
		"LIMIT 100",
		0);

	if (ret >= 0 && SPI_processed > 0)
	{
		int nrows = (int) SPI_processed;

		/* Copy results before issuing further SPI calls */
		typedef struct
		{
			char   *relname;
			char   *nspname;
			int64	toast_bytes;
			int64	total_bytes;
		} ToastInfo;

		ToastInfo *toasts = palloc(sizeof(ToastInfo) * nrows);

		for (i = 0; i < nrows; i++)
		{
			toasts[i].relname     = pstrdup(sage_spi_getval_str(i, 0));
			toasts[i].nspname     = pstrdup(sage_spi_getval_str(i, 1));
			toasts[i].toast_bytes = sage_spi_getval_int64(i, 2);
			toasts[i].total_bytes = sage_spi_getval_int64(i, 3);
		}

		for (i = 0; i < nrows; i++)
		{
			double toast_pct;
			StringInfoData detail;
			StringInfoData object_id;
			StringInfoData title_buf;
			char *oid;

			CHECK_FOR_INTERRUPTS();

			if (toasts[i].total_bytes == 0)
				continue;

			toast_pct = (double) toasts[i].toast_bytes / (double) toasts[i].total_bytes * 100.0;

			oid = psprintf("%s.%s", toasts[i].nspname, toasts[i].relname);

			if (toast_pct <= 50.0)
			{
				sage_resolve_finding("toast_bloat", oid);
				pfree(oid);
				continue;
			}

			initStringInfo(&detail);
			initStringInfo(&object_id);
			initStringInfo(&title_buf);

			appendStringInfoString(&object_id, oid);

			appendStringInfo(&title_buf,
				"Toast bloat: %.0f%% of %s is toast data",
				toast_pct, oid);

			{
				char *esc_tbl = sage_escape_json_string(oid);
				appendStringInfo(&detail,
					"{\"table\": \"%s\", \"toast_bytes\": " INT64_FORMAT ", "
					"\"total_bytes\": " INT64_FORMAT ", \"toast_pct\": %.1f}",
					esc_tbl, toasts[i].toast_bytes,
					toasts[i].total_bytes, toast_pct);
				pfree(esc_tbl);
			}

			sage_upsert_finding(
				"toast_bloat", "warning",
				"table", object_id.data,
				title_buf.data, detail.data,
				"The TOAST portion of this table accounts for more than 50% "
				"of total table size. Consider VACUUM FULL or reviewing column "
				"storage strategies (EXTERNAL vs EXTENDED).",
				psprintf("VACUUM FULL %s;", oid),
				NULL);

			pfree(detail.data);
			pfree(object_id.data);
			pfree(title_buf.data);
			pfree(oid);
		}

		for (i = 0; i < nrows; i++)
		{
			pfree(toasts[i].relname);
			pfree(toasts[i].nspname);
		}
		pfree(toasts);
	}

	SPI_finish();
}

/* ================================================================
 * 2. sage_analyze_security  (Tier 1.8)
 * ================================================================ */
void
sage_analyze_security(void)
{
	int		ret;
	int		i;

	SPI_connect();
	sage_spi_exec("SET LOCAL statement_timeout = '500ms'", 0);

	/* ----- Overprivileged superuser roles ----- */
	ret = sage_spi_exec(
		"SELECT rolname FROM pg_roles "
		"WHERE rolsuper = true "
		"  AND rolname NOT IN ('postgres', 'rdsadmin', 'cloudsqlsuperuser')",
		0);

	if (ret >= 0 && SPI_processed > 0)
	{
		int nrows = (int) SPI_processed;
		char **rolenames = palloc(sizeof(char *) * nrows);

		for (i = 0; i < nrows; i++)
			rolenames[i] = pstrdup(sage_spi_getval_str(i, 0));

		for (i = 0; i < nrows; i++)
		{
			StringInfoData detail;
			StringInfoData title_buf;
			char *esc_role;

			CHECK_FOR_INTERRUPTS();

			initStringInfo(&detail);
			initStringInfo(&title_buf);

			esc_role = sage_escape_json_string(rolenames[i]);

			appendStringInfo(&title_buf,
				"Non-default superuser role: %s", rolenames[i]);

			appendStringInfo(&detail,
				"{\"role\": \"%s\", \"privilege\": \"superuser\"}",
				esc_role);

			sage_upsert_finding(
				"security_superuser", "warning",
				"role", rolenames[i],
				title_buf.data, detail.data,
				"This role has superuser privileges. Consider using more "
				"granular privileges instead to follow the principle of "
				"least privilege.",
				psprintf("ALTER ROLE %s NOSUPERUSER;", rolenames[i]),
				psprintf("ALTER ROLE %s SUPERUSER;", rolenames[i]));

			pfree(detail.data);
			pfree(title_buf.data);
			pfree(esc_role);
		}

		for (i = 0; i < nrows; i++)
			pfree(rolenames[i]);
		pfree(rolenames);
	}

	/* ----- Roles with CREATEDB/CREATEROLE that don't look admin-like ----- */
	ret = sage_spi_exec(
		"SELECT rolname, rolcreatedb, rolcreaterole FROM pg_roles "
		"WHERE (rolcreatedb = true OR rolcreaterole = true) "
		"  AND rolsuper = false "
		"  AND rolname NOT LIKE '%admin%' "
		"  AND rolname NOT LIKE '%dba%' "
		"  AND rolname NOT LIKE '%manage%' "
		"  AND rolname NOT IN ('postgres', 'rdsadmin', 'cloudsqlsuperuser')",
		0);

	if (ret >= 0 && SPI_processed > 0)
	{
		int nrows = (int) SPI_processed;

		typedef struct
		{
			char   *rolname;
			bool	createdb;
			bool	createrole;
		} RoleInfo;

		RoleInfo *roles = palloc(sizeof(RoleInfo) * nrows);

		for (i = 0; i < nrows; i++)
		{
			roles[i].rolname = pstrdup(sage_spi_getval_str(i, 0));
			/* SPI returns 't'/'f' for booleans via getvalue */
			{
				char *cdb = sage_spi_getval_str(i, 1);
				char *crl = sage_spi_getval_str(i, 2);
				roles[i].createdb   = (cdb && cdb[0] == 't');
				roles[i].createrole = (crl && crl[0] == 't');
			}
		}

		for (i = 0; i < nrows; i++)
		{
			StringInfoData detail;
			StringInfoData title_buf;
			StringInfoData rec_sql;
			StringInfoData rollback_sql;
			char *esc_role;
			const char *priv_str;

			CHECK_FOR_INTERRUPTS();

			initStringInfo(&detail);
			initStringInfo(&title_buf);
			initStringInfo(&rec_sql);
			initStringInfo(&rollback_sql);

			esc_role = sage_escape_json_string(roles[i].rolname);

			if (roles[i].createdb && roles[i].createrole)
				priv_str = "CREATEDB, CREATEROLE";
			else if (roles[i].createdb)
				priv_str = "CREATEDB";
			else
				priv_str = "CREATEROLE";

			appendStringInfo(&title_buf,
				"Role %s has elevated privileges: %s",
				roles[i].rolname, priv_str);

			appendStringInfo(&detail,
				"{\"role\": \"%s\", \"privileges\": \"%s\"}",
				esc_role, priv_str);

			/* Build recommended SQL */
			if (roles[i].createdb && roles[i].createrole)
			{
				appendStringInfo(&rec_sql,
					"ALTER ROLE %s NOCREATEDB NOCREATEROLE;",
					roles[i].rolname);
				appendStringInfo(&rollback_sql,
					"ALTER ROLE %s CREATEDB CREATEROLE;",
					roles[i].rolname);
			}
			else if (roles[i].createdb)
			{
				appendStringInfo(&rec_sql,
					"ALTER ROLE %s NOCREATEDB;", roles[i].rolname);
				appendStringInfo(&rollback_sql,
					"ALTER ROLE %s CREATEDB;", roles[i].rolname);
			}
			else
			{
				appendStringInfo(&rec_sql,
					"ALTER ROLE %s NOCREATEROLE;", roles[i].rolname);
				appendStringInfo(&rollback_sql,
					"ALTER ROLE %s CREATEROLE;", roles[i].rolname);
			}

			sage_upsert_finding(
				"security_elevated_role", "warning",
				"role", roles[i].rolname,
				title_buf.data, detail.data,
				"This non-admin role has elevated privileges. Review whether "
				"these privileges are necessary.",
				rec_sql.data, rollback_sql.data);

			pfree(detail.data);
			pfree(title_buf.data);
			pfree(rec_sql.data);
			pfree(rollback_sql.data);
			pfree(esc_role);
		}

		for (i = 0; i < nrows; i++)
			pfree(roles[i].rolname);
		pfree(roles);
	}

	/* ----- Missing RLS on sensitive tables ----- */
	ret = sage_spi_exec(
		"SELECT DISTINCT c.relname, n.nspname "
		"FROM pg_class c "
		"JOIN pg_namespace n ON n.oid = c.relnamespace "
		"JOIN information_schema.columns col "
		"  ON col.table_schema = n.nspname AND col.table_name = c.relname "
		"WHERE c.relkind = 'r' "
		"  AND NOT c.relrowsecurity "
		"  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sage') "
		"  AND lower(col.column_name) IN "
		"      ('email', 'password', 'ssn', 'credit_card', 'phone', 'address', "
		"       'credit_card_number', 'social_security', 'phone_number', "
		"       'password_hash', 'passwd') "
		"ORDER BY n.nspname, c.relname",
		0);

	if (ret >= 0 && SPI_processed > 0)
	{
		int nrows = (int) SPI_processed;

		typedef struct
		{
			char   *relname;
			char   *nspname;
		} SensitiveTbl;

		SensitiveTbl *tables = palloc(sizeof(SensitiveTbl) * nrows);

		for (i = 0; i < nrows; i++)
		{
			tables[i].relname = pstrdup(sage_spi_getval_str(i, 0));
			tables[i].nspname = pstrdup(sage_spi_getval_str(i, 1));
		}

		for (i = 0; i < nrows; i++)
		{
			StringInfoData detail;
			StringInfoData object_id;
			StringInfoData title_buf;
			char *oid;
			char *esc_tbl;

			CHECK_FOR_INTERRUPTS();

			initStringInfo(&detail);
			initStringInfo(&object_id);
			initStringInfo(&title_buf);

			oid = psprintf("%s.%s", tables[i].nspname, tables[i].relname);
			esc_tbl = sage_escape_json_string(oid);

			appendStringInfoString(&object_id, oid);

			appendStringInfo(&title_buf,
				"Table %s has sensitive columns but no RLS", oid);

			appendStringInfo(&detail,
				"{\"table\": \"%s\", \"issue\": \"missing_rls\", "
				"\"note\": \"Contains columns with sensitive-looking names\"}",
				esc_tbl);

			sage_upsert_finding(
				"security_missing_rls", "warning",
				"table", object_id.data,
				title_buf.data, detail.data,
				"This table appears to contain sensitive data (columns named "
				"email, password, ssn, etc.) but does not have Row Level "
				"Security enabled. Consider enabling RLS to control row-level "
				"access.",
				psprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;", oid),
				psprintf("ALTER TABLE %s DISABLE ROW LEVEL SECURITY;", oid));

			pfree(detail.data);
			pfree(object_id.data);
			pfree(title_buf.data);
			pfree(esc_tbl);
			pfree(oid);
		}

		for (i = 0; i < nrows; i++)
		{
			pfree(tables[i].relname);
			pfree(tables[i].nspname);
		}
		pfree(tables);
	}

	/* ----- Unusual connections from pg_stat_activity ----- */
	ret = sage_spi_exec(
		"SELECT application_name, client_addr::text, count(*) as cnt "
		"FROM pg_stat_activity "
		"WHERE pid != pg_backend_pid() "
		"  AND datname = current_database() "
		"  AND application_name IS NOT NULL "
		"  AND application_name != '' "
		"GROUP BY application_name, client_addr "
		"HAVING count(*) > 10 "
		"ORDER BY count(*) DESC "
		"LIMIT 50",
		0);

	if (ret >= 0 && SPI_processed > 0)
	{
		int nrows = (int) SPI_processed;

		typedef struct
		{
			char   *app_name;
			char   *client_addr;
			int64	cnt;
		} ConnInfo;

		ConnInfo *conns = palloc(sizeof(ConnInfo) * nrows);

		for (i = 0; i < nrows; i++)
		{
			conns[i].app_name   = pstrdup(sage_spi_getval_str(i, 0));
			conns[i].client_addr = sage_spi_isnull(i, 1)
				? pstrdup("(local)") : pstrdup(sage_spi_getval_str(i, 1));
			conns[i].cnt        = sage_spi_getval_int64(i, 2);
		}

		for (i = 0; i < nrows; i++)
		{
			StringInfoData detail;
			StringInfoData object_id;
			StringInfoData title_buf;
			char *esc_app;
			char *esc_addr;

			CHECK_FOR_INTERRUPTS();

			initStringInfo(&detail);
			initStringInfo(&object_id);
			initStringInfo(&title_buf);

			esc_app  = sage_escape_json_string(conns[i].app_name);
			esc_addr = sage_escape_json_string(conns[i].client_addr);

			appendStringInfo(&object_id, "conn:%s:%s",
							 conns[i].app_name, conns[i].client_addr);

			appendStringInfo(&title_buf,
				"High connection count (" INT64_FORMAT ") from %s @ %s",
				conns[i].cnt, conns[i].app_name, conns[i].client_addr);

			appendStringInfo(&detail,
				"{\"application_name\": \"%s\", \"client_addr\": \"%s\", "
				"\"connection_count\": " INT64_FORMAT "}",
				esc_app, esc_addr, conns[i].cnt);

			sage_upsert_finding(
				"security_connection_source", "info",
				"connection", object_id.data,
				title_buf.data, detail.data,
				"A high number of connections from this application/address "
				"combination was detected. Verify this is expected behavior.",
				NULL, NULL);

			pfree(detail.data);
			pfree(object_id.data);
			pfree(title_buf.data);
			pfree(esc_app);
			pfree(esc_addr);
		}

		for (i = 0; i < nrows; i++)
		{
			pfree(conns[i].app_name);
			pfree(conns[i].client_addr);
		}
		pfree(conns);
	}

	SPI_finish();
}

/* ================================================================
 * 3. sage_analyze_replication_health  (Tier 1.9)
 * ================================================================ */
void
sage_analyze_replication_health(void)
{
	int		ret;
	int		i;

	SPI_connect();
	sage_spi_exec("SET LOCAL statement_timeout = '500ms'", 0);

	/* ----- Inactive replication slots ----- */
	sage_spi_exec("SAVEPOINT _sage_repl_slots", 0);
	PG_TRY();
	{
		ret = sage_spi_exec(
			"SELECT slot_name, slot_type, "
			"       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as lag_bytes "
			"FROM pg_replication_slots "
			"WHERE NOT active",
			0);

		if (ret >= 0 && SPI_processed > 0)
		{
			int nrows = (int) SPI_processed;

			typedef struct
			{
				char   *slot_name;
				char   *slot_type;
				int64	lag_bytes;
			} SlotInfo;

			SlotInfo *slots = palloc(sizeof(SlotInfo) * nrows);

			for (i = 0; i < nrows; i++)
			{
				slots[i].slot_name = pstrdup(sage_spi_getval_str(i, 0));
				slots[i].slot_type = pstrdup(sage_spi_getval_str(i, 1));
				slots[i].lag_bytes = sage_spi_isnull(i, 2) ? 0 : sage_spi_getval_int64(i, 2);
			}

			for (i = 0; i < nrows; i++)
			{
				const char *severity;
				StringInfoData detail;
				StringInfoData object_id;
				StringInfoData title_buf;
				char *esc_slot;
				char *esc_type;
				double lag_gb;

				CHECK_FOR_INTERRUPTS();

				lag_gb = (double) slots[i].lag_bytes / (1024.0 * 1024.0 * 1024.0);

				/* Critical if lag > 1GB, warning otherwise */
				severity = (slots[i].lag_bytes > (int64) 1073741824) ? "critical" : "warning";

				initStringInfo(&detail);
				initStringInfo(&object_id);
				initStringInfo(&title_buf);

				esc_slot = sage_escape_json_string(slots[i].slot_name);
				esc_type = sage_escape_json_string(slots[i].slot_type);

				appendStringInfoString(&object_id, slots[i].slot_name);

				appendStringInfo(&title_buf,
					"Inactive replication slot %s (%.2f GB lag)",
					slots[i].slot_name, lag_gb);

				appendStringInfo(&detail,
					"{\"slot_name\": \"%s\", \"slot_type\": \"%s\", "
					"\"lag_bytes\": " INT64_FORMAT ", \"lag_gb\": %.2f, "
					"\"active\": false}",
					esc_slot, esc_type,
					slots[i].lag_bytes, lag_gb);

				sage_upsert_finding(
					"replication_inactive_slot", severity,
					"replication_slot", object_id.data,
					title_buf.data, detail.data,
					"This inactive replication slot is preventing WAL "
					"cleanup, causing disk usage to grow. Either reactivate "
					"the consumer or drop the slot.",
					psprintf("SELECT pg_drop_replication_slot('%s');",
							 slots[i].slot_name),
					psprintf("-- Recreate slot: SELECT pg_create_%s_replication_slot('%s', ...);",
							 strcmp(slots[i].slot_type, "logical") == 0 ? "logical" : "physical",
							 slots[i].slot_name));

				pfree(detail.data);
				pfree(object_id.data);
				pfree(title_buf.data);
				pfree(esc_slot);
				pfree(esc_type);
			}

			for (i = 0; i < nrows; i++)
			{
				pfree(slots[i].slot_name);
				pfree(slots[i].slot_type);
			}
			pfree(slots);
		}

		sage_spi_exec("RELEASE SAVEPOINT _sage_repl_slots", 0);
	}
	PG_CATCH();
	{
		FlushErrorState();
		sage_spi_exec("ROLLBACK TO SAVEPOINT _sage_repl_slots", 0);
		sage_spi_exec("RELEASE SAVEPOINT _sage_repl_slots", 0);
	}
	PG_END_TRY();

	/* ----- WAL archiving check ----- */
	sage_spi_exec("SAVEPOINT _sage_wal_archive", 0);
	PG_TRY();
	{
		ret = sage_spi_exec(
			"SELECT last_archived_wal, last_archived_time, "
			"       last_failed_wal, failed_count "
			"FROM pg_stat_archiver "
			"WHERE last_archived_time IS NOT NULL",
			0);

		if (ret >= 0 && SPI_processed > 0)
		{
			char   *last_wal    = sage_spi_isnull(0, 0) ? NULL : pstrdup(sage_spi_getval_str(0, 0));
			char   *last_time   = sage_spi_isnull(0, 1) ? NULL : pstrdup(sage_spi_getval_str(0, 1));
			char   *failed_wal  = sage_spi_isnull(0, 2) ? NULL : pstrdup(sage_spi_getval_str(0, 2));
			int64	failed_cnt  = sage_spi_isnull(0, 3) ? 0 : sage_spi_getval_int64(0, 3);

			/* Check if last archive was more than 1 hour ago */
			if (last_time != NULL)
			{
				int stale_ret;

				stale_ret = sage_spi_exec(
					"SELECT 1 FROM pg_stat_archiver "
					"WHERE last_archived_time < now() - interval '1 hour' "
					"  AND last_archived_time IS NOT NULL",
					0);

				if (stale_ret >= 0 && SPI_processed > 0)
				{
					StringInfoData detail;
					StringInfoData title_buf;
					char *esc_wal = last_wal
						? sage_escape_json_string(last_wal) : pstrdup("");
					char *esc_failed = failed_wal
						? sage_escape_json_string(failed_wal) : pstrdup("");

					initStringInfo(&detail);
					initStringInfo(&title_buf);

					appendStringInfo(&title_buf,
						"WAL archiving may be stale (last: %s)",
						last_time ? last_time : "unknown");

					appendStringInfo(&detail,
						"{\"last_archived_wal\": \"%s\", "
						"\"last_archived_time\": \"%s\", "
						"\"last_failed_wal\": \"%s\", "
						"\"failed_count\": " INT64_FORMAT "}",
						esc_wal,
						last_time ? last_time : "",
						esc_failed, failed_cnt);

					sage_upsert_finding(
						"replication_wal_archiving", "warning",
						"archiver", "pg_stat_archiver",
						title_buf.data, detail.data,
						"WAL archiving has not produced a new archive segment "
						"in over 1 hour. Check archive_command and disk space "
						"on the archive destination.",
						NULL, NULL);

					pfree(detail.data);
					pfree(title_buf.data);
					pfree(esc_wal);
					pfree(esc_failed);
				}
				else
				{
					sage_resolve_finding("replication_wal_archiving",
										 "pg_stat_archiver");
				}
			}

			/* Check for archive failures */
			if (failed_cnt > 0 && failed_wal != NULL)
			{
				StringInfoData detail;
				StringInfoData title_buf;
				char *esc_failed = sage_escape_json_string(failed_wal);

				initStringInfo(&detail);
				initStringInfo(&title_buf);

				appendStringInfo(&title_buf,
					"WAL archive failures detected (" INT64_FORMAT " failures)",
					failed_cnt);

				appendStringInfo(&detail,
					"{\"failed_count\": " INT64_FORMAT ", "
					"\"last_failed_wal\": \"%s\"}",
					failed_cnt, esc_failed);

				sage_upsert_finding(
					"replication_wal_archive_failures", "warning",
					"archiver", "pg_stat_archiver_failures",
					title_buf.data, detail.data,
					"There have been WAL archive failures. Investigate the "
					"archive_command and ensure the archive destination is "
					"available.",
					NULL, NULL);

				pfree(detail.data);
				pfree(title_buf.data);
				pfree(esc_failed);
			}

			if (last_wal)
				pfree(last_wal);
			if (last_time)
				pfree(last_time);
			if (failed_wal)
				pfree(failed_wal);
		}

		sage_spi_exec("RELEASE SAVEPOINT _sage_wal_archive", 0);
	}
	PG_CATCH();
	{
		FlushErrorState();
		sage_spi_exec("ROLLBACK TO SAVEPOINT _sage_wal_archive", 0);
		sage_spi_exec("RELEASE SAVEPOINT _sage_wal_archive", 0);
	}
	PG_END_TRY();

	/* ----- Materialized view staleness (proxy check) ----- */
	sage_spi_exec("SAVEPOINT _sage_matview", 0);
	PG_TRY();
	{
		ret = sage_spi_exec(
			"SELECT n.nspname, c.relname "
			"FROM pg_class c "
			"JOIN pg_namespace n ON n.oid = c.relnamespace "
			"LEFT JOIN pg_stat_user_tables st "
			"  ON st.schemaname = n.nspname AND st.relname = c.relname "
			"WHERE c.relkind = 'm' "
			"  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sage') "
			"  AND (st.last_analyze IS NULL "
			"       OR st.last_analyze < now() - interval '7 days') "
			"ORDER BY n.nspname, c.relname "
			"LIMIT 50",
			0);

		if (ret >= 0 && SPI_processed > 0)
		{
			int nrows = (int) SPI_processed;

			typedef struct
			{
				char   *nspname;
				char   *relname;
			} MatviewInfo;

			MatviewInfo *mvs = palloc(sizeof(MatviewInfo) * nrows);

			for (i = 0; i < nrows; i++)
			{
				mvs[i].nspname = pstrdup(sage_spi_getval_str(i, 0));
				mvs[i].relname = pstrdup(sage_spi_getval_str(i, 1));
			}

			for (i = 0; i < nrows; i++)
			{
				StringInfoData detail;
				StringInfoData object_id;
				StringInfoData title_buf;
				char *oid;
				char *esc_mv;

				CHECK_FOR_INTERRUPTS();

				initStringInfo(&detail);
				initStringInfo(&object_id);
				initStringInfo(&title_buf);

				oid = psprintf("%s.%s", mvs[i].nspname, mvs[i].relname);
				esc_mv = sage_escape_json_string(oid);

				appendStringInfoString(&object_id, oid);

				appendStringInfo(&title_buf,
					"Materialized view %s may be stale", oid);

				appendStringInfo(&detail,
					"{\"matview\": \"%s\", \"issue\": \"no_recent_refresh\"}",
					esc_mv);

				sage_upsert_finding(
					"replication_matview_stale", "info",
					"matview", object_id.data,
					title_buf.data, detail.data,
					"This materialized view has not been analyzed recently, "
					"which may indicate it has not been refreshed. Consider "
					"scheduling regular REFRESH MATERIALIZED VIEW.",
					psprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s;", oid),
					NULL);

				pfree(detail.data);
				pfree(object_id.data);
				pfree(title_buf.data);
				pfree(esc_mv);
				pfree(oid);
			}

			for (i = 0; i < nrows; i++)
			{
				pfree(mvs[i].nspname);
				pfree(mvs[i].relname);
			}
			pfree(mvs);
		}

		sage_spi_exec("RELEASE SAVEPOINT _sage_matview", 0);
	}
	PG_CATCH();
	{
		FlushErrorState();
		sage_spi_exec("ROLLBACK TO SAVEPOINT _sage_matview", 0);
		sage_spi_exec("RELEASE SAVEPOINT _sage_matview", 0);
	}
	PG_END_TRY();

	/* ----- Replication lag check ----- */
	sage_spi_exec("SAVEPOINT _sage_repl_lag", 0);
	PG_TRY();
	{
		ret = sage_spi_exec(
			"SELECT client_addr::text, application_name, "
			"       replay_lag, "
			"       EXTRACT(EPOCH FROM replay_lag) as lag_seconds "
			"FROM pg_stat_replication "
			"WHERE replay_lag > interval '5 minutes'",
			0);

		if (ret >= 0 && SPI_processed > 0)
		{
			int nrows = (int) SPI_processed;

			typedef struct
			{
				char   *client_addr;
				char   *app_name;
				char   *replay_lag;
				double	lag_seconds;
			} ReplLagInfo;

			ReplLagInfo *lags = palloc(sizeof(ReplLagInfo) * nrows);

			for (i = 0; i < nrows; i++)
			{
				lags[i].client_addr = sage_spi_isnull(i, 0)
					? pstrdup("(unknown)") : pstrdup(sage_spi_getval_str(i, 0));
				lags[i].app_name = sage_spi_isnull(i, 1)
					? pstrdup("") : pstrdup(sage_spi_getval_str(i, 1));
				lags[i].replay_lag = sage_spi_isnull(i, 2)
					? pstrdup("unknown") : pstrdup(sage_spi_getval_str(i, 2));
				lags[i].lag_seconds = sage_spi_isnull(i, 3)
					? 0.0 : sage_spi_getval_float(i, 3);
			}

			for (i = 0; i < nrows; i++)
			{
				const char *severity;
				StringInfoData detail;
				StringInfoData object_id;
				StringInfoData title_buf;
				char *esc_addr;
				char *esc_app;
				char *esc_lag;

				CHECK_FOR_INTERRUPTS();

				/* Critical if lag > 30 minutes, warning otherwise */
				severity = (lags[i].lag_seconds > 1800.0) ? "critical" : "warning";

				initStringInfo(&detail);
				initStringInfo(&object_id);
				initStringInfo(&title_buf);

				esc_addr = sage_escape_json_string(lags[i].client_addr);
				esc_app  = sage_escape_json_string(lags[i].app_name);
				esc_lag  = sage_escape_json_string(lags[i].replay_lag);

				appendStringInfo(&object_id, "repl:%s", lags[i].client_addr);

				appendStringInfo(&title_buf,
					"Replication lag %s on replica %s",
					lags[i].replay_lag, lags[i].client_addr);

				appendStringInfo(&detail,
					"{\"client_addr\": \"%s\", \"application_name\": \"%s\", "
					"\"replay_lag\": \"%s\", \"lag_seconds\": %.0f}",
					esc_addr, esc_app, esc_lag, lags[i].lag_seconds);

				sage_upsert_finding(
					"replication_lag", severity,
					"replication", object_id.data,
					title_buf.data, detail.data,
					"This replica has significant replication lag. Check "
					"network connectivity, replica load, and WAL sender "
					"configuration.",
					NULL, NULL);

				pfree(detail.data);
				pfree(object_id.data);
				pfree(title_buf.data);
				pfree(esc_addr);
				pfree(esc_app);
				pfree(esc_lag);
			}

			for (i = 0; i < nrows; i++)
			{
				pfree(lags[i].client_addr);
				pfree(lags[i].app_name);
				pfree(lags[i].replay_lag);
			}
			pfree(lags);
		}

		sage_spi_exec("RELEASE SAVEPOINT _sage_repl_lag", 0);
	}
	PG_CATCH();
	{
		FlushErrorState();
		sage_spi_exec("ROLLBACK TO SAVEPOINT _sage_repl_lag", 0);
		sage_spi_exec("RELEASE SAVEPOINT _sage_repl_lag", 0);
	}
	PG_END_TRY();

	/* ----- Logical replication subscription errors ----- */
	sage_spi_exec("SAVEPOINT _sage_logrep", 0);
	PG_TRY();
	{
		ret = sage_spi_exec(
			"SELECT subname, "
			"       CASE WHEN srsubstate = 'e' THEN true ELSE false END as has_error, "
			"       srsubstate "
			"FROM pg_subscription sub "
			"JOIN pg_subscription_rel sr ON sr.srsubid = sub.oid "
			"WHERE srsubstate IN ('e') "
			"LIMIT 50",
			0);

		if (ret >= 0 && SPI_processed > 0)
		{
			int nrows = (int) SPI_processed;

			typedef struct
			{
				char   *subname;
				char   *state;
			} SubInfo;

			SubInfo *subs = palloc(sizeof(SubInfo) * nrows);

			for (i = 0; i < nrows; i++)
			{
				subs[i].subname = pstrdup(sage_spi_getval_str(i, 0));
				subs[i].state   = pstrdup(sage_spi_getval_str(i, 2));
			}

			for (i = 0; i < nrows; i++)
			{
				StringInfoData detail;
				StringInfoData object_id;
				StringInfoData title_buf;
				char *esc_sub;

				CHECK_FOR_INTERRUPTS();

				initStringInfo(&detail);
				initStringInfo(&object_id);
				initStringInfo(&title_buf);

				esc_sub = sage_escape_json_string(subs[i].subname);

				appendStringInfoString(&object_id, subs[i].subname);

				appendStringInfo(&title_buf,
					"Logical replication error in subscription %s",
					subs[i].subname);

				appendStringInfo(&detail,
					"{\"subscription\": \"%s\", \"state\": \"%s\", "
					"\"issue\": \"replication_error\"}",
					esc_sub, subs[i].state);

				sage_upsert_finding(
					"replication_logical_error", "critical",
					"subscription", object_id.data,
					title_buf.data, detail.data,
					"A logical replication subscription has encountered an "
					"error. Check pg_subscription_rel for details and resolve "
					"the conflict to resume replication.",
					psprintf("ALTER SUBSCRIPTION %s ENABLE;", subs[i].subname),
					NULL);

				pfree(detail.data);
				pfree(object_id.data);
				pfree(title_buf.data);
				pfree(esc_sub);
			}

			for (i = 0; i < nrows; i++)
			{
				pfree(subs[i].subname);
				pfree(subs[i].state);
			}
			pfree(subs);
		}

		sage_spi_exec("RELEASE SAVEPOINT _sage_logrep", 0);
	}
	PG_CATCH();
	{
		FlushErrorState();
		sage_spi_exec("ROLLBACK TO SAVEPOINT _sage_logrep", 0);
		sage_spi_exec("RELEASE SAVEPOINT _sage_logrep", 0);
	}
	PG_END_TRY();

	SPI_finish();
}
