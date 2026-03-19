# Installation

## Docker (Recommended)

The fastest way to get pg_sage running. The included `docker-compose.yml` configures `shared_preload_libraries`, `pg_stat_statements`, and default GUCs automatically.

```bash
git clone https://github.com/jasonmassie01/pg_sage.git
cd pg_sage
docker compose up
```

This starts two services:

| Service | Port | Description |
|---|---|---|
| `pg_sage` | 5432 | PostgreSQL with pg_sage extension loaded |
| `sidecar` | 5433, 9187 | MCP server and Prometheus metrics |

Connect to the database:

```bash
docker exec -it pg_sage-pg_sage-1 psql -U postgres
```

---

## From Source

### Prerequisites

- PostgreSQL 14, 15, 16, or 17 (with development headers)
- `pg_stat_statements` extension
- `libcurl` development headers (for optional LLM integration)
- C compiler (gcc or clang) and make

On Debian/Ubuntu:

```bash
sudo apt-get install postgresql-server-dev-17 libcurl4-openssl-dev build-essential
```

On RHEL/Fedora:

```bash
sudo dnf install postgresql17-devel libcurl-devel gcc make
```

### Build and Install

```bash
git clone https://github.com/jasonmassie01/pg_sage.git
cd pg_sage
make
sudo make install
```

### Configure PostgreSQL

Add to `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_stat_statements,pg_sage'
sage.database = 'postgres'
```

!!! warning
    `shared_preload_libraries` requires a full PostgreSQL restart. A `pg_ctl reload` is not sufficient.

### Create the Extensions

Restart PostgreSQL, then connect and run:

```sql
CREATE EXTENSION pg_stat_statements;
CREATE EXTENSION pg_sage;
```

### Optional: Build the MCP Sidecar

```bash
cd sidecar
go build -o sage-sidecar .
```

Set the environment and start:

```bash
export SAGE_DATABASE_URL="postgres://user:pass@localhost:5432/postgres"
export SAGE_MCP_PORT=5433
export SAGE_PROMETHEUS_PORT=9187
./sage-sidecar
```

---

## RDS / Aurora / Cloud SQL (Sidecar-Only Mode)

The pg_sage C extension requires `shared_preload_libraries` access, which is not available on managed database services (RDS, Aurora, Cloud SQL).

The MCP sidecar can operate in **sidecar-only mode**, connecting directly to your managed database and providing monitoring through direct catalog queries instead of the extension's SQL functions.

```bash
docker compose run --rm \
  -e SAGE_DATABASE_URL="postgres://user:pass@your-rds-host:5432/dbname?sslmode=require" \
  -p 5433:5433 -p 9187:9187 \
  sidecar
```

The sidecar auto-detects whether the pg_sage extension is installed and falls back to direct catalog queries if it is not.

!!! note
    Sidecar-only mode provides monitoring and metrics but does not include the rules engine, action executor, or background workers. Those features require the C extension.

---

## Verifying the Installation

After starting pg_sage, verify everything is working:

```sql
-- Check that the extension is loaded
SELECT * FROM pg_extension WHERE extname = 'pg_sage';

-- Verify background workers are running
SELECT * FROM sage.status();
```

Expected output from `sage.status()`:

```json
{
  "version": "0.5.0",
  "enabled": true,
  "circuit_state": "closed",
  "llm_circuit_state": "closed",
  "emergency_stopped": false,
  "workers": {
    "collector": true,
    "analyzer": true,
    "briefing": true
  }
}
```

!!! tip
    Findings begin appearing after the first analyzer cycle (default: 60 seconds). Run `SELECT category, severity, title FROM sage.findings WHERE status = 'open';` to see them.

Verify the MCP sidecar (if running):

```bash
# Health check
curl -s http://localhost:9187/metrics | head -5

# MCP endpoint
curl -s http://localhost:5433/sse
```
