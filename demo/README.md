# pg_sage Demo

Interactive demo of pg_sage capabilities, designed for recording with asciinema.

## Prerequisites

- Docker and Docker Compose
- `curl` (for MCP sidecar and Prometheus tests)
- `asciinema` (for recording) — [install](https://asciinema.org/docs/installation)
- `agg` (for GIF conversion) — [install](https://github.com/asciinema/agg)

## Recording with asciinema

```bash
# From the pg_sage root directory:
cd demo

# Record the demo
asciinema rec -c "./demo.sh" pg_sage_demo.cast

# Convert to GIF (requires agg)
agg pg_sage_demo.cast pg_sage_demo.gif --cols 100 --rows 35

# Or convert with custom theme
agg pg_sage_demo.cast pg_sage_demo.gif \
    --font-size 16 \
    --cols 100 \
    --rows 35
```

## Running Manually

```bash
# Make sure Docker Compose is up first
docker compose up -d

# Wait for PostgreSQL to be healthy
docker exec pg_sage-pg_sage-1 pg_isready -U postgres

# Run the demo
chmod +x demo.sh
./demo.sh
```

## Configuration

The demo script supports environment variables to customize behavior:

| Variable | Default | Description |
|---|---|---|
| `PG_SAGE_CONTAINER` | `pg_sage-pg_sage-1` | Docker container name |
| `SIDECAR_HOST` | `localhost` | MCP sidecar hostname |
| `MCP_PORT` | `5433` | MCP sidecar port |
| `PROM_PORT` | `9187` | Prometheus metrics port |
| `TYPING_DELAY` | `0.03` | Seconds between typed characters |
| `LINE_PAUSE` | `1.5` | Pause after each command |
| `SECTION_PAUSE` | `2` | Pause between sections |

Example with faster typing for quick runs:

```bash
TYPING_DELAY=0.01 LINE_PAUSE=0.5 SECTION_PAUSE=1 ./demo.sh
```

## What the Demo Covers

1. **Docker Compose startup** — starts or connects to existing containers
2. **Extension status** — `sage.status()` showing version, workers, trust level
3. **Finding detection** — waits for the analyzer cycle, then shows findings
4. **Health briefing** — `sage.briefing()` with Tier 1 analysis
5. **Schema analysis** — `sage.schema_json('public.orders')` showing DDL, indexes, constraints
6. **Slow queries** — `sage.slow_queries_json()` from pg_stat_statements
7. **Emergency controls** — `sage.emergency_stop()` and `sage.resume()`
8. **Finding suppression** — `sage.suppress()` with expiry
9. **MCP sidecar** — SSE connection, session creation, JSON-RPC initialize
10. **Prometheus metrics** — scraping `/metrics` endpoint
11. **Summary** — recap of all features demonstrated

## Sample Output

See [sample_output.md](sample_output.md) for expected output from key commands.

## Tips for a Good Recording

- Use a terminal width of at least 100 columns
- Use a dark terminal theme for contrast
- Run `docker compose up` beforehand so the startup is instant
- For the 60-second wait, you can edit the cast file to speed it up:
  ```bash
  # Speed up the wait section in the cast file
  # Look for the progress bar section and reduce timestamps
  ```
- Alternatively, set `TYPING_DELAY=0.02` for snappier typing
