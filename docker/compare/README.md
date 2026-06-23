# `docker/compare/` — cross-engine competitor benchmarks

The honest gap the repo admits: comparisons to the heavy stack are *conceptual + our numbers* — no
competitor pipeline was stood up. This harness closes it. It runs the **same CDC workload** against
the stacks Cascade claims to replace and emits result JSON with the **same schema** as
`cascade compare-cdc`, so the numbers sit side by side.

> Needs Linux + Docker (the in-process `cascade compare-cdc` needs neither — run that first for the
> same-engine PRAGMA-vs-trigger numbers). These compose stacks are heavy (Kafka/Connect/JVM); give
> them a minute to settle before the bench script registers connectors.

## What it compares

| Pattern | Cascade | Competitor stack | Script |
|---|---|---|---|
| CDC without a broker (P2) | built-in `turso_cdc` PRAGMA | **Postgres + Debezium + Kafka** | `bench-pg-debezium.sh` |
| Self-replicating edge OLTP (P1) | native `push()/pull()` sync | **SQLite + Litestream** | `bench-litestream.sh` |

## Metric contract (emit these so results are comparable)

Each script writes `results/compare_<stack>.json`:

```json
{
  "stack": "postgres_debezium",
  "rows": 50000,
  "ingest_seconds": 0.0,
  "rows_per_sec": 0,
  "cdc_events_captured": 0,
  "capture_lag_ms_p50": 0,
  "storage_bytes": 0,
  "notes": "..."
}
```

Compare against `results/compare_cdc.json` (`turso_builtin_cdc` block) for the CDC pattern and
`results/replication.json` for the sync pattern.

## Run

```bash
# 0) same-engine baseline (no docker)
cascade compare-cdc 50000                      # -> results/compare_cdc.json

# 1) Postgres + Debezium + Kafka
docker compose -f docker/compare/docker-compose.yml up -d
./docker/compare/bench-pg-debezium.sh 50000    # -> results/compare_postgres_debezium.json
docker compose -f docker/compare/docker-compose.yml down -v

# 2) SQLite + Litestream
./docker/compare/bench-litestream.sh 50000     # -> results/compare_sqlite_litestream.json
```

## Status

- `docker-compose.yml` + `bench-pg-debezium.sh` are a **runnable scaffold** — validate on a Linux
  box and tune the connector config / event-count method to your Debezium version before quoting
  the numbers in the article.
- `bench-litestream.sh` is a thin wrapper around the upstream Litestream replicate/restore flow.

The point isn't to beat the specialists at their peak — it's to put the *operational + throughput*
cost of "six services" next to "one embedded file" with real numbers.
