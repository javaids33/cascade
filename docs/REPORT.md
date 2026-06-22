# Turso at the Edge: native replication + CDC → OLAP (DuckDB/Iceberg) + edge vector search

An experiment evaluating **Turso** (the Rust rewrite of SQLite) as a distributed edge OLTP database whose changes stream via **Change Data Capture** into an OLAP/lakehouse layer (**DuckDB + Apache Iceberg**), plus an **edge vector-search** demo — measured end to end. This harness is implemented natively in **Rust** (turso/duckdb/iceberg crates).

> Reproduce with `./setup.sh && ./run.sh`. All numbers below are emitted to `results/*.json` by the harness. Default dataset is synthetic; point `PATENTS_JSONL` at a real patents JSONL to reproduce the headline numbers.

## TL;DR

- Turso's **native sync server** gives genuine primary→replica replication in-process — no Kafka/Debezium/cloud. Page-level WAL sync makes replica catch-up sub-second.
- **CDC `full` mode** captures every change as queryable SQL (`turso_cdc`) at a measured **43.7% write-throughput cost** and **~2.01× storage**.
- That CDC stream loads cleanly into **DuckDB + Iceberg**, making Turso a tidy **source into the lakehouse** rather than an analytics engine itself.
- DuckDB beats Turso on analytics by design (columnar vs row) — see the lane comparison.
- Edge vector search works today but is **linear-scan (no ANN index)** — latency grows with rows.

## CDC capture overhead (Phase 3a)

| Mode | Throughput | DB size |
|---|---|---|
| CDC off | 307,333 rows/s | 33MB |
| CDC on (`full`) | 172,956 rows/s | 66MB |

→ **43.7% throughput cost, 2.01× storage** (50,012 change records for 50,000 inserts). `full` mode stores before+after row images; `id`/`after` modes are lighter.

## CDC → OLAP: DuckDB + Apache Iceberg (Phase 3b)

Drained **50,000 change records** → DuckDB + Iceberg in **10.306s** (**4,851 changes/s**). Both sinks verified consistent at 50,000 rows (consistent=true).

The loader tails `turso_cdc` with a `change_id` cursor, decodes before/after images via Turso's `bin_record_json_object()` SQL helper, and applies insert/update/delete to both sinks.

## Native master→replica replication (Phase 2)

| Metric | Value |
|---|---|
| Master ingest | 161,142 rows/s |
| Initial push (45,000) | 1.234s, 63MB |
| Replica bootstrap pull | **0.067s** → 45,000 rows |
| Incremental push (5,000) | 0.182s, 7MB |
| Incremental pull (5,000) | **0.028s**, 3MB |
| Converged | true |

## OLAP lane comparison: DuckDB vs Turso (Phase 4)

Turso bulk-load: 3,138,753 rows in 9.29s (338,037 rows/s), indexed.

| Query | DuckDB | Turso | Turso slower |
|---|---|---|---|
| Q1_top_assignees | 2.0 ms | 5.2 ms | 2.5× |
| Q2_grants_per_year | 0.3 ms | 3.7 ms | 13.8× |
| Q3_top_cpc_sections | 1.0 ms | 88.3 ms | 89.7× |
| Q4_most_cited | 13.6 ms | 170.6 ms | 12.6× |
| Q5_citations_by_assignee | 11.4 ms | 2144.2 ms | 188.6× |

DuckDB (columnar, vectorized) dominates scans/aggregations — exactly as expected. Turso is a row store tuned for point reads/writes; this is a lane comparison, not a defeat.

## Edge vector search (Phase 5)

Embedded abstracts as `F32_BLOB(64)`; top-10 cosine search via `vector_distance_cos`. Self-NN correctness: true.

| Rows | p50 latency | p95 latency |
|---|---|---|
| 1,000 | 0.23 ms | 0.24 ms |
| 5,000 | 1.19 ms | 1.22 ms |
| 10,000 | 2.41 ms | 2.43 ms |
| 25,000 | 6.35 ms | 6.54 ms |
| 50,000 | 14.15 ms | 14.67 ms |

> linear scan, no ANN index in Turso v0.6.1; latency grows ~linearly with rows

## Where Turso fits (data-engineer's verdict)

- **Pick Turso** for embedded/edge OLTP that needs SQLite's deployment model (single file, zero-ops, runs anywhere) *plus* async I/O, MVCC writes, built-in CDC, native replication, and co-located vectors — without bolting on extensions.
- **Don't pick Turso** as your analytics engine. Pair it with DuckDB/Iceberg for that.
- The compelling pattern: **Turso (edge OLTP + CDC) → Iceberg (lakehouse) → DuckDB (query)**. Turso is a clean *source* into the Iceberg standard, not a replacement for it.

## Caveats

- Turso is **BETA** (project's own warning); not yet SQLite-level reliability.
- Vector search is **linear-scan, no ANN index** in v0.6.1 — fine for small/medium edge sets, not a scale claim.
- Sync is **one-way primary→replica**, no conflict resolution / multi-primary.
- **CDC and MVCC (`BEGIN CONCURRENT`) are mutually exclusive** — can't measure both in one run.
- Comparison vs current standards (SQLite+Litestream, Postgres+Debezium) is **conceptual + our numbers**; no competitor pipeline was stood up.
