# Patterns: replacing heavy data infra with Turso

An experimental, **runnable** field guide. Each pattern shows **what heavy stack it replaces**, the
**core idea**, **how to run it here** (synthetic benchmark and/or the live lab), and **the number
that proves the win**. Everything is one Rust binary (`cascade`) + the `tursodb` sync-server CLI.

> Status: experimental. Turso is BETA and v0.6.1 vector search is brute-force (no ANN index). These
> patterns are about *architecture* — collapsing many services into one embedded engine — not about
> beating specialist systems at their peak.

| # | Pattern | Replaces | Run |
|---|---|---|---|
| [P1](#p1) | Self-replicating edge OLTP | Postgres read-replicas + Litestream / logical decoding | `cascade run-all`, `cascade replication` |
| [P2](#p2) | CDC without a broker | Debezium + Kafka | `cascade cdc-overhead`, `cascade cdc-to-olap` |
| [P3](#p3) | Co-located vector search | Pinecone / Weaviate / a pgvector service | `cascade vector`, `cascade search` |
| [P4](#p4) | **AI distribution (embed once, fan out)** | per-node embedding infra / hosted embedding APIs at every edge | `cascade serve` (producer) + N× `cascade search` (consumers) |
| [P5](#p5) | Lakehouse source | a bespoke ingestion pipeline into Iceberg | `cascade cdc-to-olap` |

The mega-point: the conventional "live semantic search over a changing corpus" stack is **Postgres +
pgvector + Debezium + Kafka + Pinecone + Litestream** — six moving parts. Every pattern below is a
slice of replacing that with **one embedded file that syncs itself.**

---

<a name="p1"></a>
## P1 · Self-replicating edge OLTP

**Replaces:** Postgres primary + read-replicas, or SQLite + Litestream, or logical-decoding pipelines.

**Core idea:** Turso has a built-in sync server. A primary `push()`es logical change frames; replicas
`pull()`. Replica bootstrap is sub-second; incremental catch-up is a handful of milliseconds and a
few KB — because the wire format is logical CDC, not full pages.

**Run:**
```bash
cascade run-all 50000        # synthetic: spawns the sync server, measures bootstrap + incremental sync
# or the phase alone against a running `tursodb … --sync-server`:
cascade replication
```
**Proves it:** `replica_bootstrap_pull_sec` (sub-second) and `incremental_pull_bytes` (KB, not MB) in
`results/replication.json`. One primary → many replicas; each replica is just another `pull()`.

---

<a name="p2"></a>
## P2 · CDC without a broker

**Replaces:** Debezium + Kafka (+ Zookeeper/Connect).

**Core idea:** `PRAGMA capture_data_changes_conn('full')` turns every insert/update/delete into rows
in a queryable `turso_cdc` table — in-process, no broker. You tail it with a `change_id` cursor and
decode the before/after row images to JSON with `bin_record_json_object()`.

**Run:**
```bash
cascade cdc-overhead         # the cost: ~throughput % + ~2x storage for `full` mode
cascade cdc-to-olap          # drain turso_cdc -> DuckDB + Iceberg, verify both sinks consistent
cascade compare-cdc          # built-in CDC vs the hand-rolled SQLite trigger pattern vs none
```
**Proves it:** `throughput_overhead_pct` / `storage_amplification_x` (the honest cost) and
`consistent: true` (the drained sinks match) in `results/`. `compare-cdc` puts the built-in PRAGMA
next to the trigger+shadow-table approach you'd hand-roll on stock SQLite (`results/compare_cdc.json`).
For the full cross-engine stack (**Postgres + Debezium + Kafka**), stand it up with
[`docker/compare/`](docker/compare/).

> Gotcha worth knowing: `bin_record_json_object` can't decode BLOB columns. Keep vectors in their own
> table (see the lab's `docs` vs `doc_vectors` split) so the CDC→OLAP decode never touches a BLOB.

---

<a name="p3"></a>
## P3 · Co-located vector search

**Replaces:** a separate vector database (Pinecone, Weaviate) or a pgvector service you operate.

**Core idea:** vectors live *in the same row store* as your data: `emb F32_BLOB(d)` +
`vector_distance_cos(emb, vector32(?))`. Retrieval is a local query — no cross-service round-trip.

**Run:**
```bash
cascade vector               # synthetic: latency curve as rows grow, self-NN correctness
cascade search "your question" --config configs/replica.toml   # live: embed -> co-located top-k -> LLM
```
**Proves it:** `[retrieval]` ≈ 1–3 ms in `cascade search` output — local, no network. (Honest limit:
Turso's own search is brute-force, so latency grows ~linearly; size the edge corpus to ~10k–100k
chunks.) **Breaking the ceiling:** `cascade vector` now also builds an in-memory **HNSW** index
(`hnsw_rs`) over the same vectors and reports the `ann` block — `recall@10` vs brute-force ground
truth + latency — the path to millions of vectors per edge while Turso-native ANN is pending.

---

<a name="p4"></a>
## P4 · AI distribution — embed once on the GPU, fan out everywhere

**Replaces:** running an embedding model (and a GPU, or a paid embedding API) on every node that
needs semantic search.

**Core idea — the inversion:** put the GPU where the *producer* is, not where the *queries* are. The
producer (e.g. a PC with a 3070) embeds each document **once**, writes the finished `F32_BLOB`
vector into Turso, and `push()`es. Every consumer edge `pull()`s the completed vectors and runs
brute-force cosine **on CPU, with no model and no GPU**. You distribute *results*, not *compute*.

```
        producer (GPU)                         consumers (CPU, no GPU, no embed model)
   ┌────────────────────────┐   one-primary    ┌─────────┐  ┌─────────┐  ┌─────────┐
   │ embed doc once (3070)  │  → many-replica   │ edge A  │  │ edge B  │  │ edge C  │
   │ Turso doc_vectors  ────┼──── sync :8080 ──▶│ pull()  │  │ pull()  │  │ pull()  │
   │ push()                 │                   │ cosine  │  │ cosine  │  │ cosine  │
   └────────────────────────┘                   └─────────┘  └─────────┘  └─────────┘
```

Only the **query** still needs embedding (tiny — a 384-d `all-minilm` runs fine on a consumer CPU; or
embed the query on the producer if a consumer has no model at all). The expensive part — embedding
the whole corpus — happens once, on one GPU.

**Run** (producer on the GPU box, then any number of consumers — each its own `replica.toml` with a
distinct `node.db`):
```bash
# Producer (PC, 3070): spawn hub, embed the live firehose, push finished vectors
cascade serve  --config configs/master.toml

# Consumer N (each its own replica, no GPU): pull + query
cascade search "..." --config configs/replica.toml      # edge_A.toml, edge_B.toml, …
```
**Proves it:** every consumer answers from local vectors it never computed; add edges by copying
`replica.toml` with a new `node.db` pointing at the same master. Validated locally with `./test.sh`
and two independent edges. See [`LAB.md`](LAB.md) for the full two-machine setup.

---

<a name="p5"></a>
## P5 · Turso as a lakehouse source

**Replaces:** a bespoke ingestion pipeline feeding Iceberg.

**Core idea:** Turso isn't the analytics engine — it's a clean *source* into the open lakehouse
standard. The CDC stream lands in a real Apache Iceberg table (and DuckDB for interactive query),
so analytics never touches the live OLTP edge.

**Run:**
```bash
cascade cdc-to-olap                          # turso_cdc -> DuckDB + a real Iceberg table (SqlCatalog)
cascade drain --config configs/master.toml   # live: docs CDC -> DuckDB (OLAP lane)
```
**Proves it:** `iceberg_rows == duckdb_rows` (`consistent: true`) and the DuckDB-vs-Turso lane gap in
`cascade olap` — DuckDB wins analytics by design; Turso feeds it.

---

## How to read the results

Every command writes `results/<name>.json` and the benchmark phases roll up into
[`docs/REPORT.md`](docs/REPORT.md). Numbers are reproducible: synthetic by default, or set
`PATENTS_JSONL` / run the live lab for real data. New to the repo? Start with
[README](README.md) → [`LAB.md`](LAB.md) → this file.
