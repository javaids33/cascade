# Cascade: can one embedded database be the whole edge data stack?

*A thought experiment, and what happened when we actually built it.*

## The premise

A modern "data app" usually means a pile of services: Postgres for OLTP, Debezium + Kafka for
change capture, a warehouse (Snowflake/DuckDB/Iceberg) for analytics, and a vector database
(Pinecone/Weaviate) for semantic search. Each is its own deployment, its own ops burden, its own
copy of the data to keep in sync.

The thought experiment: **what if one embedded, SQLite-compatible binary did all of it** — OLTP +
change data capture + native replication + co-located vector search + an analytics lane — and could
run *on the edge*? [Turso](https://github.com/tursodatabase/turso) (a Rust rewrite of SQLite, beta)
has the primitives. We wired them into a single binary called **`cascade`** and pushed on it.

## The pattern we wanted to prove: AI distribution

The headline use case is edge RAG:

```
  GPU producer (master)                         CPU edges (replicas)
  ─────────────────────                         ────────────────────
  ingest a firehose                             pull finished vectors
  → embed ONCE on the GPU      ── push ──▶      → search LOCALLY, no GPU
  → capture CDC → DuckDB                        → answer with a small local LLM
  → push vectors to edges
```

Embed once where the GPU is; search everywhere on cheap CPU nodes. The vectors live **in the same
database file** as the data, so an edge query is a local SQL statement — no separate vector service,
no network hop to a vector DB.

## What we built

- **One binary, two roles** (`cascade serve` / `cascade search`), driven by a TOML config
  (`role = master | replica`). The master spawns Turso's sync hub, ingests the live Wikipedia
  EventStreams firehose, embeds each edit via Ollama, captures CDC, drains to DuckDB + Apache
  Iceberg, and pushes vectors. The replica pulls and runs co-located vector search.
- **A reproducible benchmark harness** (CDC overhead, replication, OLAP, vector latency).
- **A fleet observability layer** we built to debug the live demo: a per-node telemetry agent, a
  single-file web **dashboard** (topology + master→hub→replica communication panel + live logs), a
  `/fleet-telemetry` aggregator, a terminal view, and an MCP server.

## What we tested — and the numbers

Synthetic data, 20k rows, on a developer box (Windows + WSL2 on NTFS — i.e. conservative; native
Linux/ext4 is faster).

**Change data capture costs about what you'd hope.** Turning CDC on (full before/after row images,
in-process):

| | inserts/sec | storage |
|---|---|---|
| CDC off | 98,292 | 13.7 MB |
| CDC on  | 55,944 | 27.6 MB |

~43% write-throughput cost and ~2× storage — for a `PRAGMA`, versus standing up a Debezium + Kafka
cluster. You still get ~56k inserts/sec with capture on.

**Co-located vector search is fast at edge scale** (linear scan, dim 64, nearest-neighbor accuracy
verified):

| rows | mean | p95 |
|---|---|---|
| 1,000  | 0.4 ms | 0.42 ms |
| 5,000  | 2.2 ms | 3.4 ms |
| 10,000 | 4.6 ms | 5.4 ms |
| 20,000 | 9.6 ms | 10.2 ms |

Sub-10 ms to 20k vectors in the same file, no separate service. It scales **linearly** (~0.48 ms per
1k rows) — there's no ANN index in Turso 0.6.1, so this is honest for small/medium edge sets, not
billion-scale.

**The analytics lane earns its place.** Same queries, DuckDB (fed by the CDC drain) vs the OLTP
engine:

| query | DuckDB | Turso (OLTP) | speedup |
|---|---|---|---|
| grants per year      | 0.7 ms | 2.3 ms    | 3.5× |
| top CPC sections     | 1.1 ms | 53.7 ms   | 50× |
| most cited           | 11 ms  | 546 ms    | 49× |
| citations by assignee| 14 ms  | 2,173 ms  | **155×** |

Draining CDC into a columnar engine isn't just tidy architecture — it's **1–2 orders of magnitude**
faster, and it keeps analytics off the live edge database. The drain itself is consistent: 20,000
changes applied at ~519/s, and `duckdb_rows == iceberg_rows == source` (a real Iceberg table,
verified consistent).

**It runs across two real machines.** A Windows PC with a GPU as the master and a MacBook as a CPU
edge: the master embedded and pushed **11,936 docs with 0 push failures**, auto-recovering from **6**
firehose disconnects, while the Mac pulled those vectors and searched them in **~47 ms** — having
embedded nothing itself. That's the AI-distribution pattern, working end to end.

## What we did *not* prove — the honest limits

- **Sync is one-way (master → replica).** No multi-primary, no write-back, no conflict resolution.
  The edge is a read replica. We did *not* test (and the architecture doesn't support) replicas
  writing back to the master. Don't read "distribution" as "bidirectional."
- **Vectors are linear-scan** — no ANN index in 0.6.1. Great to tens of thousands of rows per edge;
  not a billion-scale vector DB.
- **Turso is beta.** Frame reliability accordingly.
- **A real bug we hit:** pushing the `F32_BLOB` vector column intermittently failed in the sync
  engine (`column type mismatch … Blob`) during hub bootstrap. It self-healed in our runs (0 net
  push failures), but vector replication can be lossy under churn — a beta sharp edge.
- **No competitor pipeline was stood up.** The comparisons are conceptual plus our own numbers; we
  didn't benchmark SQLite+Litestream or Postgres+Debezium head-to-head. That's the obvious next step.

## Where it leaves us

The thought experiment mostly holds: **one embedded binary genuinely covered OLTP + CDC + native
replication + co-located vectors + a consistent OLAP/Iceberg lane**, and the GPU-producer →
CPU-edge pattern worked across two machines. The wins that surprised us were the *integration* wins —
no glue services, vectors and data in one file, CDC as a pragma — more than raw single-number speed.

It is not a drop-in replacement for a mature multi-master Postgres or an ANN vector DB. It's a
compelling **edge** stack: zero-ops, runs anywhere SQLite does, and collapses four services into one.

## Reproduce it

```bash
./setup.sh && ./run.sh          # synthetic data, full pipeline + benchmarks → docs/REPORT.md
# two machines:
./start-master.cmd  (Windows)   # or ./start-master.sh on Linux/macOS — GPU producer
./start-replica.sh  (the edge)  # pulls + searches
# watch it: open the master agent's http://<host>:7071/ dashboard
```

*Built as an exploration; numbers are from our hardware and will vary. Turso is beta — treat
accordingly.*
