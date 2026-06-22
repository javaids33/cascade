# Contributing

This is an **experimental, open guide** to replacing heavy data infra with Turso. Contributions that
make a pattern clearer, more honest, or easier to onboard to are very welcome — especially:

- **New patterns** (a slice of infra Turso collapses) — add a section to [`PATTERNS.md`](PATTERNS.md)
  and a `cascade` subcommand that demonstrates it with a measurable result.
- **Competitor comparisons** — stand up the heavy stack a pattern replaces (Postgres+Debezium,
  pgvector, Litestream, …) and put real numbers next to ours. This is the most-wanted contribution.
- **Data sources** for the live lab (`src/ingest.rs`) — any open, streaming feed.
- **Honesty fixes** — if a claim overstates, correct it. We'd rather be right than impressive.

## Dev setup (5 minutes)

```bash
# Rust >= 1.92, then:
./setup.sh                 # downloads the tursodb CLI + cargo build --release
cargo build                # debug build for iterating
./target/debug/cascade --help
```

For the live lab / node ops you also need [Ollama](https://ollama.com) + `ollama pull all-minilm`.

**Run the post-build gate before pushing** — it's the contract that configs still work:

```bash
cargo test            # config-contract tests (no infra) — guards how users pass configs
./test.sh             # full gate: config contract + health + master role + replica role
```

`tests/config_cases.rs` must pass for any change to `Config`/the example configs. If you change the
config shape, update the shipped `configs/*.toml` and the embedded `example_*` strings together.

## Layout

```
src/
  main.rs        clap CLI: one subcommand per phase/pattern
  common.rs      paths/env, Parquet IO, turso<->arrow, results JSON
  <phase>.rs     gen_synthetic, prep_data, cdc_overhead, cdc_to_olap, replication, olap, vector, report
  ingest.rs rag.rs lab_olap.rs ollama.rs labdb.rs   # the live "Living Knowledge Base" lab
docker/          Dockerfile + compose for master/edge
PATTERNS.md      the catalog (start here)
LAB.md           the two-machine lab guide
CLAUDE.md        build internals, crate gotchas, known gaps
```

## Adding a pattern (the shape)

1. Add `src/<pattern>.rs` with a `pub async fn run(...)` that does the thing and calls
   `common::save_result("<name>", json!({...}))`.
2. Wire a subcommand in `src/main.rs`.
3. Add a `PATTERNS.md` section: **Replaces / Core idea / Run / Proves it** (the metric).
4. Keep config in env (see `src/ollama.rs`, `src/labdb.rs`) so it's tunable without recompiles.

## Conventions

- Results are JSON in `results/` (committed) + rolled into `docs/REPORT.md` by `cascade report`.
- No hardcoded machine paths — derive from `TEO_REPO_ROOT` / `TURSO_EXP_HOME` / env.
- Pin the `turso` crate and `TURSO_VERSION` (setup.sh) in lockstep — the sync protocol is
  version-sensitive.
- State the honest cost/limit of every pattern. Turso is BETA; vector search is brute-force.

## Status & license

Experimental evaluation harness. Turso itself is BETA. See `CLAUDE.md` for known gaps.
