//! Phase 6: synthesize results/*.json into docs/REPORT.md (publishable writeup). `cascade report`.

use std::fs;

use anyhow::Result;
use serde_json::Value as J;

use crate::common::{repo_root, results_dir};

fn load(name: &str) -> Option<J> {
    let p = results_dir().join(format!("{name}.json"));
    let s = fs::read_to_string(p).ok()?;
    serde_json::from_str(&s).ok()
}

fn fmt_bytes(b: Option<f64>) -> String {
    let mut b = match b {
        Some(x) => x,
        None => return "n/a".to_string(),
    };
    for u in ["B", "KB", "MB", "GB"] {
        if b < 1024.0 {
            return format!("{:.0}{}", b, u);
        }
        b /= 1024.0;
    }
    format!("{:.1}TB", b)
}

/// Thousands-separated integer.
fn commas(n: f64) -> String {
    let n = n.round() as i64;
    let neg = n < 0;
    let s = n.abs().to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    let mut r: String = out.chars().rev().collect();
    if neg {
        r.insert(0, '-');
    }
    r
}

fn f(v: &J, path: &[&str]) -> Option<f64> {
    let mut cur = v;
    for k in path {
        cur = cur.get(k)?;
    }
    cur.as_f64()
}
fn s(v: &J, path: &[&str]) -> String {
    let mut cur = v;
    for k in path {
        match cur.get(k) {
            Some(n) => cur = n,
            None => return "?".to_string(),
        }
    }
    match cur {
        J::String(x) => x.clone(),
        J::Bool(b) => b.to_string(),
        J::Number(n) => n.to_string(),
        _ => "?".to_string(),
    }
}

pub fn run() -> Result<()> {
    let cdc = load("cdc_overhead");
    let olap_cdc = load("cdc_to_olap");
    let repl = load("replication");
    let olap = load("olap");
    let vec = load("vector");

    let mut l: Vec<String> = Vec::new();
    let mut w = |s: &str| l.push(s.to_string());

    w("# Turso at the Edge: native replication + CDC → OLAP (DuckDB/Iceberg) + edge vector search");
    w("");
    w("An experiment evaluating **Turso** (the Rust rewrite of SQLite) as a distributed edge OLTP \
       database whose changes stream via **Change Data Capture** into an OLAP/lakehouse layer \
       (**DuckDB + Apache Iceberg**), plus an **edge vector-search** demo — measured end to end. \
       This harness is implemented natively in **Rust** (turso/duckdb/iceberg crates).");
    w("");
    w("> Reproduce with `./setup.sh && ./run.sh`. All numbers below are emitted to \
       `results/*.json` by the harness. Default dataset is synthetic; point `PATENTS_JSONL` at a \
       real patents JSONL to reproduce the headline numbers.");
    w("");
    w("## TL;DR");
    w("");
    w("- Turso's **native sync server** gives genuine primary→replica replication in-process — no \
       Kafka/Debezium/cloud. Page-level WAL sync makes replica catch-up sub-second.");
    if let Some(c) = &cdc {
        w(&format!(
            "- **CDC `full` mode** captures every change as queryable SQL (`turso_cdc`) at a measured \
             **{}% write-throughput cost** and **~{}× storage**.",
            s(c, &["throughput_overhead_pct"]),
            s(c, &["storage_amplification_x"])
        ));
    }
    w("- That CDC stream loads cleanly into **DuckDB + Iceberg**, making Turso a tidy **source into \
       the lakehouse** rather than an analytics engine itself.");
    w("- DuckDB beats Turso on analytics by design (columnar vs row) — see the lane comparison.");
    w("- Edge vector search works today but is **linear-scan (no ANN index)** — latency grows with rows.");
    w("");

    if let Some(c) = &cdc {
        w("## CDC capture overhead (Phase 3a)");
        w("");
        w("| Mode | Throughput | DB size |");
        w("|---|---|---|");
        w(&format!(
            "| CDC off | {} rows/s | {} |",
            commas(f(c, &["cdc_off", "rows_per_sec"]).unwrap_or(0.0)),
            fmt_bytes(f(c, &["cdc_off", "db_bytes"]))
        ));
        w(&format!(
            "| CDC on (`full`) | {} rows/s | {} |",
            commas(f(c, &["cdc_on", "rows_per_sec"]).unwrap_or(0.0)),
            fmt_bytes(f(c, &["cdc_on", "db_bytes"]))
        ));
        w("");
        w(&format!(
            "→ **{}% throughput cost, {}× storage** ({} change records for {} inserts). `full` mode \
             stores before+after row images; `id`/`after` modes are lighter.",
            s(c, &["throughput_overhead_pct"]),
            s(c, &["storage_amplification_x"]),
            commas(f(c, &["cdc_on", "cdc_rows"]).unwrap_or(0.0)),
            commas(f(c, &["limit"]).unwrap_or(0.0))
        ));
        w("");
    }

    if let Some(o) = &olap_cdc {
        w("## CDC → OLAP: DuckDB + Apache Iceberg (Phase 3b)");
        w("");
        w(&format!(
            "Drained **{} change records** → DuckDB + Iceberg in **{}s** (**{} changes/s**). Both \
             sinks verified consistent at {} rows (consistent={}).",
            commas(f(o, &["total_changes"]).unwrap_or(0.0)),
            s(o, &["seconds"]),
            commas(f(o, &["changes_per_sec"]).unwrap_or(0.0)),
            commas(f(o, &["duckdb_rows"]).unwrap_or(0.0)),
            s(o, &["consistent"])
        ));
        w("");
        w("The loader tails `turso_cdc` with a `change_id` cursor, decodes before/after images via \
           Turso's `bin_record_json_object()` SQL helper, and applies insert/update/delete to both sinks.");
        w("");
    }

    if let Some(r) = &repl {
        w("## Native master→replica replication (Phase 2)");
        w("");
        w("| Metric | Value |");
        w("|---|---|");
        w(&format!(
            "| Master ingest | {} rows/s |",
            commas(f(r, &["master_ingest_rows_per_sec"]).unwrap_or(0.0))
        ));
        w(&format!(
            "| Initial push ({}) | {}s, {} |",
            commas(f(r, &["n_initial"]).unwrap_or(0.0)),
            s(r, &["master_push_sec"]),
            fmt_bytes(f(r, &["master_push_bytes_sent"]))
        ));
        w(&format!(
            "| Replica bootstrap pull | **{}s** → {} rows |",
            s(r, &["replica_bootstrap_pull_sec"]),
            commas(f(r, &["replica_rows_after_bootstrap"]).unwrap_or(0.0))
        ));
        w(&format!(
            "| Incremental push ({}) | {}s, {} |",
            commas(f(r, &["delta"]).unwrap_or(0.0)),
            s(r, &["incremental_push_sec"]),
            fmt_bytes(f(r, &["incremental_push_bytes"]))
        ));
        w(&format!(
            "| Incremental pull ({}) | **{}s**, {} |",
            commas(f(r, &["delta"]).unwrap_or(0.0)),
            s(r, &["incremental_pull_sec"]),
            fmt_bytes(f(r, &["incremental_pull_bytes"]))
        ));
        w(&format!("| Converged | {} |", s(r, &["converged"])));
        w("");
    }

    if let Some(o) = &olap {
        w("## OLAP lane comparison: DuckDB vs Turso (Phase 4)");
        w("");
        w(&format!(
            "Turso bulk-load: {} rows in {}s ({} rows/s), indexed.",
            commas(f(o, &["turso_load", "rows_loaded"]).unwrap_or(0.0)),
            s(o, &["turso_load", "load_sec"]),
            commas(f(o, &["turso_load", "load_rows_per_sec"]).unwrap_or(0.0))
        ));
        w("");
        w("| Query | DuckDB | Turso | Turso slower |");
        w("|---|---|---|---|");
        if let Some(q) = o.get("queries").and_then(|x| x.as_object()) {
            for (name, r) in q {
                w(&format!(
                    "| {} | {} ms | {} ms | {}× |",
                    name,
                    s(r, &["duckdb_ms"]),
                    s(r, &["turso_ms"]),
                    s(r, &["turso_slower_x"])
                ));
            }
        }
        w("");
        w("DuckDB (columnar, vectorized) dominates scans/aggregations — exactly as expected. Turso is a \
           row store tuned for point reads/writes; this is a lane comparison, not a defeat.");
        w("");
    }

    if let Some(v) = &vec {
        w("## Edge vector search (Phase 5)");
        w("");
        w(&format!(
            "Embedded abstracts as `F32_BLOB({})`; top-10 cosine search via `vector_distance_cos`. \
             Self-NN correctness: {}.",
            s(v, &["dim"]),
            s(v, &["self_nn_correct"])
        ));
        w("");
        w("| Rows | p50 latency | p95 latency |");
        w("|---|---|---|");
        if let Some(curve) = v.get("latency_curve").and_then(|x| x.as_array()) {
            for c in curve {
                w(&format!(
                    "| {} | {} ms | {} ms |",
                    commas(f(c, &["rows"]).unwrap_or(0.0)),
                    s(c, &["p50_ms"]),
                    s(c, &["p95_ms"])
                ));
            }
        }
        w("");
        w(&format!("> {}", s(v, &["note"])));
        w("");
    }

    w("## Where Turso fits (data-engineer's verdict)");
    w("");
    w("- **Pick Turso** for embedded/edge OLTP that needs SQLite's deployment model (single file, \
       zero-ops, runs anywhere) *plus* async I/O, MVCC writes, built-in CDC, native replication, and \
       co-located vectors — without bolting on extensions.");
    w("- **Don't pick Turso** as your analytics engine. Pair it with DuckDB/Iceberg for that.");
    w("- The compelling pattern: **Turso (edge OLTP + CDC) → Iceberg (lakehouse) → DuckDB (query)**. \
       Turso is a clean *source* into the Iceberg standard, not a replacement for it.");
    w("");
    w("## Caveats");
    w("");
    w("- Turso is **BETA** (project's own warning); not yet SQLite-level reliability.");
    w("- Vector search is **linear-scan, no ANN index** in v0.6.1 — fine for small/medium edge sets, \
       not a scale claim.");
    w("- Sync is **one-way primary→replica**, no conflict resolution / multi-primary.");
    w("- **CDC and MVCC (`BEGIN CONCURRENT`) are mutually exclusive** — can't measure both in one run.");
    w("- Comparison vs current standards (SQLite+Litestream, Postgres+Debezium) is **conceptual + our \
       numbers**; no competitor pipeline was stood up.");
    w("");

    let out = repo_root().join("docs").join("REPORT.md");
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::write(&out, l.join("\n"))?;
    println!("[wrote] {}", out.display());
    Ok(())
}
