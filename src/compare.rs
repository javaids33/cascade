//! A1: competitor / approach comparison for the "CDC without a broker" claim (Pattern P2).
//!
//! Runs the SAME insert workload three ways on a local Turso db and reports throughput + storage:
//!   - `none`    : plain inserts, no change capture (baseline)
//!   - `pragma`  : Turso's built-in CDC  (`PRAGMA capture_data_changes_conn('full')`)
//!   - `trigger` : the classic hand-rolled SQLite pattern — an AFTER INSERT trigger copying each
//!                 row into a shadow table (what you'd do on stock SQLite without built-in CDC)
//!
//! Same engine + same workload, so the delta isolates the *capture approach*, not the engine: the
//! built-in PRAGMA vs per-row trigger writes vs nothing. For the full cross-engine comparison
//! (Postgres + Debezium, SQLite + Litestream) see `docker/compare/` — that one needs Linux + Docker.
//!
//! Usage: `cascade compare-cdc [limit]`  ->  results/compare_cdc.json

use std::time::Instant;

use anyhow::Result;
use serde_json::{json, Value as J};
use turso::{Builder, Value};

use crate::common::{
    db_dir, db_footprint, ensure_dirs, n_patents, patents_rows, remove_db_files, round,
    save_result, PATENTS_DDL, PATENTS_INSERT,
};

const BATCH: usize = 5000;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    None,
    Pragma,
    Trigger,
}

impl Mode {
    fn key(self) -> &'static str {
        match self {
            Mode::None => "none",
            Mode::Pragma => "pragma",
            Mode::Trigger => "trigger",
        }
    }
}

// Shadow table mirroring the patents columns (full after-image) — the dependency-free way to
// capture changes on stock SQLite (no json1 required).
const SHADOW_DDL: &str = "CREATE TABLE patents_cdc (
  change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_type TEXT,
  patent_id TEXT, title TEXT, abstract TEXT, assignee TEXT, grant_date TEXT,
  grant_year INTEGER, claims_len INTEGER, n_cpc INTEGER, n_citations INTEGER)";
const SHADOW_TRIGGER: &str = "CREATE TRIGGER patents_ai AFTER INSERT ON patents BEGIN
  INSERT INTO patents_cdc(change_type, patent_id, title, abstract, assignee, grant_date,
    grant_year, claims_len, n_cpc, n_citations)
  VALUES ('INSERT', NEW.patent_id, NEW.title, NEW.abstract, NEW.assignee, NEW.grant_date,
    NEW.grant_year, NEW.claims_len, NEW.n_cpc, NEW.n_citations);
END";

async fn scalar_i64(conn: &turso::Connection, sql: &str) -> Result<i64> {
    let mut rows = conn.query(sql, ()).await?;
    if let Some(row) = rows.next().await? {
        if let Value::Integer(i) = row.get_value(0)? {
            return Ok(i);
        }
    }
    Ok(0)
}

async fn run_one(mode: Mode, rows: &[Vec<Value>]) -> Result<J> {
    let path = db_dir().join(format!("compare_{}.db", mode.key()));
    remove_db_files(&path);
    let db = Builder::new_local(path.to_str().unwrap()).build().await?;
    let conn = db.connect()?;

    if mode == Mode::Pragma {
        conn.execute("PRAGMA capture_data_changes_conn('full')", ()).await?;
    }
    conn.execute(PATENTS_DDL, ()).await?;
    if mode == Mode::Trigger {
        // If Turso's build lacks trigger support this errors here and the caller records it.
        conn.execute(SHADOW_DDL, ()).await?;
        conn.execute(SHADOW_TRIGGER, ()).await?;
    }

    let t0 = Instant::now();
    let mut stmt = conn.prepare(PATENTS_INSERT).await?;
    let mut n = 0usize;
    conn.execute("BEGIN", ()).await?;
    for (i, r) in rows.iter().enumerate() {
        stmt.execute(r.clone()).await?;
        n += 1;
        if (i + 1) % BATCH == 0 {
            conn.execute("COMMIT", ()).await?;
            conn.execute("BEGIN", ()).await?;
        }
    }
    conn.execute("COMMIT", ()).await?;
    let dt = t0.elapsed().as_secs_f64();

    let cdc_rows = match mode {
        Mode::None => 0,
        Mode::Pragma => scalar_i64(&conn, "SELECT COUNT(*) FROM turso_cdc").await?,
        Mode::Trigger => scalar_i64(&conn, "SELECT COUNT(*) FROM patents_cdc").await?,
    };
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", ()).await.ok();
    drop(stmt);
    let db_bytes = db_footprint(&path);
    Ok(json!({
        "approach": mode.key(),
        "supported": true,
        "rows": n,
        "seconds": round(dt, 3),
        "rows_per_sec": (n as f64 / dt).round() as i64,
        "cdc_rows": cdc_rows,
        "db_bytes": db_bytes,
    }))
}

/// Overhead/amplification of `m` vs the no-capture baseline `off`, folded into `m`.
fn with_deltas(mut m: J, off: &J) -> J {
    if m.get("supported").and_then(|v| v.as_bool()) != Some(true) {
        return m;
    }
    let off_rps = off["rows_per_sec"].as_f64().unwrap_or(0.0);
    let m_rps = m["rows_per_sec"].as_f64().unwrap_or(0.0);
    let off_bytes = off["db_bytes"].as_f64().unwrap_or(0.0).max(1.0);
    let m_bytes = m["db_bytes"].as_f64().unwrap_or(0.0);
    if let Some(o) = m.as_object_mut() {
        let overhead = if off_rps > 0.0 { (off_rps - m_rps) / off_rps * 100.0 } else { 0.0 };
        o.insert("throughput_overhead_pct".into(), json!(round(overhead, 1)));
        o.insert("storage_amplification_x".into(), json!(round(m_bytes / off_bytes, 2)));
    }
    m
}

pub async fn run(limit: Option<usize>) -> Result<()> {
    ensure_dirs()?;
    let limit = match limit {
        Some(l) => l,
        None => n_patents()?,
    };
    let rows = patents_rows(Some(limit))?;
    println!("loaded {} patent rows", rows.len());

    let off = run_one(Mode::None, &rows).await?;
    println!("none    : {off}");
    let pragma = with_deltas(run_one(Mode::Pragma, &rows).await?, &off);
    println!("pragma  : {pragma}");
    // Trigger CDC depends on Turso trigger support — degrade gracefully if missing.
    let trigger = match run_one(Mode::Trigger, &rows).await {
        Ok(j) => with_deltas(j, &off),
        Err(e) => json!({ "approach": "trigger", "supported": false, "error": e.to_string() }),
    };
    println!("trigger : {trigger}");

    let res = save_result(
        "compare_cdc",
        json!({
            "limit": limit,
            "batch": BATCH,
            "baseline_no_capture": off,
            "turso_builtin_cdc": pragma,
            "sqlite_trigger_cdc": trigger,
            "note": "same engine + workload; isolates capture approach. Cross-engine (Postgres+Debezium, \
                     SQLite+Litestream) lives in docker/compare/.",
        }),
    )?;
    println!("{}", serde_json::to_string_pretty(&res)?);
    Ok(())
}
