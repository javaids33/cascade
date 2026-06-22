//! Phase 3a: CDC capture overhead — insert patents into local Turso, CDC off vs on.
//! Usage: `cascade cdc-overhead [limit]`.

use std::time::Instant;

use anyhow::Result;
use serde_json::json;
use turso::{Builder, Value};

use crate::common::{
    db_dir, db_footprint, ensure_dirs, n_patents, patents_rows, remove_db_files, round,
    save_result, PATENTS_DDL, PATENTS_INSERT,
};

const BATCH: usize = 5000;

async fn scalar_i64(conn: &turso::Connection, sql: &str) -> Result<i64> {
    let mut rows = conn.query(sql, ()).await?;
    if let Some(row) = rows.next().await? {
        if let Value::Integer(i) = row.get_value(0)? {
            return Ok(i);
        }
    }
    Ok(0)
}

async fn run_one(cdc: bool, rows: &[Vec<Value>]) -> Result<serde_json::Value> {
    let path = db_dir().join(format!("cdc_{}.db", if cdc { "on" } else { "off" }));
    remove_db_files(&path);
    let db = Builder::new_local(path.to_str().unwrap()).build().await?;
    let conn = db.connect()?;
    if cdc {
        conn.execute("PRAGMA capture_data_changes_conn('full')", ()).await?;
    }
    conn.execute(PATENTS_DDL, ()).await?;

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

    let cdc_rows = if cdc {
        scalar_i64(&conn, "SELECT COUNT(*) FROM turso_cdc").await?
    } else {
        0
    };
    // Fold the WAL into the main db so the file size reflects everything written.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", ()).await.ok();
    drop(stmt);
    let db_bytes = db_footprint(&path);
    Ok(json!({
        "rows": n,
        "seconds": round(dt, 3),
        "rows_per_sec": (n as f64 / dt).round() as i64,
        "cdc_rows": cdc_rows,
        "db_bytes": db_bytes,
    }))
}

pub async fn run(limit: Option<usize>) -> Result<()> {
    ensure_dirs()?;
    let limit = match limit {
        Some(l) => l,
        None => n_patents()?,
    };
    let rows = patents_rows(Some(limit))?;
    println!("loaded {} patent rows", rows.len());

    let off = run_one(false, &rows).await?;
    println!("CDC off: {off}");
    let on = run_one(true, &rows).await?;
    println!("CDC on : {on}");

    let off_rps = off["rows_per_sec"].as_f64().unwrap();
    let on_rps = on["rows_per_sec"].as_f64().unwrap();
    let overhead = (off_rps - on_rps) / off_rps * 100.0;
    let amp = on["db_bytes"].as_f64().unwrap() / off["db_bytes"].as_f64().unwrap().max(1.0);

    save_result(
        "cdc_overhead",
        json!({
            "limit": limit,
            "batch": BATCH,
            "cdc_off": off,
            "cdc_on": on,
            "throughput_overhead_pct": round(overhead, 1),
            "storage_amplification_x": round(amp, 2),
        }),
    )?;
    println!(
        "\nCDC overhead: {:.1}% throughput, {:.2}x storage",
        overhead, amp
    );
    Ok(())
}
