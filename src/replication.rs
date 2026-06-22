//! Phase 2: master->replica native replication at scale. Master (sync client) ingests patents
//! and push()es to the sync server; replica (sync client) bootstraps + pull()s. Measures
//! throughput, sync latency, bytes, and incremental sync. Requires a running `tursodb
//! --sync-server` (run-all spawns it). Usage: `cascade replication [n] [delta]`.

use std::time::Instant;

use anyhow::Result;
use serde_json::json;
use turso::sync::Builder as SyncBuilder;
use turso::Value;

use crate::common::{
    db_dir, ensure_dirs, n_patents, patents_rows, remote_url, remove_db_files, round, save_result,
    PATENTS_DDL, PATENTS_INSERT,
};

const BATCH: usize = 5000;

async fn open_synced(path: &str, remote: &str) -> Result<turso::sync::Database> {
    let db = SyncBuilder::new_remote(path)
        .with_remote_url(remote)
        .bootstrap_if_empty(true)
        .build()
        .await?;
    Ok(db)
}

async fn count_patents(conn: &turso::Connection) -> Result<i64> {
    let mut rows = conn.query("SELECT COUNT(*) FROM patents", ()).await?;
    if let Some(row) = rows.next().await? {
        if let Value::Integer(i) = row.get_value(0)? {
            return Ok(i);
        }
    }
    Ok(0)
}

pub async fn run(n_arg: Option<usize>, delta_arg: Option<usize>) -> Result<()> {
    ensure_dirs()?;
    let total = n_patents()?;
    let mut n = n_arg.unwrap_or(50_000).min(total);
    let mut delta = delta_arg.unwrap_or(5000).min(total.saturating_sub(n));
    if delta == 0 && total > 1 {
        n = ((total as f64) * 0.9) as usize;
        n = n.max(1);
        delta = total - n;
    }
    let remote = remote_url();
    let master = db_dir().join("repl_master.db");
    let replica = db_dir().join("repl_replica.db");
    remove_db_files(&master);
    remove_db_files(&replica);

    let rows = patents_rows(Some(n + delta))?;
    println!("loaded {} rows; remote={remote}", rows.len());

    // --- master: DDL + initial ingest ---
    let m = open_synced(master.to_str().unwrap(), &remote).await?;
    let mconn = m.connect().await?;
    mconn.execute(PATENTS_DDL, ()).await?;
    m.push().await?;

    let t0 = Instant::now();
    let mut stmt = mconn.prepare(PATENTS_INSERT).await?;
    mconn.execute("BEGIN", ()).await?;
    for (i, r) in rows.iter().take(n).enumerate() {
        stmt.execute(r.clone()).await?;
        if (i + 1) % BATCH == 0 {
            mconn.execute("COMMIT", ()).await?;
            mconn.execute("BEGIN", ()).await?;
        }
    }
    mconn.execute("COMMIT", ()).await?;
    let t_ingest = t0.elapsed().as_secs_f64();

    let t1 = Instant::now();
    m.push().await?;
    let t_push = t1.elapsed().as_secs_f64();
    let ms = m.stats().await?;

    // --- replica: bootstrap pull ---
    let t2 = Instant::now();
    let r = open_synced(replica.to_str().unwrap(), &remote).await?;
    let rconn = r.connect().await?;
    r.pull().await?;
    let t_boot = t2.elapsed().as_secs_f64();
    let rcount = count_patents(&rconn).await?;
    let rs = r.stats().await?;

    // --- incremental delta ---
    let t3 = Instant::now();
    mconn.execute("BEGIN", ()).await?;
    for r in rows.iter().skip(n).take(delta) {
        stmt.execute(r.clone()).await?;
    }
    mconn.execute("COMMIT", ()).await?;
    m.push().await?;
    let t_delta_push = t3.elapsed().as_secs_f64();
    let ms2 = m.stats().await?;

    let t4 = Instant::now();
    r.pull().await?;
    let t_delta_pull = t4.elapsed().as_secs_f64();
    let rcount2 = count_patents(&rconn).await?;
    let rs2 = r.stats().await?;

    let res = save_result(
        "replication",
        json!({
            "n_initial": n,
            "delta": delta,
            "master_ingest_sec": round(t_ingest, 3),
            "master_ingest_rows_per_sec": (n as f64 / t_ingest).round() as i64,
            "master_push_sec": round(t_push, 3),
            "master_push_bytes_sent": ms.network_sent_bytes,
            "replica_bootstrap_pull_sec": round(t_boot, 3),
            "replica_rows_after_bootstrap": rcount,
            "replica_bootstrap_bytes_recv": rs.network_received_bytes,
            "incremental_push_sec": round(t_delta_push, 3),
            "incremental_push_bytes": ms2.network_sent_bytes.saturating_sub(ms.network_sent_bytes),
            "incremental_pull_sec": round(t_delta_pull, 3),
            "incremental_pull_bytes": rs2.network_received_bytes.saturating_sub(rs.network_received_bytes),
            "replica_rows_after_delta": rcount2,
            "converged": rcount2 == (n + delta) as i64,
        }),
    )?;
    println!("{}", serde_json::to_string_pretty(&res)?);
    Ok(())
}
