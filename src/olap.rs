//! Phase 4: OLAP lane comparison — same analytical queries on DuckDB (columnar) vs Turso (row
//! store). Loads patents/cpc/citations into both, indexes Turso for fairness, times each query.
//! Usage: `cascade olap`.

use std::time::Instant;

use anyhow::Result;
use serde_json::json;
use turso::Builder;

use crate::common::{
    batches_to_turso_rows, data_dir, db_dir, ensure_dirs, read_parquet, remove_db_files, round,
    save_result,
};

const QUERIES: &[(&str, &str)] = &[
    (
        "Q1_top_assignees",
        "SELECT assignee, COUNT(*) c FROM patents WHERE assignee IS NOT NULL \
         GROUP BY assignee ORDER BY c DESC LIMIT 10",
    ),
    (
        "Q2_grants_per_year",
        "SELECT grant_year, COUNT(*) c FROM patents GROUP BY grant_year ORDER BY grant_year",
    ),
    (
        "Q3_top_cpc_sections",
        "SELECT cpc_section, COUNT(*) c FROM patent_cpc GROUP BY cpc_section ORDER BY c DESC LIMIT 10",
    ),
    (
        "Q4_most_cited",
        "SELECT cited_id, COUNT(*) c FROM patent_citations GROUP BY cited_id ORDER BY c DESC LIMIT 10",
    ),
    (
        "Q5_citations_by_assignee",
        "SELECT p.assignee, COUNT(*) c FROM patent_citations x \
         JOIN patents p ON x.patent_id = p.patent_id \
         WHERE p.assignee IS NOT NULL GROUP BY p.assignee ORDER BY c DESC LIMIT 10",
    ),
];

fn build_duckdb(path: &std::path::Path) -> Result<duckdb::Connection> {
    for ext in ["", ".wal"] {
        let _ = std::fs::remove_file(format!("{}{}", path.display(), ext));
    }
    let conn = duckdb::Connection::open(path)?;
    for t in ["patents", "patent_cpc", "patent_citations"] {
        let pf = data_dir().join(format!("{t}.parquet"));
        conn.execute_batch(&format!(
            "CREATE TABLE {t} AS SELECT * FROM read_parquet('{}')",
            pf.display()
        ))?;
    }
    Ok(conn)
}

fn timeit_duck(conn: &duckdb::Connection, sql: &str, runs: usize) -> Result<f64> {
    let mut best = f64::INFINITY;
    for _ in 0..runs {
        let t = Instant::now();
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query([])?;
        while rows.next()?.is_some() {}
        best = best.min(t.elapsed().as_secs_f64());
    }
    Ok(best)
}

async fn build_turso(path: &std::path::Path) -> Result<(turso::Connection, serde_json::Value)> {
    remove_db_files(path);
    let db = Builder::new_local(path.to_str().unwrap()).build().await?;
    let conn = db.connect()?;
    conn.execute(
        "CREATE TABLE patents(patent_id TEXT PRIMARY KEY, title TEXT, abstract TEXT, \
         assignee TEXT, grant_date TEXT, grant_year INTEGER, claims_len INTEGER, \
         n_cpc INTEGER, n_citations INTEGER)",
        (),
    )
    .await?;
    conn.execute("CREATE TABLE patent_cpc(patent_id TEXT, cpc TEXT, cpc_section TEXT)", ())
        .await?;
    conn.execute("CREATE TABLE patent_citations(patent_id TEXT, cited_id TEXT)", ())
        .await?;

    let t0 = Instant::now();
    let specs = [
        ("patents", 9, "patents.parquet"),
        ("patent_cpc", 3, "patent_cpc.parquet"),
        ("patent_citations", 2, "patent_citations.parquet"),
    ];
    let mut total = 0usize;
    for (tname, ncol, pf) in specs {
        let ph = vec!["?"; ncol].join(",");
        let sql = format!("INSERT INTO {tname} VALUES ({ph})");
        let batches = read_parquet(&data_dir().join(pf))?;
        let rows = batches_to_turso_rows(&batches);
        let mut stmt = conn.prepare(&sql).await?;
        conn.execute("BEGIN", ()).await?;
        for (i, r) in rows.iter().enumerate() {
            stmt.execute(r.clone()).await?;
            if (i + 1) % 20_000 == 0 {
                conn.execute("COMMIT", ()).await?;
                conn.execute("BEGIN", ()).await?;
            }
        }
        conn.execute("COMMIT", ()).await?;
        total += rows.len();
    }
    // indexes for fair OLAP play on the row store
    for idx in [
        "CREATE INDEX ix_p_assignee ON patents(assignee)",
        "CREATE INDEX ix_p_year ON patents(grant_year)",
        "CREATE INDEX ix_cpc_sec ON patent_cpc(cpc_section)",
        "CREATE INDEX ix_cit_cited ON patent_citations(cited_id)",
        "CREATE INDEX ix_cit_pid ON patent_citations(patent_id)",
    ] {
        conn.execute(idx, ()).await?;
    }
    let load = t0.elapsed().as_secs_f64();
    let db_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let stats = json!({
        "rows_loaded": total,
        "load_sec": round(load, 2),
        "load_rows_per_sec": (total as f64 / load).round() as i64,
        "db_bytes": db_bytes,
    });
    Ok((conn, stats))
}

async fn timeit_turso(conn: &turso::Connection, sql: &str, runs: usize) -> Result<f64> {
    let mut best = f64::INFINITY;
    for _ in 0..runs {
        let t = Instant::now();
        let mut rows = conn.query(sql, ()).await?;
        while rows.next().await?.is_some() {}
        best = best.min(t.elapsed().as_secs_f64());
    }
    Ok(best)
}

pub async fn run() -> Result<()> {
    ensure_dirs()?;
    let turso_db = db_dir().join("analytics.db");
    let duck_db = db_dir().join("analytics.duckdb");

    println!("building DuckDB...");
    let duck = build_duckdb(&duck_db)?;
    println!("building Turso analytics db (bulk load + indexes)...");
    let (turso_con, load_stats) = build_turso(&turso_db).await?;
    println!("turso load: {load_stats}");

    let mut results = serde_json::Map::new();
    for (name, sql) in QUERIES {
        let dt_duck = timeit_duck(&duck, sql, 3)?;
        let dt_turso = timeit_turso(&turso_con, sql, 3).await?;
        let ratio = if dt_duck > 0.0 {
            round(dt_turso / dt_duck, 1)
        } else {
            0.0
        };
        results.insert(
            name.to_string(),
            json!({
                "duckdb_ms": round(dt_duck * 1000.0, 1),
                "turso_ms": round(dt_turso * 1000.0, 1),
                "turso_slower_x": ratio,
            }),
        );
        println!(
            "  {:26} duckdb={:8.1}ms  turso={:9.1}ms  ({}x)",
            name,
            dt_duck * 1000.0,
            dt_turso * 1000.0,
            ratio
        );
    }

    let duck_bytes = std::fs::metadata(&duck_db).map(|m| m.len()).unwrap_or(0);
    save_result(
        "olap",
        json!({
            "turso_load": load_stats,
            "queries": serde_json::Value::Object(results),
            "duckdb_bytes": duck_bytes,
        }),
    )?;
    Ok(())
}
