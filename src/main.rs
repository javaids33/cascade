//! cascade — CLI for the cascade toolkit.
//!
//! Two command families:
//!   • node ops (config-driven):  init · serve · search · drain   — set up master/replica nodes
//!   • benchmarks (the "performance" use case): gen-synthetic, cdc-overhead, replication, …

mod cdc_overhead;
mod cdc_to_olap;
mod common;
mod gen_synthetic;
mod olap;
mod prep_data;
mod replication;
mod report;
mod vector;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use cascade::sync_server::SyncServer;
use cascade::{source, Config, Node};

#[derive(Parser)]
#[command(name = "cascade", about = "Turso-core toolkit: master/replica edge nodes with CDC->OLAP + vector fan-out")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    // ---- node ops (config-driven) ----
    /// Write a starter config (master by default, --replica for a replica).
    Init {
        #[arg(default_value = "node.toml")]
        path: String,
        #[arg(long)]
        replica: bool,
    },
    /// Master: open the node, (optionally) spawn the sync server, run the configured source.
    Serve {
        #[arg(long, default_value = "node.toml")]
        config: String,
    },
    /// Replica: pull latest, run co-located vector search, optionally generate an answer.
    Search {
        query: String,
        k: Option<usize>,
        #[arg(long, default_value = "node.toml")]
        config: String,
    },
    /// Master: drain the docs CDC stream -> DuckDB (OLAP lane) and print a summary.
    Drain {
        #[arg(long, default_value = "node.toml")]
        config: String,
    },
    /// Expose the node over a local HTTP API for clients in any language (Python/JS/Go).
    Gateway {
        #[arg(long, default_value = "node.toml")]
        config: String,
        #[arg(long, default_value = "127.0.0.1:7070")]
        bind: String,
    },

    // ---- benchmarks (performance use case) ----
    /// Generate synthetic patents Parquet (default n=50000 or env SYNTH_N).
    GenSynthetic { n: Option<usize> },
    /// Parse a real patents JSONL into Parquet (path or env PATENTS_JSONL).
    PrepData { path: Option<String>, limit: Option<usize> },
    /// CDC capture overhead (CDC off vs on).
    CdcOverhead { limit: Option<usize> },
    /// Drain turso_cdc -> DuckDB + Iceberg (patents schema).
    CdcToOlap { primary: Option<String> },
    /// Master->replica native replication (needs a sync server).
    Replication { n: Option<usize>, delta: Option<usize> },
    /// OLAP lane comparison (DuckDB vs Turso).
    Olap,
    /// Edge vector search latency curve.
    Vector { max_rows: Option<usize>, dim: Option<usize> },
    /// Synthesize results/*.json -> docs/REPORT.md.
    Report,
    /// Run every benchmark phase end to end (spawns a sync server).
    RunAll { n: Option<usize> },
}

fn tursodb_search_dirs() -> Vec<PathBuf> {
    let mut v = vec![common::work().join("bin")];
    if let Ok(cwd) = std::env::current_dir() {
        v.push(cwd);
    }
    v
}

// ---- node ops ----

fn cmd_init(path: &str, replica: bool) -> Result<()> {
    if std::path::Path::new(path).exists() {
        anyhow::bail!("{path} already exists — refusing to overwrite");
    }
    let body = if replica {
        Config::example_replica()
    } else {
        Config::example_master()
    };
    std::fs::write(path, body)?;
    println!("wrote {path} ({} config)", if replica { "replica" } else { "master" });
    println!("edit it, then:  cascade serve --config {path}   (master)   |   cascade search \"q\" --config {path}   (replica)");
    Ok(())
}

async fn cmd_serve(config: &str) -> Result<()> {
    let cfg = Config::from_path(config)?;
    if !cfg.is_master() {
        anyhow::bail!("`serve` requires node.role = master (use `search` on a replica)");
    }
    // spawn the sync hub if asked
    let _server = if cfg.sync.serve {
        let hub = common::db_dir().join("hub.db");
        common::remove_db_files(&hub);
        let s = SyncServer::start(&cfg.sync.bind, &hub.to_string_lossy(), &tursodb_search_dirs())?;
        println!("sync server up (pid {}) on {}", s.pid(), cfg.sync.bind);
        Some(s)
    } else {
        None
    };
    let node = Node::open(cfg.clone()).await?;
    println!("master node open: db={}", cfg.node.db);
    source::run(&node).await?;
    if cfg.olap.duckdb.is_some() {
        let st = node.drain_olap().await?;
        println!("OLAP drain: {} changes -> {} ({} rows)", st.changes, st.duckdb_path, st.duckdb_rows);
    }
    Ok(())
}

async fn cmd_search(query: &str, k: Option<usize>, config: &str) -> Result<()> {
    let cfg = Config::from_path(config)?;
    let node = Node::open(cfg.clone()).await?;
    let changed = node.pull().await.unwrap_or(false);
    let k = k.unwrap_or(5);
    let t = std::time::Instant::now();
    let hits = node.search(query, k).await?;
    let retrieval_ms = t.elapsed().as_secs_f64() * 1000.0;
    if hits.is_empty() {
        println!("no documents yet (pulled changed={changed}). Run a master `cascade serve` and let it sync.");
        return Ok(());
    }
    let answer = node.answer(query, &hits).await?;
    if !answer.is_empty() {
        println!("\n=== Answer ===\n{answer}\n");
    }
    println!("=== Sources (top {k}) ===");
    for (i, h) in hits.iter().enumerate() {
        let title = h.meta.get("title").and_then(|x| x.as_str()).unwrap_or(&h.text);
        let url = h.meta.get("url").and_then(|x| x.as_str()).unwrap_or("");
        println!("  [{}] (cos_dist={:.3}) {}  {}", i + 1, h.score, title, url);
    }
    println!("\n[retrieval] {retrieval_ms:.1}ms  (local, co-located vector search)");
    Ok(())
}

async fn cmd_drain(config: &str) -> Result<()> {
    let cfg = Config::from_path(config)?;
    let node = Node::open(cfg).await?;
    let st = node.drain_olap().await?;
    println!("drained {} changes -> {} ({} rows) in {:.2}s", st.changes, st.duckdb_path, st.duckdb_rows, st.seconds);
    let summary = Node::olap_summary(&st.duckdb_path)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

// ---- benchmarks ----

fn step(msg: &str) {
    println!("\n==================== {msg} ====================");
}

async fn run_all(n: Option<usize>) -> Result<()> {
    common::ensure_dirs()?;
    if let Some(src) = common::patents_jsonl() {
        step("Phase 1: prep real data from PATENTS_JSONL");
        prep_data::run(&src, None)?;
    } else {
        let synth = n
            .or_else(|| std::env::var("SYNTH_N").ok().and_then(|s| s.parse().ok()))
            .unwrap_or(50_000);
        step(&format!("Phase 1: generate synthetic data (n={synth})"));
        gen_synthetic::run(synth)?;
    }
    step("Phase 3a: CDC overhead");
    cdc_overhead::run(None).await?;
    step("Phase 3b: CDC -> OLAP (DuckDB + Iceberg)");
    cdc_to_olap::run(None).await?;
    step("Phase 2: master->replica replication");
    {
        let server = SyncServer::start(
            "127.0.0.1:8080",
            &common::db_dir().join("repl_server.db").to_string_lossy(),
            &tursodb_search_dirs(),
        )?;
        println!("sync server up (pid {})", server.pid());
        let repl = replication::run(None, None).await;
        drop(server);
        println!("server stopped");
        repl?;
    }
    step("Phase 4: OLAP lane comparison (DuckDB vs Turso)");
    olap::run().await?;
    step("Phase 5: edge vector search");
    vector::run(None, None).await?;
    step("Phase 6: report");
    report::run()?;
    println!("\nALL DONE. Results in results/*.json and docs/REPORT.md");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init { path, replica } => cmd_init(&path, replica)?,
        Cmd::Serve { config } => cmd_serve(&config).await?,
        Cmd::Search { query, k, config } => cmd_search(&query, k, &config).await?,
        Cmd::Drain { config } => cmd_drain(&config).await?,
        Cmd::Gateway { config, bind } => {
            let node = Node::open(Config::from_path(&config)?).await?;
            cascade::gateway::serve(node, &bind).await?;
        }

        Cmd::GenSynthetic { n } => {
            let n = n
                .or_else(|| std::env::var("SYNTH_N").ok().and_then(|s| s.parse().ok()))
                .unwrap_or(50_000);
            gen_synthetic::run(n)?;
        }
        Cmd::PrepData { path, limit } => {
            let src = path
                .or_else(common::patents_jsonl)
                .context("no source JSONL: pass a path or set PATENTS_JSONL")?;
            prep_data::run(&src, limit)?;
        }
        Cmd::CdcOverhead { limit } => cdc_overhead::run(limit).await?,
        Cmd::CdcToOlap { primary } => cdc_to_olap::run(primary).await?,
        Cmd::Replication { n, delta } => replication::run(n, delta).await?,
        Cmd::Olap => olap::run().await?,
        Cmd::Vector { max_rows, dim } => vector::run(max_rows, dim).await?,
        Cmd::Report => report::run()?,
        Cmd::RunAll { n } => run_all(n).await?,
    }
    Ok(())
}
