//! Minimal library usage — no sync server, no config file: open a LOCAL node in code, store a
//! few docs (embedded via Ollama), and run co-located vector search.
//!
//!   ollama pull all-minilm
//!   cargo run --example library_local
//!
//! This is the smallest "drop the crate in and use it" demo. For master/replica sync, build a
//! Config with `sync.enabled = true` (see configs/ and LAB.md).

use serde_json::json;
use cascade::config::{
    CdcCfg, Config, EmbedCfg, GenCfg, NodeCfg, OlapCfg, Role, SourceCfg, SyncCfg,
};
use cascade::Node;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // A standalone local node (sync disabled) — everything in one process/file.
    let cfg = Config {
        node: NodeCfg { role: Role::Master, db: ".work/db/library_demo.db".into() },
        sync: SyncCfg { enabled: false, ..Default::default() },
        cdc: CdcCfg { enabled: true },
        embedding: EmbedCfg::default(), // all-minilm / 384 / localhost:11434
        olap: OlapCfg::default(),
        generation: GenCfg { model: String::new() },
        source: SourceCfg::default(),
    };
    let _ = std::fs::remove_file(&cfg.node.db);
    let node = Node::open(cfg).await?;

    for (id, text) in [
        ("a", "Turso has built-in CDC and native replication."),
        ("b", "Vectors are co-located with the rows via F32_BLOB and vector_distance_cos."),
        ("c", "DuckDB and Iceberg form the analytics lane fed by CDC."),
    ] {
        node.put(id, text, &json!({"source": "library_demo"})).await?;
    }

    let hits = node.search("how does turso store embeddings?", 3).await?;
    println!("top {} hits:", hits.len());
    for h in &hits {
        println!("  {:.3}  [{}] {}", h.score, h.id, h.text);
    }
    Ok(())
}
