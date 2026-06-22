//! # cascade
//!
//! A small toolkit with **Turso at its core** for standing up master/replica edge nodes:
//!
//! - **Master** ingests data (optionally embedding it), captures **CDC**, drains it to an
//!   **OLAP** sink (DuckDB/Iceberg), and **push()es** to a sync server.
//! - **Replicas** **pull()** the finished data + vectors and run **co-located vector search**
//!   locally — no GPU, no separate vector DB.
//!
//! Drive it from a [`Config`] (TOML) via the `cascade` CLI, or from Rust:
//!
//! ```no_run
//! use cascade::{Config, Node};
//! use serde_json::json;
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let cfg = Config::from_path("node.toml")?;     // role = "master" | "replica"
//! let node = Node::open(cfg).await?;
//!
//! // master: embed + store + capture CDC, then push to replicas
//! node.put("doc-1", "Turso replaces a heavy CDC + vector stack", &json!({"src": "demo"})).await?;
//! node.push().await?;
//! node.drain_olap().await?;                       // CDC -> DuckDB (+ Iceberg)
//!
//! // replica: pull latest, search locally
//! node.pull().await?;
//! for hit in node.search("what does turso replace?", 5).await? {
//!     println!("{:.3}  {}", hit.score, hit.text);
//! }
//! # Ok(()) }
//! ```

pub mod config;
pub mod gateway;
pub mod node;
pub mod ollama;
pub mod source;
pub mod sync_server;

pub use config::{Config, Role};
pub use node::{Hit, Node, OlapStats};
pub use source::Source;
pub use sync_server::SyncServer;
