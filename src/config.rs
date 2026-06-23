//! Declarative node configuration (TOML). One file describes a master or a replica and all of
//! its behavior, so users `cascade init` a config, edit it, and `cascade serve` / `cascade search` it.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Writes/embeds, captures CDC, drains to OLAP, push()es to replicas.
    Master,
    /// Pull()s the data + vectors and serves co-located reads/search.
    Replica,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node: NodeCfg,
    #[serde(default)]
    pub sync: SyncCfg,
    #[serde(default)]
    pub cdc: CdcCfg,
    #[serde(default)]
    pub embedding: EmbedCfg,
    #[serde(default)]
    pub olap: OlapCfg,
    #[serde(default)]
    pub generation: GenCfg,
    #[serde(default)]
    pub source: SourceCfg,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCfg {
    pub role: Role,
    /// Local db file (each replica keeps its own).
    pub db: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCfg {
    /// Replicate via a Turso sync server. If false, the node is a standalone local db.
    #[serde(default)]
    pub enabled: bool,
    /// Sync server URL (master push()es here; replicas pull()).
    #[serde(default = "default_remote")]
    pub remote_url: String,
    /// Master only: spawn `tursodb --sync-server` as part of `cascade serve`.
    #[serde(default)]
    pub serve: bool,
    /// Bind address for the spawned sync server.
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Commit + push every N writes.
    #[serde(default = "default_push_every")]
    pub push_every: usize,
    /// Replica write-back: the master's HTTP gateway URL (e.g. http://MASTER_IP:7070). `cascade
    /// flush` ships the local outbox to `<writeback_url>/inbox`. Native sync is one-way, so edge
    /// writes travel out-of-band over this gateway rather than through the sync engine.
    #[serde(default)]
    pub writeback_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcCfg {
    /// Master: capture changes for the OLAP lane.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Capture mode: off | id | before | after | full. `full` keeps before+after images (~2x
    /// storage); `id`/`after` are lighter. `turso_cdc` grows unbounded, so size this with `retain`.
    #[serde(default = "default_cdc_mode")]
    pub mode: String,
    /// Keep `turso_cdc` rows after a drain. `false` = prune up to the drained cursor each drain,
    /// so the change log doesn't grow forever on a long-running master.
    #[serde(default = "default_true")]
    pub retain: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedCfg {
    #[serde(default = "default_ollama_url")]
    pub url: String,
    #[serde(default = "default_embed_model")]
    pub model: String,
    #[serde(default = "default_dim")]
    pub dim: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OlapCfg {
    /// DuckDB file for the CDC drain (None disables the OLAP lane).
    pub duckdb: Option<String>,
    /// Iceberg warehouse dir (None skips Iceberg).
    pub iceberg: Option<String>,
    /// POST each decoded change (JSON) to this URL — a broker-free "change feed as a service".
    pub webhook: Option<String>,
    /// Append each decoded change as a JSON line to this file.
    pub jsonl: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenCfg {
    /// LLM for `cascade search` answers (Ollama model). Empty = retrieval only, no generation.
    #[serde(default = "default_gen_model")]
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCfg {
    /// Built-in ingestion source for `cascade serve`: none | wikimedia | hn.
    #[serde(default = "default_source_kind")]
    pub kind: String,
    /// Wikimedia: wiki filter (e.g. enwiki; "" = all).
    #[serde(default = "default_wiki")]
    pub wiki: String,
    /// Wikimedia: namespace filter (e.g. "0" = articles; "" = all).
    #[serde(default = "default_ns")]
    pub namespace: String,
    /// Stop after N items (0 = run forever).
    #[serde(default)]
    pub max_items: usize,
}

fn default_true() -> bool {
    true
}
fn default_remote() -> String {
    "http://127.0.0.1:8080".into()
}
fn default_bind() -> String {
    "0.0.0.0:8080".into()
}
fn default_push_every() -> usize {
    32
}
fn default_ollama_url() -> String {
    "http://localhost:11434".into()
}
fn default_embed_model() -> String {
    "all-minilm".into()
}
fn default_dim() -> usize {
    384
}
fn default_gen_model() -> String {
    "qwen2.5:1.5b".into()
}
fn default_cdc_mode() -> String {
    "full".into()
}
fn default_source_kind() -> String {
    "none".into()
}
fn default_wiki() -> String {
    "enwiki".into()
}
fn default_ns() -> String {
    "0".into()
}

impl Default for SyncCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            remote_url: default_remote(),
            serve: false,
            bind: default_bind(),
            push_every: default_push_every(),
            writeback_url: None,
        }
    }
}
impl Default for CdcCfg {
    fn default() -> Self {
        Self { enabled: true, mode: default_cdc_mode(), retain: true }
    }
}
impl Default for EmbedCfg {
    fn default() -> Self {
        Self {
            url: default_ollama_url(),
            model: default_embed_model(),
            dim: default_dim(),
        }
    }
}
impl Default for GenCfg {
    fn default() -> Self {
        Self { model: default_gen_model() }
    }
}
impl Default for SourceCfg {
    fn default() -> Self {
        Self {
            kind: default_source_kind(),
            wiki: default_wiki(),
            namespace: default_ns(),
            max_items: 0,
        }
    }
}

impl Config {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let p = path.as_ref();
        let s = std::fs::read_to_string(p).with_context(|| format!("read config {}", p.display()))?;
        let mut cfg: Config = toml::from_str(&s).with_context(|| format!("parse config {}", p.display()))?;
        cfg.apply_env_overrides();
        Ok(cfg)
    }

    /// Environment overrides applied after loading a config file. These let a launcher inject
    /// host-specific values without rewriting the committed `configs/*.toml`. The motivating
    /// case: on a Windows PC the master runs inside WSL2 but Ollama runs on the Windows host, so
    /// `start-master.sh` sets `CASCADE_EMBED_URL=http://<windows-host-ip>:11434` (the config's
    /// `localhost` would point at WSL, where there is no Ollama). `embedding.url` also drives the
    /// generation endpoint, so this one override covers both embedding and RAG answers.
    ///
    /// - `CASCADE_EMBED_URL` → `embedding.url` (also the RAG/generation endpoint).
    /// - `CASCADE_REMOTE_URL` → `sync.remote_url` (the sync hub to push/pull). Used by the docker
    ///   edge, where the master's Tailscale/LAN ip is only known at run time (passed as `PC_IP`).
    fn apply_env_overrides(&mut self) {
        if let Ok(u) = std::env::var("CASCADE_EMBED_URL") {
            if !u.is_empty() {
                self.embedding.url = u;
            }
        }
        if let Ok(u) = std::env::var("CASCADE_REMOTE_URL") {
            if !u.is_empty() {
                self.sync.remote_url = u;
            }
        }
    }

    pub fn is_master(&self) -> bool {
        self.node.role == Role::Master
    }

    /// A commented starter config for `cascade init`.
    pub fn example_master() -> &'static str {
        EXAMPLE_MASTER
    }
    pub fn example_replica() -> &'static str {
        EXAMPLE_REPLICA
    }
}

pub const EXAMPLE_MASTER: &str = r#"# cascade master node.
# Embeds + stores incoming data, captures CDC for OLAP, and pushes vectors to replicas.

[node]
role = "master"
db   = ".work/db/master.db"

[sync]
enabled    = true
serve      = true                 # spawn the tursodb --sync-server hub as part of `cascade serve`
bind       = "0.0.0.0:8080"       # replicas connect to http://<this-ip>:8080
remote_url = "http://127.0.0.1:8080"
push_every = 32

[cdc]
enabled = true
mode    = "full"                  # off | id | before | after | full  (full = ~2x storage)
retain  = true                    # false = prune turso_cdc up to the drained cursor each drain

[embedding]                       # runs on this node's GPU (Ollama)
url   = "http://localhost:11434"
model = "all-minilm"
dim   = 384

[olap]                            # CDC drain target(s) — set any combination
duckdb  = ".work/olap/master.duckdb"
# iceberg = ".work/olap/iceberg"  # uncomment for a real Iceberg table too
# webhook = "http://localhost:9000/cdc"  # also POST each change (broker-free change feed)
# jsonl   = ".work/olap/changes.jsonl"   # also append each change as a JSON line

[source]                          # built-in ingestion for `cascade serve`: none | wikimedia | hn
kind      = "wikimedia"
wiki      = "enwiki"
namespace = "0"
max_items = 0                     # 0 = run forever
"#;

pub const EXAMPLE_REPLICA: &str = r#"# cascade replica node.
# Pulls finished data + vectors from the master and serves co-located search. No GPU needed.

[node]
role = "replica"
db   = ".work/db/replica.db"

[sync]
enabled    = true
remote_url = "http://MASTER_IP:8080"   # <- the master's Tailscale/LAN ip
# writeback_url = "http://MASTER_IP:7070"  # master gateway for `cascade flush` (edge write-back)

[embedding]                            # used to embed the *query* only (small, CPU-fine)
url   = "http://localhost:11434"
model = "all-minilm"
dim   = 384

[generation]                           # LLM for `cascade search` answers ("" = retrieval only)
model = "qwen2.5:1.5b"
"#;
