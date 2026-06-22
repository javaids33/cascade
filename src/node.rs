//! The engine. A [`Node`] is a Turso db opened from a [`Config`] in one of two roles:
//!
//! - **master**: [`put`](Node::put) (embed + store + CDC), [`push`](Node::push) to replicas,
//!   [`drain_olap`](Node::drain_olap) (CDC -> DuckDB).
//! - **replica**: [`pull`](Node::pull) the finished data + vectors, [`search`](Node::search)
//!   them locally (co-located vector search, no GPU).
//!
//! Data model (generic on purpose): `docs(id, text, meta, ts)` holds the text + a JSON metadata
//! blob; `doc_vectors(id, emb F32_BLOB(dim))` holds the embedding. They're split so the
//! CDC -> OLAP JSON decode never touches a BLOB column.

use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value as J};
use turso::Value;

use crate::config::Config;
use crate::ollama::{embed, vec_to_json};

const INS_META: &str = "INSERT OR REPLACE INTO docs VALUES (?1,?2,?3,?4)";
const INS_VEC: &str = "INSERT OR REPLACE INTO doc_vectors VALUES (?1, vector32(?2))";

/// One search result.
#[derive(Debug, Clone, Serialize)]
pub struct Hit {
    pub id: String,
    pub text: String,
    pub meta: J,
    /// cosine distance (0 = identical); lower is closer.
    pub score: f64,
}

/// Result of a CDC -> OLAP drain.
#[derive(Debug, Clone, Serialize)]
pub struct OlapStats {
    pub changes: u64,
    pub duckdb_rows: i64,
    pub seconds: f64,
    pub duckdb_path: String,
}

enum DbHandle {
    Local {
        _db: turso::Database,
        conn: turso::Connection,
    },
    Synced {
        db: turso::sync::Database,
        conn: turso::Connection,
    },
}

pub struct Node {
    cfg: Config,
    handle: DbHandle,
    client: reqwest::Client,
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn ensure_parent(path: &str) {
    if let Some(p) = Path::new(path).parent() {
        let _ = std::fs::create_dir_all(p);
    }
}

impl Node {
    fn conn(&self) -> &turso::Connection {
        match &self.handle {
            DbHandle::Local { conn, .. } => conn,
            DbHandle::Synced { conn, .. } => conn,
        }
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }
    pub fn is_master(&self) -> bool {
        self.cfg.is_master()
    }

    /// Open (or create) the node per its config.
    pub async fn open(cfg: Config) -> Result<Node> {
        ensure_parent(&cfg.node.db);
        let client = reqwest::Client::builder()
            .user_agent("cascade/0.1")
            .build()?;

        let handle = if cfg.sync.enabled {
            let db = turso::sync::Builder::new_remote(&cfg.node.db)
                .with_remote_url(&cfg.sync.remote_url)
                .bootstrap_if_empty(true)
                .build()
                .await
                .context("open synced db")?;
            let conn = db.connect().await?;
            DbHandle::Synced { db, conn }
        } else {
            let db = turso::Builder::new_local(&cfg.node.db).build().await?;
            let conn = db.connect()?;
            DbHandle::Local { _db: db, conn }
        };

        let node = Node { cfg, handle, client };

        if node.cfg.is_master() {
            if node.cfg.cdc.enabled {
                node.conn()
                    .execute("PRAGMA capture_data_changes_conn('full')", ())
                    .await?;
            }
            node.conn()
                .execute(
                    "CREATE TABLE IF NOT EXISTS docs(id TEXT PRIMARY KEY, text TEXT, meta TEXT, ts INTEGER)",
                    (),
                )
                .await?;
            node.conn()
                .execute(
                    &format!(
                        "CREATE TABLE IF NOT EXISTS doc_vectors(id TEXT PRIMARY KEY, emb F32_BLOB({}))",
                        node.cfg.embedding.dim
                    ),
                    (),
                )
                .await?;
        } else if node.cfg.sync.enabled {
            // Replica: bootstrap schema + data from the master.
            let _ = node.pull().await;
        }
        Ok(node)
    }

    /// Master: embed `text`, upsert it + its metadata. (Call [`push`](Node::push) to replicate.)
    pub async fn put(&self, id: &str, text: &str, meta: &J) -> Result<()> {
        let v = embed(&self.client, &self.cfg.embedding.url, &self.cfg.embedding.model, text).await?;
        self.conn()
            .execute(
                INS_META,
                vec![
                    Value::Text(id.to_string()),
                    Value::Text(text.to_string()),
                    Value::Text(meta.to_string()),
                    Value::Integer(now_ts()),
                ],
            )
            .await?;
        self.conn()
            .execute(INS_VEC, vec![Value::Text(id.to_string()), Value::Text(vec_to_json(&v))])
            .await?;
        Ok(())
    }

    /// Master: push local writes to the sync server (no-op on a non-synced node).
    pub async fn push(&self) -> Result<()> {
        if let DbHandle::Synced { db, .. } = &self.handle {
            db.push().await?;
        }
        Ok(())
    }

    /// Replica: pull latest changes from the sync server. Returns whether anything changed.
    pub async fn pull(&self) -> Result<bool> {
        if let DbHandle::Synced { db, .. } = &self.handle {
            return Ok(db.pull().await?);
        }
        Ok(false)
    }

    /// Co-located vector search: embed `query`, return the top-`k` nearest docs locally.
    pub async fn search(&self, query: &str, k: usize) -> Result<Vec<Hit>> {
        let qv = embed(&self.client, &self.cfg.embedding.url, &self.cfg.embedding.model, query).await?;
        let mut rows = self
            .conn()
            .query(
                "SELECT d.id, d.text, d.meta, vector_distance_cos(v.emb, vector32(?1)) dist \
                 FROM doc_vectors v JOIN docs d ON d.id = v.id ORDER BY dist ASC LIMIT ?2",
                (vec_to_json(&qv), k as i64),
            )
            .await?;
        let mut hits = Vec::new();
        while let Some(r) = rows.next().await? {
            let text = match r.get_value(1)? {
                Value::Text(s) => s.to_string(),
                _ => String::new(),
            };
            let meta = match r.get_value(2)? {
                Value::Text(s) => serde_json::from_str(&s).unwrap_or(J::Null),
                _ => J::Null,
            };
            let id = match r.get_value(0)? {
                Value::Text(s) => s.to_string(),
                _ => String::new(),
            };
            let score = match r.get_value(3)? {
                Value::Real(f) => f,
                Value::Integer(i) => i as f64,
                _ => f64::NAN,
            };
            hits.push(Hit { id, text, meta, score });
        }
        Ok(hits)
    }

    /// Build a cited RAG prompt from hits and generate an answer via the configured LLM.
    pub async fn answer(&self, query: &str, hits: &[Hit]) -> Result<String> {
        if self.cfg.generation.model.is_empty() {
            return Ok(String::new());
        }
        let mut ctx = String::new();
        for (i, h) in hits.iter().enumerate() {
            ctx.push_str(&format!("[{}] {}\n", i + 1, h.text));
        }
        let prompt = format!(
            "Answer the question using ONLY the numbered sources, citing inline like [1]. \
             If they don't answer it, say so.\n\nSources:\n{ctx}\nQuestion: {query}\nAnswer:"
        );
        crate::ollama::generate(&self.client, &self.cfg.embedding.url, &self.cfg.generation.model, &prompt).await
    }

    /// Master: drain the `docs` CDC stream into DuckDB (full rebuild from the change log).
    pub async fn drain_olap(&self) -> Result<OlapStats> {
        let duck_path = self
            .cfg
            .olap
            .duckdb
            .clone()
            .context("olap.duckdb not set in config")?;
        ensure_parent(&duck_path);
        let duck = duckdb::Connection::open(&duck_path)?;
        duck.execute_batch(
            "CREATE TABLE IF NOT EXISTS docs(id VARCHAR PRIMARY KEY, text VARCHAR, meta VARCHAR, ts BIGINT); \
             DELETE FROM docs;",
        )?;
        let ins = "INSERT OR REPLACE INTO docs VALUES (?,?,?,?)";
        let del = "DELETE FROM docs WHERE id=?";

        let decode = "SELECT change_id, change_type, \
             bin_record_json_object(table_columns_json_array(table_name), after)  AS a, \
             bin_record_json_object(table_columns_json_array(table_name), before) AS b \
             FROM turso_cdc WHERE change_id > ?1 AND table_name = 'docs' \
             ORDER BY change_id ASC LIMIT 10000";

        let mut cursor: i64 = 0;
        let mut changes: u64 = 0;
        let t0 = Instant::now();
        loop {
            let mut rows = self.conn().query(decode, (cursor,)).await?;
            let mut got = 0usize;
            while let Some(row) = rows.next().await? {
                got += 1;
                if let Value::Integer(i) = row.get_value(0)? {
                    cursor = i;
                }
                let ctype = match row.get_value(1)? {
                    Value::Integer(i) => i,
                    _ => continue,
                };
                let after = match row.get_value(2)? {
                    Value::Text(s) => Some(s.to_string()),
                    _ => None,
                };
                let before = match row.get_value(3)? {
                    Value::Text(s) => Some(s.to_string()),
                    _ => None,
                };
                match ctype {
                    1 | 0 => {
                        if let Some(a) = after {
                            let o: J = serde_json::from_str(&a)?;
                            duck.execute(
                                ins,
                                duckdb::params![
                                    o.get("id").and_then(|x| x.as_str()).unwrap_or(""),
                                    o.get("text").and_then(|x| x.as_str()).unwrap_or(""),
                                    o.get("meta").and_then(|x| x.as_str()).unwrap_or(""),
                                    o.get("ts").and_then(|x| x.as_i64()).unwrap_or(0)
                                ],
                            )?;
                            changes += 1;
                        }
                    }
                    -1 => {
                        if let Some(b) = before {
                            let o: J = serde_json::from_str(&b)?;
                            duck.execute(del, [o.get("id").and_then(|x| x.as_str()).unwrap_or("")])?;
                            changes += 1;
                        }
                    }
                    _ => {}
                }
            }
            if got == 0 {
                break;
            }
        }
        let rows: i64 = duck.query_row("SELECT COUNT(*) FROM docs", [], |r| r.get(0))?;
        Ok(OlapStats {
            changes,
            duckdb_rows: rows,
            seconds: t0.elapsed().as_secs_f64(),
            duckdb_path: duck_path,
        })
    }

    /// Convenience: a small trends summary over the drained DuckDB (counts + by-source if meta
    /// carries a `source` field).
    pub fn olap_summary(duck_path: &str) -> Result<J> {
        let duck = duckdb::Connection::open(duck_path)?;
        let total: i64 = duck.query_row("SELECT COUNT(*) FROM docs", [], |r| r.get(0))?;
        let mut by_source = Vec::new();
        if let Ok(mut stmt) = duck.prepare(
            "SELECT COALESCE(json_extract_string(meta,'$.source'),'?') s, COUNT(*) c \
             FROM docs GROUP BY s ORDER BY c DESC",
        ) {
            if let Ok(mut rows) = stmt.query([]) {
                while let Ok(Some(r)) = rows.next() {
                    let s: String = r.get(0).unwrap_or_default();
                    let c: i64 = r.get(1).unwrap_or(0);
                    by_source.push(json!({"source": s, "count": c}));
                }
            }
        }
        Ok(json!({ "total": total, "by_source": by_source }))
    }
}
