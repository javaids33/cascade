//! B1: pluggable CDC sinks. The drain decodes `turso_cdc` into a stream of [`ChangeRow`]s and
//! feeds each configured [`Sink`] — so one change stream can fan out to DuckDB (analytics), a
//! webhook ("change feed as a service", no broker), and/or a JSONL file at once.
//!
//! Sinks are held as concrete types and called statically (no `dyn`), so the async trait needs no
//! `async-trait` dep and stays object-safety-free. Add a new sink = a new struct + `impl Sink`.

use anyhow::Result;
use serde_json::{json, Value as J};

/// CDC operation, decoded from `turso_cdc.change_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Insert => "insert",
            Op::Update => "update",
            Op::Delete => "delete",
        }
    }
    /// Map a turso_cdc change_type code (1=INSERT, 0=UPDATE, -1=DELETE) to an [`Op`].
    pub fn from_cdc(code: i64) -> Option<Op> {
        match code {
            1 => Some(Op::Insert),
            0 => Some(Op::Update),
            -1 => Some(Op::Delete),
            _ => None,
        }
    }
}

/// One decoded change: the operation, source table, primary key, and before/after row images.
#[derive(Debug, Clone)]
pub struct ChangeRow {
    pub op: Op,
    pub table: String,
    pub id: String,
    pub after: Option<J>,
    pub before: Option<J>,
}

impl ChangeRow {
    fn as_event(&self) -> J {
        json!({
            "op": self.op.as_str(),
            "table": self.table,
            "id": self.id,
            "after": self.after,
            "before": self.before,
        })
    }
}

/// A destination for decoded CDC changes. Called per change, then `flush`ed once at the end.
#[allow(async_fn_in_trait)]
pub trait Sink {
    async fn apply(&mut self, change: &ChangeRow) -> Result<()>;
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DuckDB sink for the `docs` table (full rebuild from the change log).
// ---------------------------------------------------------------------------

pub struct DuckDbDocsSink {
    conn: duckdb::Connection,
    path: String,
}

impl DuckDbDocsSink {
    pub fn open(path: &str) -> Result<Self> {
        if let Some(p) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(p);
        }
        let conn = duckdb::Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS docs(id VARCHAR PRIMARY KEY, text VARCHAR, meta VARCHAR, ts BIGINT); \
             DELETE FROM docs;",
        )?;
        Ok(Self { conn, path: path.to_string() })
    }
    pub fn rows(&self) -> Result<i64> {
        Ok(self.conn.query_row("SELECT COUNT(*) FROM docs", [], |r| r.get(0))?)
    }
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl Sink for DuckDbDocsSink {
    async fn apply(&mut self, c: &ChangeRow) -> Result<()> {
        match c.op {
            Op::Insert | Op::Update => {
                if let Some(a) = &c.after {
                    self.conn.execute(
                        "INSERT OR REPLACE INTO docs VALUES (?,?,?,?)",
                        duckdb::params![
                            a.get("id").and_then(|x| x.as_str()).unwrap_or(""),
                            a.get("text").and_then(|x| x.as_str()).unwrap_or(""),
                            a.get("meta").and_then(|x| x.as_str()).unwrap_or(""),
                            a.get("ts").and_then(|x| x.as_i64()).unwrap_or(0)
                        ],
                    )?;
                }
            }
            Op::Delete => {
                self.conn.execute("DELETE FROM docs WHERE id=?", [c.id.as_str()])?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Webhook sink — POST each change as JSON. The "change feed as a service" with no broker.
// ---------------------------------------------------------------------------

pub struct WebhookSink {
    client: reqwest::Client,
    url: String,
    pub sent: u64,
}

impl WebhookSink {
    pub fn new(client: reqwest::Client, url: &str) -> Self {
        Self { client, url: url.to_string(), sent: 0 }
    }
}

impl Sink for WebhookSink {
    async fn apply(&mut self, c: &ChangeRow) -> Result<()> {
        self.client
            .post(&self.url)
            .json(&c.as_event())
            .send()
            .await?
            .error_for_status()?;
        self.sent += 1;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JSONL sink — append each change as one JSON line.
// ---------------------------------------------------------------------------

pub struct JsonlSink {
    w: std::io::BufWriter<std::fs::File>,
}

impl JsonlSink {
    pub fn create(path: &str) -> Result<Self> {
        if let Some(p) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(p);
        }
        Ok(Self { w: std::io::BufWriter::new(std::fs::File::create(path)?) })
    }
}

impl Sink for JsonlSink {
    async fn apply(&mut self, c: &ChangeRow) -> Result<()> {
        use std::io::Write;
        writeln!(self.w, "{}", c.as_event())?;
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        use std::io::Write;
        self.w.flush()?;
        Ok(())
    }
}
