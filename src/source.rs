//! Built-in ingestion sources for `cascade serve`. A source streams items and feeds them into the
//! master [`Node`] (which embeds + stores + captures CDC). Selected via `[source] kind` in config.
//!
//! - `wikimedia`: the Wikimedia EventStreams `recentchange` firehose (open, ~33/s).
//! - `hn`: Hacker News newest stories (low frequency; a fallback / demo).
//! - `none`: no built-in source — feed the node yourself via [`Node::put`].

use anyhow::Result;
use futures_util::StreamExt;
use serde_json::json;

use crate::node::Node;

const WIKI_STREAM: &str = "https://stream.wikimedia.org/v2/stream/recentchange";
const HN_NEW: &str = "https://hacker-news.firebaseio.com/v0/newstories.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    None,
    Wikimedia,
    Hn,
    /// Deterministic, offline fixed docs — used by the local smoke test.
    Demo,
}

impl Source {
    pub fn parse(kind: &str) -> Source {
        match kind {
            "wikimedia" => Source::Wikimedia,
            "hn" => Source::Hn,
            "demo" => Source::Demo,
            _ => Source::None,
        }
    }
}

/// Fixed corpus for the `demo` source (no network) — enough to make vector search meaningful.
const DEMO_DOCS: &[(&str, &str)] = &[
    ("d1", "Turso is a Rust rewrite of SQLite with built-in change data capture, native replication, and co-located vector search."),
    ("d2", "Change data capture in Turso turns every insert, update and delete into queryable rows in the turso_cdc table, with no Kafka or Debezium."),
    ("d3", "Native sync replicates a primary database to many read replicas; replica bootstrap is sub-second and incremental catch-up is a few kilobytes."),
    ("d4", "Vectors live in the same row store as the data using F32_BLOB columns and vector_distance_cos, so retrieval is a local query with no separate vector database."),
    ("d5", "In the AI distribution pattern a GPU producer embeds documents once and pushes the finished vectors; CPU-only edges pull them and search locally."),
    ("d6", "DuckDB and Apache Iceberg form the analytics lane: the CDC stream drains into them so analytics never touches the live edge database."),
];

/// Run the node's configured source loop until `max_items` (0 = forever).
pub async fn run(node: &Node) -> Result<()> {
    let cfg = node.config().clone();
    let source = Source::parse(&cfg.source.kind);
    let push_every = cfg.sync.push_every.max(1);
    let max = cfg.source.max_items;
    let client = reqwest::Client::builder()
        .user_agent("cascade/0.1 (https://github.com/javaids33/turso-edge-olap)")
        .build()?;

    let mut n = 0usize;
    let mut since = 0usize;
    let t0 = std::time::Instant::now();

    match source {
        Source::None => {
            println!("source=none — feed the node via the library (Node::put) or set [source].kind");
        }
        Source::Demo => {
            println!("source=demo — {} fixed docs (offline, deterministic)", DEMO_DOCS.len());
            for (id, text) in DEMO_DOCS {
                node.put(id, text, &json!({"source": "demo"})).await?;
                n += 1;
            }
            node.push().await?;
        }
        Source::Wikimedia => {
            let wiki = cfg.source.wiki.clone();
            let ns = cfg.source.namespace.clone();
            println!("source=wikimedia wiki={wiki:?} ns={ns:?} max={max}");
            // The EventStreams firehose drops the connection periodically (and a transient embed
            // error shouldn't be fatal either). Reconnect/skip instead of dying so the master
            // stays up indefinitely.
            'reconnect: loop {
                let resp = match client.get(WIKI_STREAM).send().await {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("  wikimedia connect failed: {e}; retrying in 3s");
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        continue;
                    }
                };
                let mut stream = resp.bytes_stream();
                let mut buf = String::new();
                while let Some(chunk) = stream.next().await {
                    let chunk = match chunk {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("  wikimedia stream dropped: {e}; reconnecting in 3s");
                            break;
                        }
                    };
                    buf.push_str(&String::from_utf8_lossy(&chunk));
                    while let Some(pos) = buf.find('\n') {
                        let line: String = buf.drain(..=pos).collect();
                        let data = match line.trim_end().strip_prefix("data: ") {
                            Some(d) => d.to_string(),
                            None => continue,
                        };
                        let ev: serde_json::Value = match serde_json::from_str(&data) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let typ = ev.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        if typ != "edit" && typ != "new" {
                            continue;
                        }
                        if !wiki.is_empty() && ev.get("wiki").and_then(|w| w.as_str()) != Some(wiki.as_str()) {
                            continue;
                        }
                        if !ns.is_empty()
                            && ev.get("namespace").and_then(|x| x.as_i64()).map(|x| x.to_string()).as_deref()
                                != Some(ns.as_str())
                        {
                            continue;
                        }
                        let title = ev.get("title").and_then(|t| t.as_str()).unwrap_or("").to_string();
                        if title.is_empty() {
                            continue;
                        }
                        let comment = ev.get("comment").and_then(|c| c.as_str()).unwrap_or("").to_string();
                        let url = ev.get("meta").and_then(|m| m.get("uri")).and_then(|u| u.as_str()).unwrap_or("").to_string();
                        let id = ev
                            .get("meta")
                            .and_then(|m| m.get("id"))
                            .and_then(|i| i.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| format!("{wiki}:{title}"));
                        let text = format!("{title}: {comment}");
                        let meta = json!({"title": title, "url": url, "source": "wikimedia"});

                        if let Err(e) = node.put(&id, &text, &meta).await {
                            eprintln!("  put failed (skipping {id}): {e}");
                            continue;
                        }
                        n += 1;
                        since += 1;
                        if since >= push_every {
                            if let Err(e) = node.push().await {
                                eprintln!("  push failed: {e}");
                            }
                            since = 0;
                            let rate = n as f64 / t0.elapsed().as_secs_f64();
                            println!("  stored={n}  rate={rate:.1}/s  (pushed)");
                        }
                        if max != 0 && n >= max {
                            break 'reconnect;
                        }
                    }
                }
                // stream ended/dropped: push what we have, pause, reconnect.
                let _ = node.push().await;
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
            let _ = node.push().await;
        }
        Source::Hn => {
            println!("source=hn (low frequency) max={max}");
            let ids: Vec<i64> = client.get(HN_NEW).send().await?.json().await?;
            for id in ids {
                if max != 0 && n >= max {
                    break;
                }
                let item: serde_json::Value = client
                    .get(format!("https://hacker-news.firebaseio.com/v0/item/{id}.json"))
                    .send()
                    .await?
                    .json()
                    .await?;
                let title = item.get("title").and_then(|t| t.as_str()).unwrap_or("").to_string();
                if title.is_empty() {
                    continue;
                }
                let body = item.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string();
                let url = item
                    .get("url")
                    .and_then(|u| u.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("https://news.ycombinator.com/item?id={id}"));
                let text = format!("{title}. {body}");
                let meta = json!({"title": title, "url": url, "source": "hn"});
                node.put(&format!("hn:{id}"), &text, &meta).await?;
                n += 1;
                since += 1;
                if since >= push_every {
                    node.push().await?;
                    since = 0;
                }
            }
            node.push().await?;
        }
    }
    println!("source done: {n} items in {:.1}s", t0.elapsed().as_secs_f64());
    Ok(())
}
