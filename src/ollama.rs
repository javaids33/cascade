//! Minimal Ollama HTTP client: embeddings (ingest + query) and text generation (RAG answers).
//! Endpoints/models come from [`crate::config`] so a node is fully config-driven.
//!
//! CI/offline escape hatch: set `CASCADE_FAKE_EMBED=1` to use a deterministic, dependency-free
//! hashing embedder instead of calling Ollama. This lets `cargo test` / a smoke run exercise the
//! full ingest → CDC → search path on a machine with no GPU and no model pulled. Dimension comes
//! from `CASCADE_EMBED_DIM` (default 384). It is NOT semantically meaningful — tests only.

use std::hash::{Hash, Hasher};

use anyhow::{Context, Result};
use serde_json::json;

fn fake_enabled() -> bool {
    std::env::var("CASCADE_FAKE_EMBED").is_ok_and(|v| !v.is_empty() && v != "0")
}

/// Deterministic hashing vectorizer (same shape as the `vector` benchmark): bucket lowercase
/// `[a-z]+` tokens into `dim` dims and L2-normalize. Stable across runs, no network.
fn fake_embed(text: &str) -> Vec<f32> {
    let dim: usize = std::env::var("CASCADE_EMBED_DIM")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|d| *d > 0)
        .unwrap_or(384);
    let mut v = vec![0f32; dim];
    let mut cur = String::new();
    let mut flush = |cur: &mut String, v: &mut Vec<f32>| {
        if !cur.is_empty() {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            cur.hash(&mut h);
            v[(h.finish() % dim as u64) as usize] += 1.0;
            cur.clear();
        }
    };
    for ch in text.chars() {
        if ch.is_ascii_alphabetic() {
            cur.push(ch.to_ascii_lowercase());
        } else {
            flush(&mut cur, &mut v);
        }
    }
    flush(&mut cur, &mut v);
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
    v
}

/// Embed one text into a vector via Ollama `/api/embed`.
pub async fn embed(client: &reqwest::Client, url: &str, model: &str, text: &str) -> Result<Vec<f32>> {
    if fake_enabled() {
        return Ok(fake_embed(text));
    }
    let resp = client
        .post(format!("{url}/api/embed"))
        .json(&json!({ "model": model, "input": text }))
        .send()
        .await
        .context("ollama /api/embed request")?
        .error_for_status()
        .context("ollama /api/embed status")?;
    let v: serde_json::Value = resp.json().await?;
    let arr = v
        .get("embeddings")
        .and_then(|e| e.as_array())
        .and_then(|a| a.first())
        .and_then(|f| f.as_array())
        .context("no embeddings in ollama response")?;
    Ok(arr.iter().filter_map(|x| x.as_f64().map(|f| f as f32)).collect())
}

/// Non-streaming text generation via Ollama `/api/generate`.
pub async fn generate(client: &reqwest::Client, url: &str, model: &str, prompt: &str) -> Result<String> {
    if fake_enabled() {
        return Ok("[fake-llm] generation disabled in CASCADE_FAKE_EMBED mode".to_string());
    }
    let resp = client
        .post(format!("{url}/api/generate"))
        .json(&json!({ "model": model, "prompt": prompt, "stream": false }))
        .send()
        .await
        .context("ollama /api/generate request")?
        .error_for_status()
        .context("ollama /api/generate status")?;
    let v: serde_json::Value = resp.json().await?;
    Ok(v.get("response").and_then(|r| r.as_str()).unwrap_or("").trim().to_string())
}

/// Format a float vector as the JSON string Turso's `vector32()` expects.
pub fn vec_to_json(v: &[f32]) -> String {
    let mut s = String::from("[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{:.6}", x));
    }
    s.push(']');
    s
}
