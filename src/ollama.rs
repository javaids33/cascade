//! Minimal Ollama HTTP client: embeddings (ingest + query) and text generation (RAG answers).
//! Endpoints/models come from [`crate::config`] so a node is fully config-driven.

use anyhow::{Context, Result};
use serde_json::json;

/// Embed one text into a vector via Ollama `/api/embed`.
pub async fn embed(client: &reqwest::Client, url: &str, model: &str, text: &str) -> Result<Vec<f32>> {
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
