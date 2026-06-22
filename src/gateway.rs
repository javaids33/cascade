//! Local HTTP gateway over a [`Node`], so clients in any language (Python/JS/Go) can drive a node
//! without a Turso binding. Run it co-located with the node (`cascade gateway --config X`); an edge's
//! app then hits its LOCAL gateway and still gets local, co-located vector search.
//!
//! Routes:
//!   GET  /health                      -> {role, ok}
//!   POST /put     {id,text,meta?}     -> {ok}                 (master)
//!   GET  /search?q=...&k=5            -> {hits:[{id,text,meta,score}]}
//!   POST /drain                       -> OlapStats            (master)

use std::sync::Arc;

use anyhow::Result;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value as J};
use tokio::sync::Mutex;

use crate::node::Node;

type Shared = Arc<Mutex<Node>>;
type ApiErr = (StatusCode, String);

fn err(e: anyhow::Error) -> ApiErr {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

/// Serve the node over HTTP at `bind` (e.g. 127.0.0.1:7070) until the process stops.
pub async fn serve(node: Node, bind: &str) -> Result<()> {
    let role = if node.is_master() { "master" } else { "replica" };
    let state: Shared = Arc::new(Mutex::new(node));
    let app = Router::new()
        .route("/health", get(health))
        .route("/put", post(put))
        .route("/search", get(search))
        .route("/drain", post(drain))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    println!("cascade gateway ({role}) on http://{bind}");
    println!("  GET /health · POST /put · GET /search?q=&k= · POST /drain");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(s): State<Shared>) -> Json<J> {
    let node = s.lock().await;
    Json(json!({ "ok": true, "role": if node.is_master() { "master" } else { "replica" } }))
}

#[derive(Deserialize)]
struct PutReq {
    id: String,
    text: String,
    #[serde(default)]
    meta: Option<J>,
}

async fn put(State(s): State<Shared>, Json(req): Json<PutReq>) -> Result<Json<J>, ApiErr> {
    let node = s.lock().await;
    let meta = req.meta.unwrap_or(J::Null);
    node.put(&req.id, &req.text, &meta).await.map_err(err)?;
    node.push().await.map_err(err)?;
    Ok(Json(json!({ "ok": true, "id": req.id })))
}

#[derive(Deserialize)]
struct SearchReq {
    q: String,
    k: Option<usize>,
}

async fn search(State(s): State<Shared>, Query(req): Query<SearchReq>) -> Result<Json<J>, ApiErr> {
    let node = s.lock().await;
    let _ = node.pull().await; // freshness on replicas; no-op on a local node
    let hits = node.search(&req.q, req.k.unwrap_or(5)).await.map_err(err)?;
    Ok(Json(json!({ "hits": hits })))
}

async fn drain(State(s): State<Shared>) -> Result<Json<J>, ApiErr> {
    let node = s.lock().await;
    let stats = node.drain_olap().await.map_err(err)?;
    Ok(Json(json!(stats)))
}
