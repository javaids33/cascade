//! B3: vector index abstraction. `BruteForce` mirrors today's `vector_distance_cos` linear scan
//! (the honest baseline / ground truth); `HnswIndex` is an in-memory approximate-nearest-neighbor
//! index via the pure-Rust `hnsw_rs` crate, which breaks the linear ceiling so an edge can hold
//! millions of vectors and still answer in ~ms. Both implement [`VectorIndex`].
//!
//! NOTE (verify-on-build): the three `hnsw_rs` call sites in `HnswIndex` (`Hnsw::new`, `insert`,
//! `search` + the `Neighbour` fields `d_id`/`distance`) track the crate's 0.3 API — if you pin a
//! different major, those are the only lines to adjust.

use hnsw_rs::prelude::*;

/// A nearest-neighbor index over a fixed set of vectors. `search` returns `(item_index, distance)`
/// pairs (ascending distance), where `item_index` is the position the vector was added in.
pub trait VectorIndex {
    fn search(&self, q: &[f32], k: usize) -> Vec<(usize, f32)>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Cosine distance (0 = identical), matching Turso's `vector_distance_cos`.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let (mut dot, mut na, mut nb) = (0f32, 0f32, 0f32);
    let n = a.len().min(b.len());
    for i in 0..n {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    1.0 - dot / (na.sqrt() * nb.sqrt())
}

// ---------------------------------------------------------------------------
// Exact linear scan — ground truth for recall, and what an edge does today.
// ---------------------------------------------------------------------------

pub struct BruteForce {
    vecs: Vec<Vec<f32>>,
}

impl BruteForce {
    pub fn new(vecs: Vec<Vec<f32>>) -> Self {
        Self { vecs }
    }
}

impl VectorIndex for BruteForce {
    fn search(&self, q: &[f32], k: usize) -> Vec<(usize, f32)> {
        let mut scored: Vec<(usize, f32)> = self
            .vecs
            .iter()
            .enumerate()
            .map(|(i, v)| (i, cosine_distance(q, v)))
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }
    fn len(&self) -> usize {
        self.vecs.len()
    }
}

// ---------------------------------------------------------------------------
// HNSW approximate index (hnsw_rs) — the ceiling-breaker.
// ---------------------------------------------------------------------------

pub struct HnswIndex {
    hnsw: Hnsw<'static, f32, DistCosine>,
    len: usize,
    ef_search: usize,
}

impl HnswIndex {
    /// Build an HNSW index over `vecs`. `m` = max connections per node (graph degree),
    /// `ef_construction` = build-time candidate list size (quality vs build cost).
    pub fn build(vecs: &[Vec<f32>], m: usize, ef_construction: usize, ef_search: usize) -> Self {
        let n = vecs.len().max(1);
        let max_layer = 16;
        let hnsw = Hnsw::<f32, DistCosine>::new(m, n, max_layer, ef_construction, DistCosine {});
        for (i, v) in vecs.iter().enumerate() {
            hnsw.insert((v, i));
        }
        Self { hnsw, len: vecs.len(), ef_search }
    }
}

impl VectorIndex for HnswIndex {
    fn search(&self, q: &[f32], k: usize) -> Vec<(usize, f32)> {
        let ef = self.ef_search.max(k);
        self.hnsw
            .search(&q.to_vec(), k, ef)
            .into_iter()
            .map(|nb| (nb.d_id, nb.distance))
            .collect()
    }
    fn len(&self) -> usize {
        self.len
    }
}
