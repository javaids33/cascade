//! Phase 5: edge vector search. Embed patent abstracts with a dependency-free hashing
//! vectorizer, store as F32_BLOB in Turso, and measure `vector_distance_cos` top-k latency as
//! the table grows. Usage: `cascade vector [max_rows] [dim]`.

use std::hash::{Hash, Hasher};
use std::time::Instant;

use anyhow::Result;
use serde_json::json;
use turso::{Builder, Value};

use crate::common::{
    db_dir, ensure_dirs, n_patents, patents_id_abstract, remove_db_files, round, save_result,
};

fn hash_token(tok: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    tok.hash(&mut h);
    h.finish()
}

/// Hashing vectorizer: bucket lowercase [a-z]+ tokens into `dim` dims, L2-normalize.
fn embed(text: &str, dim: usize) -> Vec<f32> {
    let mut v = vec![0f32; dim];
    let mut cur = String::new();
    let push = |cur: &mut String, v: &mut Vec<f32>| {
        if !cur.is_empty() {
            let idx = (hash_token(cur) % dim as u64) as usize;
            v[idx] += 1.0;
            cur.clear();
        }
    };
    for ch in text.chars() {
        if ch.is_ascii_alphabetic() {
            cur.push(ch.to_ascii_lowercase());
        } else {
            push(&mut cur, &mut v);
        }
    }
    push(&mut cur, &mut v);
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
    v
}

fn to_json(v: &[f32]) -> String {
    let mut s = String::from("[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{:.5}", x));
    }
    s.push(']');
    s
}

async fn measure(
    conn: &turso::Connection,
    nrows: usize,
    qvecs: &[String],
    runs: usize,
) -> Result<serde_json::Value> {
    let mut times = Vec::with_capacity(qvecs.len());
    for qj in qvecs {
        let mut best = f64::INFINITY;
        for _ in 0..runs {
            let t = Instant::now();
            let mut rows = conn
                .query(
                    "SELECT patent_id, vector_distance_cos(emb, vector32(?1)) d FROM docs ORDER BY d LIMIT 10",
                    (qj.clone(),),
                )
                .await?;
            while rows.next().await?.is_some() {}
            best = best.min(t.elapsed().as_secs_f64());
        }
        times.push(best);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = times[times.len() / 2];
    let p95 = times[((times.len() as f64) * 0.95) as usize];
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    Ok(json!({
        "rows": nrows,
        "p50_ms": round(p50 * 1000.0, 2),
        "p95_ms": round(p95 * 1000.0, 2),
        "mean_ms": round(mean * 1000.0, 2),
    }))
}

pub async fn run(max_rows: Option<usize>, dim: Option<usize>) -> Result<()> {
    ensure_dirs()?;
    let max = match max_rows {
        Some(m) => m,
        None => n_patents()?.min(50_000),
    };
    let dim = dim.unwrap_or(64);
    let path = db_dir().join("vector.db");

    println!("embedding {max} abstracts (dim={dim})...");
    let pa = patents_id_abstract(max)?;
    let rows: Vec<(String, Vec<f32>)> = pa
        .iter()
        .map(|(pid, abs)| (pid.clone(), embed(abs, dim)))
        .collect();

    remove_db_files(&path);
    let db = Builder::new_local(path.to_str().unwrap()).build().await?;
    let conn = db.connect()?;
    conn.execute(
        &format!("CREATE TABLE docs(patent_id TEXT PRIMARY KEY, emb F32_BLOB({dim}))"),
        (),
    )
    .await?;

    let mut milestones: Vec<usize> = [1000, 5000, 10000, 25000, 50000, 100000, 171317]
        .into_iter()
        .filter(|m| *m <= max)
        .collect();
    if !milestones.contains(&max) {
        milestones.push(max);
    }
    milestones.sort_unstable();
    milestones.dedup();

    // 20 fixed query vectors (reuse some doc embeddings) — JSON for SQL, floats for the ANN index.
    let query_idx: Vec<usize> = (0..rows.len().min(2000)).step_by(100).take(20).collect();
    let qvecs: Vec<String> = query_idx.iter().map(|&i| to_json(&rows[i].1)).collect();
    let qfloats: Vec<Vec<f32>> = query_idx.iter().map(|&i| rows[i].1.clone()).collect();

    let mut curve = Vec::new();
    let mut inserted = 0usize;
    let mut ins = conn.prepare("INSERT INTO docs VALUES (?1, vector32(?2))").await?;
    for m in milestones {
        conn.execute("BEGIN", ()).await?;
        for (pid, v) in &rows[inserted..m] {
            ins.execute((pid.clone(), to_json(v))).await?;
        }
        conn.execute("COMMIT", ()).await?;
        inserted = m;
        let stat = measure(&conn, m, &qvecs, 3).await?;
        println!(
            "  rows={:>7}  p50={:>7}ms  p95={:>7}ms",
            m, stat["p50_ms"], stat["p95_ms"]
        );
        curve.push(stat);
    }

    // correctness: a doc's own embedding should return itself as nearest.
    let (pid0, v0) = &rows[0];
    let mut r = conn
        .query(
            "SELECT patent_id FROM docs ORDER BY vector_distance_cos(emb, vector32(?1)) LIMIT 1",
            (to_json(v0),),
        )
        .await?;
    let nn = match r.next().await? {
        Some(row) => match row.get_value(0)? {
            Value::Text(s) => s.to_string(),
            _ => String::new(),
        },
        None => String::new(),
    };
    let self_nn = &nn == pid0;
    let db_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    // ANN (HNSW) vs brute-force ground truth over the full corpus: recall@10 + latency. This is
    // the answer to the "linear scan" honest-limit — an in-memory index that stays ~ms at scale.
    let ann = if qfloats.is_empty() {
        serde_json::Value::Null
    } else {
        use crate::index::{BruteForce, HnswIndex, VectorIndex};
        let all: Vec<Vec<f32>> = rows.iter().map(|(_, v)| v.clone()).collect();
        let bf = BruteForce::new(all.clone());
        let tb = Instant::now();
        let hnsw = HnswIndex::build(&all, 16, 200, 64);
        let build_sec = tb.elapsed().as_secs_f64();
        let mut times = Vec::new();
        let mut recalls = Vec::new();
        for q in &qfloats {
            let truth: std::collections::HashSet<usize> =
                bf.search(q, 10).into_iter().map(|(i, _)| i).collect();
            let t = Instant::now();
            let got = hnsw.search(q, 10);
            times.push(t.elapsed().as_secs_f64());
            let hit = got.iter().filter(|(i, _)| truth.contains(i)).count();
            recalls.push(hit as f64 / 10.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = times[times.len() / 2];
        let p95 = times[((times.len() as f64) * 0.95) as usize];
        let recall = recalls.iter().sum::<f64>() / recalls.len() as f64;
        println!(
            "  [ann] hnsw {} vecs built in {:.2}s  p50={:.3}ms  recall@10={:.3}",
            all.len(), build_sec, p50 * 1000.0, recall
        );
        json!({
            "index": "hnsw", "m": 16, "ef_construction": 200, "ef_search": 64,
            "rows": all.len(),
            "build_sec": round(build_sec, 3),
            "p50_ms": round(p50 * 1000.0, 3),
            "p95_ms": round(p95 * 1000.0, 3),
            "recall_at_10": round(recall, 3),
        })
    };

    save_result(
        "vector",
        json!({
            "max_rows": max,
            "dim": dim,
            "latency_curve": curve,
            "self_nn_correct": self_nn,
            "db_bytes": db_bytes,
            "ann": ann,
            "note": "brute-force in Turso v0.6.1 (linear); the `ann` block is an in-memory HNSW (hnsw_rs) \
                     over the same vectors — recall@10 vs brute-force ground truth + latency.",
        }),
    )?;
    println!("\nself-NN correct: {self_nn}");
    Ok(())
}
