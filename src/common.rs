//! Shared config + helpers for the Turso edge->CDC->OLAP harness.
//!
//! All runtime paths derive from the repo root and are overridable via env vars, so the
//! harness runs unchanged on Linux/WSL and macOS. No machine-specific paths.
//!
//! Env overrides:
//!   TURSO_EXP_HOME   working dir for data/db/out (default: <repo>/.work)
//!   TURSO_REMOTE_URL sync server URL            (default: http://127.0.0.1:8080)
//!   PATENTS_JSONL    optional real source data  (default: unset -> synthetic)
//!   TEO_REPO_ROOT    repo root override         (default: current working dir)

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use turso::Value;

/// Columns of the patents fact table (single source of truth).
pub const PATENT_COLS: [&str; 9] = [
    "patent_id",
    "title",
    "abstract",
    "assignee",
    "grant_date",
    "grant_year",
    "claims_len",
    "n_cpc",
    "n_citations",
];

pub const PATENTS_DDL: &str = "CREATE TABLE IF NOT EXISTS patents (
  patent_id TEXT PRIMARY KEY, title TEXT, abstract TEXT, assignee TEXT,
  grant_date TEXT, grant_year INTEGER, claims_len INTEGER, n_cpc INTEGER, n_citations INTEGER)";
pub const PATENTS_INSERT: &str = "INSERT INTO patents VALUES (?,?,?,?,?,?,?,?,?)";

pub fn repo_root() -> PathBuf {
    if let Ok(r) = std::env::var("TEO_REPO_ROOT") {
        return PathBuf::from(r);
    }
    std::env::current_dir().expect("cwd")
}

pub fn work() -> PathBuf {
    std::env::var("TURSO_EXP_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join(".work"))
}

pub fn data_dir() -> PathBuf {
    work().join("data")
}
pub fn db_dir() -> PathBuf {
    work().join("db")
}
pub fn out_dir() -> PathBuf {
    work().join("out")
}
pub fn results_dir() -> PathBuf {
    repo_root().join("results")
}

/// A run identifier for archiving a self-consistent set of results under `results/<run-id>/`.
/// Set `CASCADE_RUN_ID` (or pass `--run-id` to `run-all`) to keep a stable baseline instead of
/// only the overwritten `results/*.json`. `None` = archive disabled (just the flat latest files).
pub fn run_id() -> Option<String> {
    std::env::var("CASCADE_RUN_ID").ok().filter(|s| !s.is_empty())
}

pub fn remote_url() -> String {
    std::env::var("TURSO_REMOTE_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
}

pub fn patents_jsonl() -> Option<String> {
    std::env::var("PATENTS_JSONL").ok().filter(|s| !s.is_empty())
}

pub fn ensure_dirs() -> Result<()> {
    for d in [data_dir(), db_dir(), out_dir(), results_dir()] {
        fs::create_dir_all(&d).with_context(|| format!("mkdir {}", d.display()))?;
    }
    Ok(())
}

/// Remove a turso/duckdb db file plus all sidecars: WAL/shm and the sync engine's metadata
/// (`-info`, `-changes`, `-wal-revert`). Stale sync metadata without a main db makes the sync
/// engine fail with "main DB file doesn't exist, but metadata is".
pub fn remove_db_files(path: &Path) {
    let p = path.to_string_lossy().to_string();
    for ext in [
        "",
        "-wal",
        "-shm",
        ".tshm",
        ".wal",
        "-info",
        "-changes",
        "-wal-revert",
    ] {
        let _ = fs::remove_file(format!("{p}{ext}"));
    }
}

/// Total on-disk footprint of a db: main file + WAL/shm sidecars. Turso keeps recent writes
/// in the WAL until checkpoint, so the main file alone understates real storage — checkpoint
/// first (`PRAGMA wal_checkpoint(TRUNCATE)`) for the cleanest single number.
pub fn db_footprint(path: &Path) -> u64 {
    let p = path.to_string_lossy().to_string();
    let mut total = 0;
    for ext in ["", "-wal", "-shm", ".tshm", ".wal"] {
        if let Ok(m) = fs::metadata(format!("{p}{ext}")) {
            total += m.len();
        }
    }
    total
}

// ----------------------------------------------------------------------------
// Arrow schemas for the three Parquet tables.
// ----------------------------------------------------------------------------

pub fn patents_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("patent_id", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, true),
        Field::new("abstract", DataType::Utf8, true),
        Field::new("assignee", DataType::Utf8, true),
        Field::new("grant_date", DataType::Utf8, true),
        Field::new("grant_year", DataType::Int32, true),
        Field::new("claims_len", DataType::Int32, true),
        Field::new("n_cpc", DataType::Int32, true),
        Field::new("n_citations", DataType::Int32, true),
    ]))
}

pub fn cpc_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("patent_id", DataType::Utf8, false),
        Field::new("cpc", DataType::Utf8, false),
        Field::new("cpc_section", DataType::Utf8, false),
    ]))
}

pub fn citations_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("patent_id", DataType::Utf8, false),
        Field::new("cited_id", DataType::Utf8, false),
    ]))
}

// ----------------------------------------------------------------------------
// Parquet IO.
// ----------------------------------------------------------------------------

/// Streaming Parquet writer that buffers columnar batches and flushes periodically.
pub struct ParquetSink {
    writer: ArrowWriter<fs::File>,
    schema: Arc<Schema>,
}

impl ParquetSink {
    pub fn create(path: &Path, schema: Arc<Schema>) -> Result<Self> {
        let file = fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
        let writer = ArrowWriter::try_new(file, schema.clone(), None)?;
        Ok(Self { writer, schema })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        debug_assert_eq!(batch.schema(), self.schema);
        self.writer.write(batch)?;
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        self.writer.close()?;
        Ok(())
    }
}

/// Read every RecordBatch from a Parquet file (arrow 55).
pub fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(20_000)
        .build()?;
    let mut out = Vec::new();
    for b in reader {
        out.push(b?);
    }
    Ok(out)
}

/// Total row count across all batches.
pub fn rows_in(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

pub fn n_patents() -> Result<usize> {
    let batches = read_parquet(&data_dir().join("patents.parquet"))?;
    Ok(rows_in(&batches))
}

/// Convert one cell of an arrow column to a turso `Value` (Utf8 / Int32 only — the
/// types this harness uses). Nulls become `Value::Null`.
fn cell_to_value(col: &dyn Array, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        Value::Text(a.value(row).to_string())
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        Value::Integer(a.value(row) as i64)
    } else {
        Value::Null
    }
}

/// Flatten Parquet batches into row-major turso parameter vectors (one Vec<Value> per row).
pub fn batches_to_turso_rows(batches: &[RecordBatch]) -> Vec<Vec<Value>> {
    let mut rows = Vec::with_capacity(rows_in(batches));
    for b in batches {
        let ncol = b.num_columns();
        for r in 0..b.num_rows() {
            let mut v = Vec::with_capacity(ncol);
            for c in 0..ncol {
                v.push(cell_to_value(b.column(c).as_ref(), r));
            }
            rows.push(v);
        }
    }
    rows
}

/// Load patents Parquet as turso rows, optionally limited.
pub fn patents_rows(limit: Option<usize>) -> Result<Vec<Vec<Value>>> {
    let path = data_dir().join("patents.parquet");
    if !path.exists() {
        anyhow::bail!(
            "missing {} -- run `cascade gen-synthetic` or `cascade prep-data` first",
            path.display()
        );
    }
    let batches = read_parquet(&path)?;
    let mut rows = batches_to_turso_rows(&batches);
    if let Some(n) = limit {
        rows.truncate(n);
    }
    Ok(rows)
}

/// Extract (patent_id, abstract) string pairs from patents Parquet, limited.
pub fn patents_id_abstract(limit: usize) -> Result<Vec<(String, String)>> {
    let batches = read_parquet(&data_dir().join("patents.parquet"))?;
    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("col0 not utf8")?;
        let abs = b
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("col2 not utf8")?;
        for r in 0..b.num_rows() {
            if out.len() >= limit {
                return Ok(out);
            }
            let a = if abs.is_null(r) { "" } else { abs.value(r) };
            out.push((ids.value(r).to_string(), a.to_string()));
        }
    }
    Ok(out)
}

// ----------------------------------------------------------------------------
// Results + timing.
// ----------------------------------------------------------------------------

/// Write a result JSON to both the runtime OUT dir and the committed results/ dir.
pub fn save_result(name: &str, mut payload: serde_json::Value) -> Result<serde_json::Value> {
    if let Some(obj) = payload.as_object_mut() {
        let mut full = serde_json::Map::new();
        full.insert("_name".to_string(), serde_json::Value::String(name.to_string()));
        for (k, v) in obj.iter() {
            full.insert(k.clone(), v.clone());
        }
        payload = serde_json::Value::Object(full);
    }
    let pretty = serde_json::to_string_pretty(&payload)?;
    for d in [out_dir(), results_dir()] {
        fs::create_dir_all(&d).ok();
        fs::write(d.join(format!("{name}.json")), &pretty)?;
    }
    // Archive a stable, self-consistent copy under results/<run-id>/ when a run id is set, so a
    // later `./run.sh` (which overwrites results/*.json) doesn't clobber a baseline you cited.
    if let Some(id) = run_id() {
        let d = results_dir().join(id);
        fs::create_dir_all(&d).ok();
        let _ = fs::write(d.join(format!("{name}.json")), &pretty);
    }
    println!("[saved] results/{name}.json");
    Ok(payload)
}

pub fn round(x: f64, places: i32) -> f64 {
    let f = 10f64.powi(places);
    (x * f).round() / f
}
