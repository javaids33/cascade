//! Phase 1 (real data): parse a patents JSONL -> normalized Parquet (patents/cpc/citations).
//! Source from env PATENTS_JSONL or the path argument. Usage: `cascade prep-data [path] [limit]`.
//! Expected JSONL fields: patent_id, title, abstract, claims, cpc_codes[], assignee, grant_date,
//! cited_patent_ids[].

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};

use anyhow::{Context, Result};
use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use serde_json::Value as J;

use crate::common::{
    citations_schema, cpc_schema, data_dir, ensure_dirs, patents_schema, ParquetSink,
};

const BATCH: usize = 20_000;

fn str_opt(v: &J, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn str_list(v: &J, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|e| e.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

pub fn run(src: &str, limit: Option<usize>) -> Result<()> {
    ensure_dirs()?;
    let data = data_dir();
    let mut pw_p = ParquetSink::create(&data.join("patents.parquet"), patents_schema())?;
    let mut pw_c = ParquetSink::create(&data.join("patent_cpc.parquet"), cpc_schema())?;
    let mut pw_x = ParquetSink::create(&data.join("patent_citations.parquet"), citations_schema())?;

    // patents buffers
    let (mut pid, mut title, mut abs, mut assignee, mut gdate) = (
        Vec::<String>::new(),
        Vec::<Option<String>>::new(),
        Vec::<Option<String>>::new(),
        Vec::<Option<String>>::new(),
        Vec::<Option<String>>::new(),
    );
    let (mut gyear, mut clen, mut ncpc_col, mut ncit_col) = (
        Vec::<Option<i32>>::new(),
        Vec::<i32>::new(),
        Vec::<i32>::new(),
        Vec::<i32>::new(),
    );
    let (mut c_pid, mut c_code, mut c_sec) =
        (Vec::<String>::new(), Vec::<String>::new(), Vec::<String>::new());
    let (mut x_pid, mut x_cited) = (Vec::<String>::new(), Vec::<String>::new());

    let flush_patents = |w: &mut ParquetSink,
                         pid: &mut Vec<String>,
                         title: &mut Vec<Option<String>>,
                         abs: &mut Vec<Option<String>>,
                         assignee: &mut Vec<Option<String>>,
                         gdate: &mut Vec<Option<String>>,
                         gyear: &mut Vec<Option<i32>>,
                         clen: &mut Vec<i32>,
                         ncpc: &mut Vec<i32>,
                         ncit: &mut Vec<i32>|
     -> Result<()> {
        if pid.is_empty() {
            return Ok(());
        }
        let b = RecordBatch::try_new(
            patents_schema(),
            vec![
                std::sync::Arc::new(StringArray::from(pid.clone())),
                std::sync::Arc::new(StringArray::from(title.clone())),
                std::sync::Arc::new(StringArray::from(abs.clone())),
                std::sync::Arc::new(StringArray::from(assignee.clone())),
                std::sync::Arc::new(StringArray::from(gdate.clone())),
                std::sync::Arc::new(Int32Array::from(gyear.clone())),
                std::sync::Arc::new(Int32Array::from(clen.clone())),
                std::sync::Arc::new(Int32Array::from(ncpc.clone())),
                std::sync::Arc::new(Int32Array::from(ncit.clone())),
            ],
        )?;
        w.write(&b)?;
        pid.clear();
        title.clear();
        abs.clear();
        assignee.clear();
        gdate.clear();
        gyear.clear();
        clen.clear();
        ncpc.clear();
        ncit.clear();
        Ok(())
    };
    let flush3 = |w: &mut ParquetSink,
                  a: &mut Vec<String>,
                  b: &mut Vec<String>,
                  c: &mut Vec<String>,
                  schema: std::sync::Arc<arrow::datatypes::Schema>|
     -> Result<()> {
        if a.is_empty() {
            return Ok(());
        }
        let rb = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(a.clone())),
                std::sync::Arc::new(StringArray::from(b.clone())),
                std::sync::Arc::new(StringArray::from(c.clone())),
            ],
        )?;
        w.write(&rb)?;
        a.clear();
        b.clear();
        c.clear();
        Ok(())
    };
    let flush2 = |w: &mut ParquetSink,
                  a: &mut Vec<String>,
                  b: &mut Vec<String>,
                  schema: std::sync::Arc<arrow::datatypes::Schema>|
     -> Result<()> {
        if a.is_empty() {
            return Ok(());
        }
        let rb = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(a.clone())),
                std::sync::Arc::new(StringArray::from(b.clone())),
            ],
        )?;
        w.write(&rb)?;
        a.clear();
        b.clear();
        Ok(())
    };

    let f = File::open(src).with_context(|| format!("open {src}"))?;
    let reader = BufReader::new(f);
    let (mut n, mut ncpc, mut ncit) = (0usize, 0usize, 0usize);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(lim) = limit {
            if n >= lim {
                break;
            }
        }
        let r: J = serde_json::from_str(&line)?;
        let id = match str_opt(&r, "patent_id") {
            Some(s) => s,
            None => continue,
        };
        let gd = str_opt(&r, "grant_date").unwrap_or_default();
        let gy: Option<i32> = if gd.len() >= 4 && gd[..4].chars().all(|c| c.is_ascii_digit()) {
            gd[..4].parse().ok()
        } else {
            None
        };
        let cpc_uniq: BTreeSet<String> = str_list(&r, "cpc_codes").into_iter().collect();
        let cits = str_list(&r, "cited_patent_ids");
        let claims_len = str_opt(&r, "claims").map(|s| s.len()).unwrap_or(0) as i32;

        pid.push(id.clone());
        title.push(str_opt(&r, "title"));
        abs.push(str_opt(&r, "abstract"));
        assignee.push(str_opt(&r, "assignee"));
        gdate.push(if gd.is_empty() { None } else { Some(gd) });
        gyear.push(gy);
        clen.push(claims_len);
        ncpc_col.push(cpc_uniq.len() as i32);
        ncit_col.push(cits.len() as i32);

        for c in &cpc_uniq {
            c_pid.push(id.clone());
            c_code.push(c.clone());
            c_sec.push(c.chars().next().map(|ch| ch.to_string()).unwrap_or_default());
            ncpc += 1;
        }
        for cid in &cits {
            x_pid.push(id.clone());
            x_cited.push(cid.clone());
            ncit += 1;
        }
        n += 1;
        if n % BATCH == 0 {
            flush_patents(
                &mut pw_p,
                &mut pid,
                &mut title,
                &mut abs,
                &mut assignee,
                &mut gdate,
                &mut gyear,
                &mut clen,
                &mut ncpc_col,
                &mut ncit_col,
            )?;
            flush3(&mut pw_c, &mut c_pid, &mut c_code, &mut c_sec, cpc_schema())?;
            flush2(&mut pw_x, &mut x_pid, &mut x_cited, citations_schema())?;
            println!("  ...{n} patents");
        }
    }
    flush_patents(
        &mut pw_p,
        &mut pid,
        &mut title,
        &mut abs,
        &mut assignee,
        &mut gdate,
        &mut gyear,
        &mut clen,
        &mut ncpc_col,
        &mut ncit_col,
    )?;
    flush3(&mut pw_c, &mut c_pid, &mut c_code, &mut c_sec, cpc_schema())?;
    flush2(&mut pw_x, &mut x_pid, &mut x_cited, citations_schema())?;
    pw_p.close()?;
    pw_c.close()?;
    pw_x.close()?;
    println!("DONE: {n} patents, {ncpc} cpc, {ncit} citations -> {}", data.display());
    Ok(())
}
