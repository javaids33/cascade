//! Phase 1 (synthetic): generate patents/cpc/citations Parquet so the harness runs anywhere
//! with no external dataset. Mimics the real distributions closely enough for OLTP/OLAP/vector
//! benchmarking. Seeded for reproducibility (RNG differs from the Python harness, so exact byte
//! values differ, but distributions match). Usage: `cascade gen-synthetic [n]` or env SYNTH_N.

use std::collections::HashSet;

use anyhow::Result;
use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::Gamma;

use crate::common::{
    citations_schema, cpc_schema, data_dir, ensure_dirs, patents_schema, ParquetSink,
};

const SECTIONS: &[char] = &['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'Y'];
const PREFIXES: &[&str] = &[
    "ACME", "GLOBEX", "INITECH", "UMBRELLA", "STARK", "WAYNE", "TYRELL", "HOOLI", "PIED PIPER",
    "SOYLENT", "MASSIVE DYNAMIC", "CYBERDYNE", "OSCORP", "WONKA", "GRINGOTTS", "NAKATOMI",
    "VEHEMENT", "BLUE SUN", "WEYLAND", "ENCOM",
];
const SUFFIXES: &[&str] = &[
    "CORP", "LABS", "TECH", "INDUSTRIES", "SYSTEMS", "HOLDINGS", "R&D", "CO LTD",
];
const WORDS: &[&str] = &[
    "system", "method", "apparatus", "device", "circuit", "signal", "battery", "cell", "electrode",
    "anode", "cathode", "polymer", "membrane", "voltage", "current", "charge", "discharge",
    "module", "controller", "sensor", "array", "semiconductor", "wafer", "substrate", "dielectric",
    "capacitor", "inductor", "converter", "inverter", "grid", "thermal", "cooling", "fluid",
    "channel", "valve", "pump", "rotor", "stator", "magnet", "winding", "insulation",
];
const CPC_LETTERS: &[char] = &['B', 'C', 'D', 'F', 'G', 'H', 'K', 'L', 'M', 'N'];

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
        None => String::new(),
    }
}

fn sentence(rng: &mut StdRng, nwords: usize) -> String {
    let joined: Vec<&str> = (0..nwords).map(|_| WORDS[rng.gen_range(0..WORDS.len())]).collect();
    capitalize(&joined.join(" ")) + "."
}

fn make_abstract(rng: &mut StdRng) -> String {
    let nsent = rng.gen_range(3..=6);
    (0..nsent)
        .map(|_| {
            let nw = rng.gen_range(8..=18);
            sentence(rng, nw)
        })
        .collect::<Vec<_>>()
        .join(" ")
}

struct PatentsBuf {
    patent_id: Vec<String>,
    title: Vec<String>,
    abstract_: Vec<String>,
    assignee: Vec<Option<String>>,
    grant_date: Vec<String>,
    grant_year: Vec<i32>,
    claims_len: Vec<i32>,
    n_cpc: Vec<i32>,
    n_citations: Vec<i32>,
}
impl PatentsBuf {
    fn new() -> Self {
        Self {
            patent_id: vec![],
            title: vec![],
            abstract_: vec![],
            assignee: vec![],
            grant_date: vec![],
            grant_year: vec![],
            claims_len: vec![],
            n_cpc: vec![],
            n_citations: vec![],
        }
    }
    fn is_empty(&self) -> bool {
        self.patent_id.is_empty()
    }
    fn batch(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            patents_schema(),
            vec![
                std::sync::Arc::new(StringArray::from(self.patent_id.clone())),
                std::sync::Arc::new(StringArray::from(self.title.clone())),
                std::sync::Arc::new(StringArray::from(self.abstract_.clone())),
                std::sync::Arc::new(StringArray::from(self.assignee.clone())),
                std::sync::Arc::new(StringArray::from(self.grant_date.clone())),
                std::sync::Arc::new(Int32Array::from(self.grant_year.clone())),
                std::sync::Arc::new(Int32Array::from(self.claims_len.clone())),
                std::sync::Arc::new(Int32Array::from(self.n_cpc.clone())),
                std::sync::Arc::new(Int32Array::from(self.n_citations.clone())),
            ],
        )?)
    }
    fn clear(&mut self) {
        self.patent_id.clear();
        self.title.clear();
        self.abstract_.clear();
        self.assignee.clear();
        self.grant_date.clear();
        self.grant_year.clear();
        self.claims_len.clear();
        self.n_cpc.clear();
        self.n_citations.clear();
    }
}

pub fn run(n: usize) -> Result<()> {
    ensure_dirs()?;
    let data = data_dir();
    let mut rng = StdRng::seed_from_u64(42);

    let assignees: Vec<String> = PREFIXES
        .iter()
        .flat_map(|a| SUFFIXES.iter().map(move |b| format!("{a} {b}")))
        .collect();

    let mut pw_p = ParquetSink::create(&data.join("patents.parquet"), patents_schema())?;
    let mut pw_c = ParquetSink::create(&data.join("patent_cpc.parquet"), cpc_schema())?;
    let mut pw_x = ParquetSink::create(&data.join("patent_citations.parquet"), citations_schema())?;

    let ids: Vec<String> = (0..n).map(|i| format!("US-{}-B2", 9_000_000 + i)).collect();
    const BATCH: usize = 20_000;
    let mut pb = PatentsBuf::new();
    // cpc buffer
    let (mut c_pid, mut c_code, mut c_sec): (Vec<String>, Vec<String>, Vec<String>) =
        (vec![], vec![], vec![]);
    // citations buffer
    let (mut x_pid, mut x_cited): (Vec<String>, Vec<String>) = (vec![], vec![]);

    let gamma_cpc = Gamma::new(2.0, 12.0).unwrap();
    let gamma_cit = Gamma::new(1.3, 30.0).unwrap();
    let mut tot_cpc: usize = 0;
    let mut tot_cit: usize = 0;

    let flush_cpc = |w: &mut ParquetSink,
                     pid: &mut Vec<String>,
                     code: &mut Vec<String>,
                     sec: &mut Vec<String>|
     -> Result<()> {
        if !pid.is_empty() {
            let b = RecordBatch::try_new(
                cpc_schema(),
                vec![
                    std::sync::Arc::new(StringArray::from(pid.clone())),
                    std::sync::Arc::new(StringArray::from(code.clone())),
                    std::sync::Arc::new(StringArray::from(sec.clone())),
                ],
            )?;
            w.write(&b)?;
            pid.clear();
            code.clear();
            sec.clear();
        }
        Ok(())
    };
    let flush_cit =
        |w: &mut ParquetSink, pid: &mut Vec<String>, cited: &mut Vec<String>| -> Result<()> {
            if !pid.is_empty() {
                let b = RecordBatch::try_new(
                    citations_schema(),
                    vec![
                        std::sync::Arc::new(StringArray::from(pid.clone())),
                        std::sync::Arc::new(StringArray::from(cited.clone())),
                    ],
                )?;
                w.write(&b)?;
                pid.clear();
                cited.clear();
            }
            Ok(())
        };

    for i in 0..n {
        let pid = &ids[i];
        let year: i32 = rng.gen_range(2015..=2025);
        let sec = SECTIONS[rng.gen_range(0..SECTIONS.len())];
        let n_cpc = (rng.sample(gamma_cpc) as i32).max(1);
        let n_cit = rng.sample(gamma_cit) as i32;
        pb.patent_id.push(pid.clone());
        let title_nw = rng.gen_range(5..=14);
        pb.title.push(sentence(&mut rng, title_nw));
        pb.abstract_.push(make_abstract(&mut rng));
        pb.assignee.push(if rng.gen::<f64>() > 0.01 {
            Some(assignees[rng.gen_range(0..assignees.len())].clone())
        } else {
            None
        });
        pb.grant_date.push(format!(
            "{year}-{:02}-{:02}",
            rng.gen_range(1..=12),
            rng.gen_range(1..=28)
        ));
        pb.grant_year.push(year);
        pb.claims_len.push(rng.gen_range(80..=30_000));
        pb.n_cpc.push(n_cpc);
        pb.n_citations.push(n_cit);

        let mut seen: HashSet<String> = HashSet::new();
        for _ in 0..n_cpc {
            let code = format!(
                "{sec}{:02}{}{}/{:02}",
                rng.gen_range(1..=99),
                CPC_LETTERS[rng.gen_range(0..CPC_LETTERS.len())],
                rng.gen_range(1..=99),
                rng.gen_range(1..=99)
            );
            if seen.contains(&code) {
                continue;
            }
            seen.insert(code.clone());
            c_pid.push(pid.clone());
            c_code.push(code);
            c_sec.push(sec.to_string());
            tot_cpc += 1;
        }
        for _ in 0..n_cit {
            let j = if i > 0 { rng.gen_range(0..i) } else { 0 };
            x_pid.push(pid.clone());
            x_cited.push(ids[j].clone());
            tot_cit += 1;
        }

        if (i + 1) % BATCH == 0 {
            pw_p.write(&pb.batch()?)?;
            pb.clear();
            flush_cpc(&mut pw_c, &mut c_pid, &mut c_code, &mut c_sec)?;
            flush_cit(&mut pw_x, &mut x_pid, &mut x_cited)?;
            println!("  ...{} patents", i + 1);
        }
    }
    if !pb.is_empty() {
        pw_p.write(&pb.batch()?)?;
    }
    flush_cpc(&mut pw_c, &mut c_pid, &mut c_code, &mut c_sec)?;
    flush_cit(&mut pw_x, &mut x_pid, &mut x_cited)?;
    pw_p.close()?;
    pw_c.close()?;
    pw_x.close()?;
    println!(
        "DONE synthetic: {n} patents, {tot_cpc} cpc, {tot_cit} citations -> {}",
        data.display()
    );
    Ok(())
}
