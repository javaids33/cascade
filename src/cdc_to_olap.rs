//! Phase 3b: drain Turso CDC -> DuckDB + Apache Iceberg.
//!
//! Tails turso_cdc with a change_id cursor, decodes before/after via Turso SQL helpers, applies
//! INSERT/UPDATE/DELETE to DuckDB, and appends the materialized rows to a real Iceberg table
//! (iceberg-rust 0.9.1 + SqlCatalog). Usage: `cascade cdc-to-olap [primary_db]`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arrow::array::{Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use serde_json::{json, Value as J};
use turso::{Builder, Value};

use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{
    SqlBindStyle, SqlCatalogBuilder, SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI,
    SQL_CATALOG_PROP_WAREHOUSE,
};
use parquet::file::properties::WriterProperties;

use crate::common::{
    db_dir, ensure_dirs, patents_schema, round, save_result, work, PATENT_COLS,
};

const BATCH: usize = 10_000;
const TABLE: &str = "patents";

/// Column-wise accumulator for the rows that land in DuckDB + Iceberg.
#[derive(Default)]
struct RowAccum {
    patent_id: Vec<String>,
    title: Vec<Option<String>>,
    abstract_: Vec<Option<String>>,
    assignee: Vec<Option<String>>,
    grant_date: Vec<Option<String>>,
    grant_year: Vec<Option<i32>>,
    claims_len: Vec<Option<i32>>,
    n_cpc: Vec<Option<i32>>,
    n_citations: Vec<Option<i32>>,
}

fn js(o: &J, key: &str) -> Option<String> {
    o.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}
fn ji(o: &J, key: &str) -> Option<i32> {
    o.get(key).and_then(|v| v.as_i64()).map(|n| n as i32)
}

impl RowAccum {
    fn push(&mut self, o: &J) {
        self.patent_id.push(js(o, "patent_id").unwrap_or_default());
        self.title.push(js(o, "title"));
        self.abstract_.push(js(o, "abstract"));
        self.assignee.push(js(o, "assignee"));
        self.grant_date.push(js(o, "grant_date"));
        self.grant_year.push(ji(o, "grant_year"));
        self.claims_len.push(ji(o, "claims_len"));
        self.n_cpc.push(ji(o, "n_cpc"));
        self.n_citations.push(ji(o, "n_citations"));
    }
    fn len(&self) -> usize {
        self.patent_id.len()
    }
    /// Build an arrow RecordBatch using the iceberg table's write schema (field-id metadata).
    fn record_batch(&self, write_schema: Arc<arrow::datatypes::Schema>) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            write_schema,
            vec![
                Arc::new(StringArray::from(self.patent_id.clone())),
                Arc::new(StringArray::from(self.title.clone())),
                Arc::new(StringArray::from(self.abstract_.clone())),
                Arc::new(StringArray::from(self.assignee.clone())),
                Arc::new(StringArray::from(self.grant_date.clone())),
                Arc::new(Int32Array::from(self.grant_year.clone())),
                Arc::new(Int32Array::from(self.claims_len.clone())),
                Arc::new(Int32Array::from(self.n_cpc.clone())),
                Arc::new(Int32Array::from(self.n_citations.clone())),
            ],
        )?)
    }
}

/// Map a decoded patents JSON object to DuckDB positional params (PATENT_COLS order).
fn duck_params(o: &J) -> Vec<duckdb::types::Value> {
    use duckdb::types::Value as DV;
    let mut v = Vec::with_capacity(PATENT_COLS.len());
    for (i, c) in PATENT_COLS.iter().enumerate() {
        if i < 5 {
            v.push(match js(o, c) {
                Some(s) => DV::Text(s),
                None => DV::Null,
            });
        } else {
            v.push(match ji(o, c) {
                Some(n) => DV::Int(n),
                None => DV::Null,
            });
        }
    }
    v
}

pub async fn run(primary: Option<String>) -> Result<()> {
    ensure_dirs()?;
    let primary = primary.unwrap_or_else(|| db_dir().join("cdc_on.db").to_string_lossy().to_string());
    let olap = work().join("olap");
    let _ = std::fs::remove_dir_all(&olap);
    std::fs::create_dir_all(&olap)?;
    let duck_path = olap.join("olap.duckdb");
    let wh = olap.join("iceberg");
    std::fs::create_dir_all(&wh)?;

    // --- DuckDB sink ---
    let duck = duckdb::Connection::open(&duck_path)?;
    duck.execute_batch(
        "CREATE TABLE patents(patent_id VARCHAR PRIMARY KEY, title VARCHAR, abstract VARCHAR, \
         assignee VARCHAR, grant_date VARCHAR, grant_year INTEGER, claims_len INTEGER, \
         n_cpc INTEGER, n_citations INTEGER)",
    )?;

    // --- Iceberg sink (real table via SqlCatalog) ---
    let cat_db = wh.join("catalog.db");
    std::fs::File::create(&cat_db)?;
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "local",
            HashMap::from_iter([
                (
                    SQL_CATALOG_PROP_URI.to_string(),
                    format!("sqlite://{}?mode=rwc", cat_db.display()),
                ),
                (
                    SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                    format!("file://{}", wh.display()),
                ),
                (
                    SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                    SqlBindStyle::QMark.to_string(),
                ),
            ]),
        )
        .await?;
    let ns = NamespaceIdent::new("lake".to_string());
    if !catalog.namespace_exists(&ns).await? {
        catalog.create_namespace(&ns, HashMap::new()).await?;
    }
    let table_id = TableIdent::new(ns.clone(), TABLE.to_string());
    let ice_schema = arrow_schema_to_schema_auto_assign_ids(patents_schema().as_ref())?;
    let table = if catalog.table_exists(&table_id).await? {
        catalog.load_table(&table_id).await?
    } else {
        let creation = TableCreation::builder()
            .name(TABLE.to_string())
            .schema(ice_schema)
            .build();
        catalog.create_table(&ns, creation).await?
    };

    // --- drain turso_cdc ---
    let db = Builder::new_local(&primary).build().await?;
    let src = db.connect()?;
    let decode_sql = format!(
        "SELECT change_id, change_type, \
         bin_record_json_object(table_columns_json_array(table_name), after)  AS after_json, \
         bin_record_json_object(table_columns_json_array(table_name), before) AS before_json \
         FROM turso_cdc WHERE change_id > ?1 AND table_name = '{TABLE}' \
         ORDER BY change_id ASC LIMIT {BATCH}"
    );

    let duck_ins = "INSERT INTO patents VALUES (?,?,?,?,?,?,?,?,?)";
    let duck_del = "DELETE FROM patents WHERE patent_id=?";

    let mut cursor: i64 = 0;
    let (mut n_ins, mut n_upd, mut n_del) = (0u64, 0u64, 0u64);
    let mut accum = RowAccum::default();
    let t0 = Instant::now();
    loop {
        let mut rows = src.query(&decode_sql, (cursor,)).await?;
        let mut got = 0usize;
        while let Some(row) = rows.next().await? {
            got += 1;
            let change_id = match row.get_value(0)? {
                Value::Integer(i) => i,
                _ => cursor,
            };
            cursor = change_id;
            let ctype = match row.get_value(1)? {
                Value::Integer(i) => i,
                _ => continue,
            };
            let after = match row.get_value(2)? {
                Value::Text(s) => Some(s.to_string()),
                _ => None,
            };
            let before = match row.get_value(3)? {
                Value::Text(s) => Some(s.to_string()),
                _ => None,
            };
            match ctype {
                1 => {
                    if let Some(a) = &after {
                        let o: J = serde_json::from_str(a)?;
                        duck.execute(
                            duck_ins,
                            duckdb::params_from_iter(duck_params(&o).into_iter()),
                        )?;
                        accum.push(&o);
                        n_ins += 1;
                    }
                }
                0 => {
                    if let Some(a) = &after {
                        let o: J = serde_json::from_str(a)?;
                        let pid = js(&o, "patent_id").unwrap_or_default();
                        duck.execute(duck_del, [pid])?;
                        duck.execute(
                            duck_ins,
                            duckdb::params_from_iter(duck_params(&o).into_iter()),
                        )?;
                        accum.push(&o);
                        n_upd += 1;
                    }
                }
                -1 => {
                    if let Some(b) = &before {
                        let o: J = serde_json::from_str(b)?;
                        let pid = js(&o, "patent_id").unwrap_or_default();
                        duck.execute(duck_del, [pid])?;
                        n_del += 1;
                    }
                }
                _ => {}
            }
        }
        if got == 0 {
            break;
        }
    }
    let dt = t0.elapsed().as_secs_f64();

    // --- append accumulated rows to Iceberg in one transaction ---
    if accum.len() > 0 {
        let write_schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        let batch = accum.record_batch(write_schema)?;
        let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
        let file_name_generator =
            DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_builder = DataFileWriterBuilder::new(rolling_builder);
        let mut writer = data_file_builder.build(None).await?;
        writer.write(batch).await?;
        let data_files = writer.close().await?;
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action.apply(tx)?;
        let _table = tx.commit(&catalog).await?;
    }

    // --- consistency check ---
    let duck_count: i64 = duck.query_row("SELECT COUNT(*) FROM patents", [], |r| r.get(0))?;
    let reloaded = catalog.load_table(&table_id).await?;
    let scan = reloaded.scan().select_all().build()?;
    let stream = scan.to_arrow().await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    let ice_count: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
    let total = n_ins + n_upd + n_del;
    let duck_bytes = std::fs::metadata(&duck_path).map(|m| m.len()).unwrap_or(0);

    let res = save_result(
        "cdc_to_olap",
        json!({
            "primary_db": primary,
            "batch": BATCH,
            "seconds": round(dt, 3),
            "changes_applied": {"insert": n_ins, "update": n_upd, "delete": n_del},
            "total_changes": total,
            "changes_per_sec": if dt > 0.0 { (total as f64 / dt).round() as i64 } else { 0 },
            "duckdb_rows": duck_count,
            "iceberg_rows": ice_count,
            "duckdb_bytes": duck_bytes,
            "consistent": duck_count == ice_count,
        }),
    )?;
    println!("{}", serde_json::to_string_pretty(&res)?);
    Ok(())
}
