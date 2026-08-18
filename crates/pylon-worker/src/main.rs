//! `pylon` — M1 single-worker SQL runner. Reads SQL, returns RecordBatches.

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use pylon_plan::physical::PhysicalPlan;
use pylon_plan::physical::physical_expr::PhysicalExpr as PE;
use pylon_plan::translate::{logical_from_sql, physical_from_logical, CatalogStub};
use pylon_runtime::ops::{FilterOp, ProjectOp, SeqScanOp};
use pylon_runtime::{Driver, PipelineOp};
use std::sync::Arc;
use tracing::info;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    init_tracing();

    let args = parse_args().context("parse args")?;
    info!("pylon M1 starting");

    let catalog = if let Some(p) = args.path {
        let mut cat = CatalogStub::new();
        let schema = peek_parquet_schema(&p)?;
        cat.register(&args.table, schema, &p);
        cat
    } else {
        CatalogStub::with_builtin()
    };

    let logical = logical_from_sql(&args.sql, &catalog)?;
    let physical = physical_from_logical(logical)?;
    info!("plan:\n{:#?}", physical);

    let ops = build_ops(&physical, &catalog)?;
    info!("built {} ops", ops.len());

    // Trino-aligned: build Pipeline first, then a Driver to run it.
    let pipeline = std::sync::Arc::new(pylon_runtime::Pipeline::new(ops));
    let driver = pylon_runtime::Driver::new(pipeline).with_mode(pylon_runtime::DriverMode::PerOpTokioTask);
    let mut output = driver.run(None).await?;

    if let Some(out_path) = args.out {
        write_parquet(&mut output, &out_path).await?;
        info!("wrote {}", out_path);
    } else {
        let mut count = 0usize;
        let mut total_rows = 0usize;
        while let Some(batch) = output.recv().await {
            count += 1;
            total_rows += batch.num_rows();
            info!(
                "batch #{count}: rows={} cols={} schema={}",
                batch.num_rows(),
                batch.num_columns(),
                batch.schema()
            );
            if count >= 10 {
                info!("(truncated)");
                break;
            }
        }
        info!("summary: batches={count} rows={total_rows}");
    }
    Ok(())
}

#[derive(Debug)]
struct Args {
    sql: String,
    table: String,
    path: Option<String>,
    out: Option<String>,
}

fn parse_args() -> Result<Args> {
    let mut sql = String::new();
    let mut table = "sample".to_string();
    let mut path: Option<String> = None;
    let mut out: Option<String> = None;

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--sql" => { sql = argv.get(i + 1).cloned().unwrap_or_default(); i += 2; }
            "--table" => { table = argv.get(i + 1).cloned().unwrap_or_default(); i += 2; }
            "--path" => { path = Some(argv.get(i + 1).cloned().unwrap_or_default()); i += 2; }
            "--out" => { out = Some(argv.get(i + 1).cloned().unwrap_or_default()); i += 2; }
            "--help" | "-h" => { print_help(); std::process::exit(0); }
            other => return Err(anyhow::anyhow!("unknown arg: {other}")),
        }
    }

    if sql.is_empty() {
        print_help();
        std::process::exit(2);
    }
    Ok(Args { sql, table, path, out })
}

fn print_help() {
    println!(
        "pylon M1 — single-worker pipeline runner

USAGE
  pylon --sql <SQL> [--table name] [--path file.parquet] [--out file.parquet]

EXAMPLES
  pylon --sql \"SELECT * FROM sample WHERE id > 5\"
  pylon --sql \"SELECT id FROM t WHERE amount >= 100\" --table t --path t.parquet \\
        --out result.parquet

SUPPORTED SQL SUBSET (M1)
  SELECT [* | col] FROM <table> [WHERE <col> <op> <literal>]
  ops: >  <  >=  <=  =  <>
  literals: integer, float, string
"
    );
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("pylon=info"))
        )
        .try_init();
}

fn peek_parquet_schema(path: &str) -> Result<arrow_schema::SchemaRef> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(Arc::new(builder.schema().as_ref().clone()))
}

fn build_ops(plan: &PhysicalPlan, catalog: &CatalogStub) -> Result<Vec<Box<dyn PipelineOp>>> {
    fn go(plan: &PhysicalPlan, catalog: &CatalogStub) -> Result<Vec<Box<dyn PipelineOp>>> {
        Ok(match plan {
            PhysicalPlan::SeqScan { table, .. } => {
                let path = catalog.get_path(table)?.to_string();
                vec![Box::new(SeqScanOp::new(path, 8192))]
            }
            PhysicalPlan::Filter { input, predicate } => {
                let mut ops = go(input, catalog)?;
                let (col, op_str, lit) = decompose_filter(predicate)?;
                ops.push(Box::new(FilterOp::new(col, op_str, lit)));
                ops
            }
            PhysicalPlan::Project { input, projections, schema } => {
                let mut ops = go(input, catalog)?;
                let col_names: Vec<String> = projections.iter().map(col_name).collect();
                let fields: arrow_schema::Fields = schema.fields().clone();
                let out_schema = Arc::new(arrow_schema::Schema::new(fields));
                ops.push(Box::new(ProjectOp::new(col_names, out_schema)));
                ops
            }
        })
    }
    go(plan, catalog)
}

fn col_name(e: &PE) -> String {
    match e {
        PE::Column { field, .. } => field.name().clone(),
        PE::Literal { .. } | PE::BinaryOp { .. } => "_".into(),
    }
}

fn decompose_filter(e: &PE) -> Result<(String, String, String)> {
    Ok(match e {
        PE::BinaryOp { left, op, right } => {
            let col = col_name(left);
            let lit = match right.as_ref() {
                PE::Literal { value, .. } => value.clone(),
                _ => "0".to_string(),
            };
            (col, op.clone(), lit)
        }
        _ => ("_".into(), "=".into(), "0".into()),
    })
}

async fn write_parquet(
    output: &mut tokio::sync::mpsc::Receiver<RecordBatch>,
    path: &str,
) -> Result<()> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(b) = output.recv().await {
        batches.push(b);
    }

    if batches.is_empty() {
        File::create(path)?;
        return Ok(());
    }

    let combined = {
        let schema = batches[0].schema();
        arrow_select::concat::concat_batches(&schema, &batches)?
    };

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, combined.schema(), Some(props))?;
    writer.write(&combined)?;
    writer.close()?;
    Ok(())
}
