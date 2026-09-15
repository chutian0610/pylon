use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;

fn arg(flag: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == flag)
        .and_then(|pos| args.get(pos + 1))
        .cloned()
}

fn main() {
    // Scale knobs (M4.S8): `--rows N --groups G --out PATH`. Defaults
    // reproduce the original 100k all-distinct file exactly. Groups
    // < rows makes the aggregate output smaller than the input — the
    // S8 shape (1M groups over 1B rows keeps the final batch and the
    // coord result set bounded while the input exercises spill).
    let n: usize = arg("--rows")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let groups: usize = arg("--groups").and_then(|v| v.parse().ok()).unwrap_or(n);
    let out = arg("--out").unwrap_or_else(|| "data/sample.parquet".to_string());
    assert!(groups > 0 && groups <= n, "groups must be in (0, rows]");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));

    // data/ is gitignored, so a fresh checkout (e.g. the GH Actions
    // runner) will not have the directory. Create it before
    // File::create; idempotent on dev boxes where data/ already
    // exists.
    if let Some(parent) = std::path::Path::new(&out).parent() {
        std::fs::create_dir_all(parent).expect("create output dir");
    }
    let file = File::create(&out).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    // Stream in 8192-row chunks: memory stays O(chunk) even at 1B
    // rows, and ArrowWriter splits them into parquet row groups.
    let chunk = 8192usize;
    let mut written = 0usize;
    while written < n {
        let take = chunk.min(n - written);
        let mut ids = Vec::with_capacity(take);
        let mut names = Vec::with_capacity(take);
        let mut amounts = Vec::with_capacity(take);
        for i in written..written + take {
            ids.push(i as i64);
            names.push(format!("name_{:08}", i % groups));
            amounts.push((i as f64) * 1.5 + 0.01);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(amounts)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        written += take;
    }
    writer.close().unwrap();
    println!("wrote {n} rows ({groups} groups) to {out}");
}
