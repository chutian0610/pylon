use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;

fn main() {
    let n = 100_000usize;
    let mut ids = Vec::with_capacity(n);
    let mut names = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    for i in 0..n {
        ids.push(i as i64);
        names.push(format!("name_{:05}", i));
        amounts.push((i as f64) * 1.5 + 0.01);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(amounts)),
        ],
    ).unwrap();
    // data/ is gitignored, so a fresh checkout (e.g. the GH Actions
    // runner) will not have the directory. Create it before
    // File::create; idempotent on dev boxes where data/ already
    // exists.
    std::fs::create_dir_all("data").expect("create data/");
    let file = File::create("data/sample.parquet").unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    println!("wrote 100K rows to data/sample.parquet");
}
