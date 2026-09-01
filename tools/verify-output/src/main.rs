use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

fn main() {
    let path = std::env::args().nth(1).expect("path arg required");
    let file = File::open(&path).expect("open file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("builder");
    let schema = builder.schema().as_ref().clone();

    let reader = builder.build().expect("reader");
    let mut total = 0usize;
    let mut samples: Vec<Vec<String>> = Vec::new(); // up to 5 rows, each row → column values
    for batch in reader {
        let b = batch.expect("batch");
        total += b.num_rows();
        if samples.len() < 3 && b.num_rows() > 0 {
            let cols = b.columns();
            for r in 0..b.num_rows().min(3) {
                let row: Vec<String> = (0..cols.len())
                    .map(|c| arrow_value_to_string(&cols[c], r))
                    .collect();
                samples.push(row);
            }
        }
    }
    println!("rows: {total}");
    println!("schema ({} columns):", schema.fields().len());
    for f in schema.fields() {
        println!("  - {} : {:?}", f.name(), f.data_type());
    }
    println!("sample rows (up to 3):");
    for row in samples {
        println!("  {:?}", row);
    }
}

fn arrow_value_to_string(col: &std::sync::Arc<dyn arrow_array::Array>, r: usize) -> String {
    use arrow_array::*;
    if col.is_null(r) {
        return "NULL".to_string();
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(r)
            .to_string(),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(r)
            .to_string(),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(r)
            .to_string(),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(r)
            .to_string(),
        dt => format!("<{dt:?}>"),
    }
}
