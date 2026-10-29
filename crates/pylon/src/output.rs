//! 结果渲染：Arrow RecordBatch → stdout 文本表格。

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};

/// 渲染为对齐文本表格：表头、分隔线、数据行、行数脚注。
///
/// NULL 显示为 `NULL`；数值列右对齐，其余左对齐；行尾空白裁掉。
///
/// ```text
///    a | b
///  ----+----
///   -7 | s0
///  (1 row)
/// ```
pub fn render_table(schema: &Schema, batches: &[RecordBatch]) -> Result<String, ArrowError> {
    let options = FormatOptions::default().with_null("NULL");
    let headers: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str().to_string())
        .collect();
    let right_align: Vec<bool> = schema
        .fields()
        .iter()
        .map(|f| f.data_type().is_numeric())
        .collect();

    let mut rows: Vec<Vec<String>> = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            let cells = formatters
                .iter()
                .map(|f| f.value(row).try_to_string())
                .collect::<Result<Vec<_>, _>>()?;
            rows.push(cells);
        }
    }

    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }

    let mut out = String::new();
    push_row(&mut out, &headers, &widths, &right_align);
    out.push_str(
        &widths
            .iter()
            .map(|w| "-".repeat(w + 2))
            .collect::<Vec<_>>()
            .join("+"),
    );
    out.push('\n');
    for row in &rows {
        push_row(&mut out, row, &widths, &right_align);
    }
    let n = rows.len();
    out.push_str(&format!("({n} row{})", if n == 1 { "" } else { "s" }));
    Ok(out)
}

fn push_row(out: &mut String, cells: &[String], widths: &[usize], right_align: &[bool]) {
    let line = cells
        .iter()
        .zip(widths)
        .zip(right_align)
        .map(|((cell, &width), &right)| {
            let pad = " ".repeat(width - cell.chars().count());
            if right {
                format!(" {pad}{cell} ")
            } else {
                format!(" {cell}{pad} ")
            }
        })
        .collect::<Vec<_>>()
        .join("|");
    out.push_str(line.trim_end());
    out.push('\n');
}
