//! 计划渲染：`--explain` 与计划快照测试共用。逻辑计划每节点一行、缩进 2 空格；
//! 物理计划按 Pipeline 顺序列出。

use crate::logical::LogicalPlan;
use crate::{Operator, Pipeline};

pub fn logical(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    render_logical(plan, 0, &mut out);
    out
}

fn render_logical(plan: &LogicalPlan, depth: usize, out: &mut String) {
    let ind = "  ".repeat(depth);
    match plan {
        LogicalPlan::Scan {
            table,
            schema,
            table_schema,
            filters,
            ..
        } => {
            let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            if filters.is_empty() {
                out.push_str(&format!("{ind}Scan {table} [{}]\n", cols.join(", ")));
            } else {
                let preds: Vec<String> = filters
                    .iter()
                    .map(|p| {
                        // filters 的列索引是表 schema 索引（下推契约）
                        let name = table_schema
                            .fields()
                            .get(p.column)
                            .map(|f| f.name().clone())
                            .unwrap_or_else(|| p.column.to_string());
                        format!("{name} {} {}", p.op, p.value)
                    })
                    .collect();
                out.push_str(&format!(
                    "{ind}Scan {table} [{}] pushed=[{}]\n",
                    cols.join(", "),
                    preds.join(" AND ")
                ));
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            out.push_str(&format!("{ind}Filter ({predicate})\n"));
            render_logical(input, depth + 1, out);
        }
        LogicalPlan::Project { exprs, input, .. } => {
            let items: Vec<String> = exprs
                .iter()
                .map(|e| {
                    if e.expr.to_string() == e.name {
                        e.name.clone()
                    } else {
                        format!("{} AS {}", e.expr, e.name)
                    }
                })
                .collect();
            out.push_str(&format!("{ind}Project [{}]\n", items.join(", ")));
            render_logical(input, depth + 1, out);
        }
        LogicalPlan::Aggregate {
            keys, aggs, input, ..
        } => {
            let k: Vec<&str> = keys.iter().map(|e| e.name.as_str()).collect();
            let a: Vec<&str> = aggs.iter().map(|e| e.name.as_str()).collect();
            out.push_str(&format!(
                "{ind}Aggregate keys=[{}] aggs=[{}]\n",
                k.join(", "),
                a.join(", ")
            ));
            render_logical(input, depth + 1, out);
        }
        LogicalPlan::Sort { keys, input, .. } => {
            let k: Vec<String> = keys
                .iter()
                .map(|key| {
                    format!(
                        "{} {}",
                        key.expr,
                        match key.direction {
                            pylon_sql::Direction::Asc => "ASC",
                            pylon_sql::Direction::Desc => "DESC",
                        }
                    )
                })
                .collect();
            out.push_str(&format!("{ind}Sort [{}]\n", k.join(", ")));
            render_logical(input, depth + 1, out);
        }
        LogicalPlan::Limit { n, input } => {
            out.push_str(&format!("{ind}Limit {n}\n"));
            render_logical(input, depth + 1, out);
        }
    }
}

pub fn physical(pipeline: &Pipeline) -> String {
    let mut out = String::new();
    let cols: Vec<&str> = pipeline
        .scan_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    out.push_str(&format!(
        "Pipeline scan={} [{}] splits={}\n",
        pipeline.table,
        cols.join(", "),
        pipeline.scan.splits.len()
    ));
    for op in &pipeline.ops {
        match op {
            Operator::Filter { predicate } => {
                out.push_str(&format!("  Filter ({predicate})\n"));
            }
            Operator::Project { exprs, .. } => {
                let items: Vec<String> = exprs.iter().map(|e| e.to_string()).collect();
                out.push_str(&format!("  Project [{}]\n", items.join(", ")));
            }
        }
    }
    if let Some(n) = pipeline.limit {
        out.push_str(&format!("  Limit {n}\n"));
    }
    out
}
