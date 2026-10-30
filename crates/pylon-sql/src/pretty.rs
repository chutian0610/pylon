//! AST 的确定性文本渲染：`--parse` 演示输出与 golden 快照测试共用。
//!
//! 形态为每节点一行的 S 表达式，缩进 2 空格。只反映结构，不含 span；span 的正确性
//! 由 tests/parse.rs 的定点断言钉死。

use crate::ast::{
    AggFunc, BinaryOp, Direction, Expr, IntervalField, OrderKey, Query, SelectItem, Statement,
    UnaryOp,
};

pub fn render(stmt: &Statement) -> String {
    let mut out = String::new();
    match stmt {
        Statement::Query(q) => {
            out.push_str("(query");
            render_query_into(q, 1, &mut out);
            out.push(')');
        }
        Statement::ShowTables(_) => out.push_str("(show-tables)"),
        Statement::Describe { table } => {
            out.push_str("(describe ");
            out.push_str(&ident_text(&table.name));
            out.push(')');
        }
    }
    out
}

/// 普通标识符原样输出；含空格/特殊字符/引号的按 SQL 引号形态转义，保输出可读可辨。
fn ident_text(name: &str) -> String {
    let plain = !name.is_empty()
        && name
            .chars()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphanumeric() || c == '_' && i > 0);
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// 字符串类字面量（string / date / interval 的值）按 SQL 单引号形态输出，`'` 加倍。
fn quoted_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn render_query_into(q: &Query, depth: usize, out: &mut String) {
    let ind = indent(depth);
    if q.distinct.is_some() {
        out.push_str(&format!("\n{ind}(select distinct"));
    } else {
        out.push_str(&format!("\n{ind}(select"));
    }
    // 投影列表由语法保证非空（separated_list1）
    for item in &q.items {
        match item {
            SelectItem::Wildcard(_) => out.push_str(&format!("\n{}*", indent(depth + 1))),
            SelectItem::Expr(e) => render_expr_into(e, depth + 1, out),
        }
    }
    out.push_str(&format!("\n{ind})"));
    out.push_str(&format!("\n{ind}(from {})", ident_text(&q.from.name)));
    if let Some(e) = &q.selection {
        out.push_str(&format!("\n{ind}(where"));
        render_expr_into(e, depth + 1, out);
        out.push_str(&format!("\n{ind})"));
    }
    if !q.group_by.is_empty() {
        out.push_str(&format!("\n{ind}(group-by"));
        for e in &q.group_by {
            render_expr_into(e, depth + 1, out);
        }
        out.push_str(&format!("\n{ind})"));
    }
    if !q.order_by.is_empty() {
        out.push_str(&format!("\n{ind}(order-by"));
        for key in &q.order_by {
            render_order_key_into(key, depth + 1, out);
        }
        out.push_str(&format!("\n{ind})"));
    }
    if let Some(l) = &q.limit {
        out.push_str(&format!("\n{ind}(limit {})", l.n));
    }
}

fn render_order_key_into(key: &OrderKey, depth: usize, out: &mut String) {
    let head = match key.direction {
        Direction::Asc => "asc",
        Direction::Desc => "desc",
    };
    out.push_str(&format!("\n{}({head}", indent(depth)));
    render_expr_into(&key.expr, depth + 1, out);
    out.push_str(&format!("\n{})", indent(depth)));
}

fn render_expr_into(e: &Expr, depth: usize, out: &mut String) {
    let ind = indent(depth);
    match e {
        Expr::Column(id) => out.push_str(&format!("\n{ind}(column {})", ident_text(&id.name))),
        Expr::Number { value, .. } => out.push_str(&format!("\n{ind}(number {value})")),
        Expr::String { value, .. } => {
            out.push_str(&format!("\n{ind}(string {})", quoted_text(value)))
        }
        Expr::Bool { value, .. } => out.push_str(&format!("\n{ind}(boolean {value})")),
        Expr::Null(_) => out.push_str(&format!("\n{ind}(null)")),
        Expr::Date { value, .. } => out.push_str(&format!("\n{ind}(date {})", quoted_text(value))),
        Expr::Interval { value, unit, .. } => {
            out.push_str(&format!(
                "\n{ind}(interval {} {})",
                quoted_text(value),
                interval_field_str(*unit)
            ));
        }
        Expr::Unary { op, expr, .. } => {
            let head = match op {
                UnaryOp::Not => "not",
                UnaryOp::Neg => "neg",
                UnaryOp::Pos => "pos",
            };
            out.push_str(&format!("\n{ind}({head}"));
            render_expr_into(expr, depth + 1, out);
            out.push_str(&format!("\n{ind})"));
        }
        Expr::Binary {
            left, op, right, ..
        } => {
            out.push_str(&format!("\n{ind}({}", binary_op_str(*op)));
            render_expr_into(left, depth + 1, out);
            render_expr_into(right, depth + 1, out);
            out.push_str(&format!("\n{ind})"));
        }
        Expr::IsNull { expr, negated, .. } => {
            let head = if *negated { "is-not-null" } else { "is-null" };
            out.push_str(&format!("\n{ind}({head}"));
            render_expr_into(expr, depth + 1, out);
            out.push_str(&format!("\n{ind})"));
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
            ..
        } => {
            let head = if *negated { "not-between" } else { "between" };
            out.push_str(&format!("\n{ind}({head}"));
            render_expr_into(expr, depth + 1, out);
            render_expr_into(low, depth + 1, out);
            render_expr_into(high, depth + 1, out);
            out.push_str(&format!("\n{ind})"));
        }
        Expr::Aggregate { func, arg, .. } => {
            out.push_str(&format!("\n{ind}({}", agg_func_str(*func)));
            render_expr_into(arg, depth + 1, out);
            out.push_str(&format!("\n{ind})"));
        }
        Expr::CountStar(_) => out.push_str(&format!("\n{ind}(count *)")),
    }
}

fn binary_op_str(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::Eq => "eq",
        BinaryOp::NotEq => "neq",
        BinaryOp::Lt => "lt",
        BinaryOp::LtEq => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::GtEq => "gte",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
    }
}

fn agg_func_str(f: AggFunc) -> &'static str {
    match f {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::Avg => "avg",
    }
}

fn interval_field_str(f: IntervalField) -> &'static str {
    match f {
        IntervalField::Year => "year",
        IntervalField::Month => "month",
        IntervalField::Day => "day",
        IntervalField::Hour => "hour",
        IntervalField::Minute => "minute",
        IntervalField::Second => "second",
    }
}

fn indent(depth: usize) -> String {
    "  ".repeat(depth)
}
