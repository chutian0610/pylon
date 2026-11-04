//! AST → LogicalPlan 绑定（analysis）：列名解析、类型推断（含隐式转换物化为
//! Cast）、聚合提取与 GROUP BY 合法性、BETWEEN 脱糖。所有用户可见错误带 Span。

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use pylon_expr::{Expr, IntervalUnit, Scalar, parse_interval, parse_number, ymd_to_date32};
use pylon_sql::{AggFunc, Expr as Ast, Ident, Query, SelectItem, Span, UnaryOp};

use crate::logical::{AggExpr, LogicalPlan, NamedExpr, schema_of, schema_of_aggregate};
use crate::{PlanError, resolve_column};

/// 查询 lowering 入口（语句分派在 [`crate::plan_inner`]，SHOW/DESCRIBE 不走到这）。
pub(crate) fn lower(query: &Query, schema: SchemaRef) -> Result<LogicalPlan, PlanError> {
    lower_query(query, schema)
}

fn lower_query(q: &Query, table_schema: SchemaRef) -> Result<LogicalPlan, PlanError> {
    let table = &q.from;
    let scan = LogicalPlan::Scan {
        table: table.name.clone(),
        columns: (0..table_schema.fields().len()).collect(),
        schema: table_schema.clone(),
        table_schema,
        filters: Vec::new(),
    };

    // WHERE：聚合不许进谓词（那是 HAVING，M1 明确不做）
    let base = if let Some(ast) = &q.selection {
        if let Some(span) = first_aggregate_span(ast) {
            return Err(PlanError::Bind {
                span,
                message: "aggregate functions are not allowed in WHERE".into(),
            });
        }
        let predicate = bind_expr(ast, &scan.schema())?;
        LogicalPlan::Filter {
            predicate,
            input: Arc::new(scan),
        }
    } else {
        scan
    };

    // 聚合模式：SELECT 里出现聚合，或有 GROUP BY
    let agg_mode = !q.group_by.is_empty() || q.items.iter().any(item_has_aggregate);
    let (agg_keys, agg_calls, agg_asts) = if agg_mode {
        collect_aggregation(q, &base.schema())?
    } else {
        (Vec::new(), Vec::new(), Vec::new())
    };

    let input = if agg_mode {
        let schema = schema_of_aggregate(&agg_keys, &agg_calls);
        let span = q
            .group_by
            .first()
            .map(|k| k.span())
            .or_else(|| first_aggregate_span_of_items(&q.items))
            .unwrap_or_else(|| q.span());
        LogicalPlan::Aggregate {
            keys: agg_keys.clone(),
            aggs: agg_calls.clone(),
            schema,
            span,
            distinct: false,
            input: Arc::new(base),
        }
    } else {
        base
    };

    // 投影
    let mut exprs: Vec<NamedExpr> = Vec::new();
    for item in &q.items {
        match item {
            SelectItem::Wildcard(_) => {
                if agg_mode {
                    return Err(PlanError::Bind {
                        span: item.span(),
                        message: "SELECT * cannot be combined with GROUP BY or aggregates".into(),
                    });
                }
                for (i, field) in input.schema().fields().iter().enumerate() {
                    exprs.push(NamedExpr {
                        expr: Expr::column(i, field.name(), field.data_type().clone()),
                        name: field.name().clone(),
                    });
                }
            }
            SelectItem::Expr(ast) => {
                if agg_mode {
                    exprs.push(bind_in_agg_context(
                        ast,
                        &agg_keys,
                        &agg_calls,
                        &agg_asts,
                        &q.group_by,
                    )?);
                } else {
                    let bound = bind_expr(ast, &input.schema())?;
                    let name = output_name(ast, &bound);
                    exprs.push(NamedExpr { expr: bound, name });
                }
            }
        }
    }
    let project = LogicalPlan::Project {
        schema: schema_of(&exprs),
        exprs,
        input: Arc::new(input),
    };

    // DISTINCT：以全输出列 GROUP BY 实现（父 spec 决策）
    let distinct = match q.distinct {
        Some(span) => {
            let outputs: Vec<NamedExpr> = project
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| NamedExpr {
                    expr: Expr::column(i, f.name(), f.data_type().clone()),
                    name: f.name().clone(),
                })
                .collect();
            LogicalPlan::Aggregate {
                schema: schema_of(&outputs),
                keys: outputs,
                aggs: Vec::new(),
                span,
                distinct: true,
                input: Arc::new(project),
            }
        }
        None => project,
    };

    // ORDER BY：按投影输出解析（M1 限制：排序键须可由输出列表达）
    let sorted = if q.order_by.is_empty() {
        distinct
    } else {
        let out_schema = distinct.schema();
        let mut keys = Vec::new();
        for key in &q.order_by {
            let bound = bind_post_projection(&key.expr, &out_schema)?;
            keys.push(crate::logical::SortKey {
                expr: bound,
                direction: key.direction,
            });
        }
        let span = q
            .order_by
            .first()
            .map(|k| k.expr.span())
            .unwrap_or_else(|| q.span());
        LogicalPlan::Sort {
            keys,
            span,
            input: Arc::new(distinct),
        }
    };

    Ok(match &q.limit {
        Some(l) => LogicalPlan::Limit {
            n: l.n,
            input: Arc::new(sorted),
        },
        None => sorted,
    })
}

// ---- AST 表达式绑定 ----

/// 把 AST 表达式绑定到 `schema`：列名解析、字面量定型、类型裁决（错误带 Span）。
/// 聚合调用仅在投影后上下文有意义，这里按输出列名解析（见 [`bind_post_projection`]）。
fn bind_expr(ast: &Ast, schema: &SchemaRef) -> Result<Expr, PlanError> {
    bind_expr_with(ast, schema, false)
}

/// 投影后绑定：ORDER BY 等场景，聚合调用按其输出列名（如 `sum(b)`）解析。
fn bind_post_projection(ast: &Ast, schema: &SchemaRef) -> Result<Expr, PlanError> {
    bind_expr_with(ast, schema, true)
}

fn bind_expr_with(ast: &Ast, schema: &SchemaRef, post_projection: bool) -> Result<Expr, PlanError> {
    use pylon_expr::BinaryOp as BO;
    match ast {
        Ast::Column(ident) => {
            let idx = resolve_column(schema, ident)?;
            let field = &schema.fields()[idx];
            Ok(Expr::column(idx, field.name(), field.data_type().clone()))
        }
        Ast::Number { value, span } => match parse_number(value) {
            Ok(v) => Ok(Expr::literal(v)),
            Err(message) => Err(PlanError::Bind {
                span: *span,
                message,
            }),
        },
        Ast::String { value, .. } => Ok(Expr::literal(Scalar::Utf8(value.clone()))),
        Ast::Bool { value, .. } => Ok(Expr::literal(Scalar::Boolean(*value))),
        Ast::Null(_) => Ok(Expr::literal(Scalar::Null(DataType::Null))),
        Ast::Date { value, span } => match ymd_to_date32(value) {
            Some(days) => Ok(Expr::literal(Scalar::Date32(days))),
            None => Err(PlanError::Bind {
                span: *span,
                message: format!("invalid DATE literal '{value}'"),
            }),
        },
        Ast::Interval { value, unit, span } => {
            let unit = match unit {
                pylon_sql::IntervalField::Year => IntervalUnit::Year,
                pylon_sql::IntervalField::Month => IntervalUnit::Month,
                pylon_sql::IntervalField::Day => IntervalUnit::Day,
                pylon_sql::IntervalField::Hour => IntervalUnit::Hour,
                pylon_sql::IntervalField::Minute => IntervalUnit::Minute,
                pylon_sql::IntervalField::Second => IntervalUnit::Second,
            };
            match parse_interval(value, unit) {
                Ok(v) => Ok(Expr::literal(v)),
                Err(message) => Err(PlanError::Bind {
                    span: *span,
                    message,
                }),
            }
        }
        Ast::Unary { op, expr, span } => {
            let inner = bind_expr_with(expr, schema, post_projection)?;
            match op {
                UnaryOp::Not => Expr::logical_not(inner).map_err(|m| PlanError::Bind {
                    span: *span,
                    message: m,
                }),
                UnaryOp::Neg => Expr::negate(inner).map_err(|m| PlanError::Bind {
                    span: *span,
                    message: m,
                }),
                UnaryOp::Pos => Ok(inner),
            }
        }
        Ast::Binary {
            left,
            op,
            right,
            span,
        } => {
            let l = bind_expr_with(left, schema, post_projection)?;
            let r = bind_expr_with(right, schema, post_projection)?;
            let op = match op {
                pylon_sql::BinaryOp::Add => BO::Add,
                pylon_sql::BinaryOp::Sub => BO::Sub,
                pylon_sql::BinaryOp::Mul => BO::Mul,
                pylon_sql::BinaryOp::Div => BO::Div,
                pylon_sql::BinaryOp::Eq => BO::Eq,
                pylon_sql::BinaryOp::NotEq => BO::NotEq,
                pylon_sql::BinaryOp::Lt => BO::Lt,
                pylon_sql::BinaryOp::LtEq => BO::LtEq,
                pylon_sql::BinaryOp::Gt => BO::Gt,
                pylon_sql::BinaryOp::GtEq => BO::GtEq,
                pylon_sql::BinaryOp::And => BO::And,
                pylon_sql::BinaryOp::Or => BO::Or,
            };
            Expr::binary(op, l, r).map_err(|m| PlanError::Bind {
                span: *span,
                message: m,
            })
        }
        Ast::IsNull { expr, negated, .. } => Ok(Expr::is_null(
            bind_expr_with(expr, schema, post_projection)?,
            *negated,
        )),
        // BETWEEN 脱糖：x BETWEEN lo AND hi ≡ x >= lo AND x <= hi（三值逻辑等价）
        Ast::Between {
            expr,
            low,
            high,
            negated,
            span,
        } => desugar_between(
            *negated,
            *span,
            || bind_expr_with(expr, schema, post_projection),
            || bind_expr_with(low, schema, post_projection),
            || bind_expr_with(high, schema, post_projection),
        ),
        Ast::Aggregate { .. } | Ast::CountStar(_) => {
            if post_projection {
                let name = ast_display_for_projection(ast, schema)?;
                let ident = Ident {
                    name: name.clone(),
                    span: ast.span(),
                };
                let idx = resolve_column(schema, &ident).map_err(|_| PlanError::Bind {
                    span: ast.span(),
                    message: format!("'{name}' is not an output column of the SELECT list"),
                })?;
                let field = &schema.fields()[idx];
                Ok(Expr::column(idx, field.name(), field.data_type().clone()))
            } else {
                Err(PlanError::Bind {
                    span: ast.span(),
                    message: "aggregate functions are only allowed in the SELECT list".into(),
                })
            }
        }
    }
}

/// 聚合调用的输出列名（与 [`collect_aggregation`] 的命名一致）。
fn ast_display_for_projection(ast: &Ast, schema: &SchemaRef) -> Result<String, PlanError> {
    match ast {
        Ast::CountStar(_) => Ok("count(*)".into()),
        Ast::Aggregate { func, arg, .. } => {
            let bound = bind_expr_with(arg, schema, true)?;
            Ok(format!("{}({bound})", func_name(*func)))
        }
        other => Err(PlanError::Internal(format!(
            "not an aggregate call: {other:?}"
        ))),
    }
}

fn func_name(func: AggFunc) -> &'static str {
    match func {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
        AggFunc::Avg => "avg",
    }
}

// ---- 聚合提取 ----

/// 聚合模式下的分组键与聚合调用（顺序 = SELECT 里首次出现的先序序）。
/// 聚合提取结果：分组键、聚合调用、对应的 AST 节点。`ast` 与 `aggs` 按序平行，
/// 供 SELECT 项绑定阶段做结构匹配。
type Aggregation = (Vec<NamedExpr>, Vec<AggExpr>, Vec<Ast>);

/// 返回 (分组键, 聚合调用, 对应的 AST 节点)。
fn collect_aggregation(q: &Query, input_schema: &SchemaRef) -> Result<Aggregation, PlanError> {
    let mut keys = Vec::new();
    for ast in &q.group_by {
        if let Some(span) = first_aggregate_span(ast) {
            return Err(PlanError::Bind {
                span,
                message: "aggregate functions are not allowed in GROUP BY".into(),
            });
        }
        let bound = bind_expr(ast, input_schema)?;
        let name = output_name(ast, &bound);
        keys.push(NamedExpr { expr: bound, name });
    }

    let mut aggs = Vec::new();
    let mut asts: Vec<Ast> = Vec::new();
    for item in &q.items {
        let ast = match item {
            SelectItem::Expr(ast) => ast,
            SelectItem::Wildcard(_) => continue, // agg+wildcard 的错误在投影阶段报
        };
        collect_agg_calls(ast, input_schema, &mut aggs, &mut asts)?;
    }
    Ok((keys, aggs, asts))
}

fn collect_agg_calls(
    ast: &Ast,
    schema: &SchemaRef,
    aggs: &mut Vec<AggExpr>,
    asts: &mut Vec<Ast>,
) -> Result<(), PlanError> {
    match ast {
        Ast::CountStar(_) => {
            if !aggs
                .iter()
                .any(|a| a.func == AggFunc::Count && a.arg.is_none())
            {
                aggs.push(AggExpr {
                    func: AggFunc::Count,
                    arg: None,
                    name: "count(*)".into(),
                    data_type: DataType::Int64,
                });
                asts.push(ast.clone());
            }
        }
        Ast::Aggregate { func, arg, span } => {
            let bound = bind_expr(arg, schema)?;
            let name = format!("{}({bound})", func_name(*func));
            if !aggs.iter().any(|a| a.name == name) {
                let data_type =
                    aggregate_type(*func, Some(&bound)).map_err(|m| PlanError::Bind {
                        span: *span,
                        message: m,
                    })?;
                aggs.push(AggExpr {
                    func: *func,
                    arg: Some(bound),
                    name,
                    data_type,
                });
                asts.push(ast.clone());
            }
        }
        Ast::Unary { expr, .. } => collect_agg_calls(expr, schema, aggs, asts)?,
        Ast::Binary { left, right, .. } => {
            collect_agg_calls(left, schema, aggs, asts)?;
            collect_agg_calls(right, schema, aggs, asts)?;
        }
        Ast::IsNull { expr, .. } => collect_agg_calls(expr, schema, aggs, asts)?,
        Ast::Between {
            expr, low, high, ..
        } => {
            collect_agg_calls(expr, schema, aggs, asts)?;
            collect_agg_calls(low, schema, aggs, asts)?;
            collect_agg_calls(high, schema, aggs, asts)?;
        }
        _ => {}
    }
    Ok(())
}

/// 聚合模式里绑定 SELECT 项：与分组键结构相同的子树解析为键列，聚合调用解析为
/// 聚合输出列，其余列引用报"须出现在 GROUP BY"。
fn bind_in_agg_context(
    ast: &Ast,
    keys: &[NamedExpr],
    aggs: &[AggExpr],
    agg_asts: &[Ast],
    key_asts: &[Ast],
) -> Result<NamedExpr, PlanError> {
    let bound = bind_agg_subtree(ast, keys, aggs, agg_asts, key_asts)?;
    let name = output_name(ast, &bound);
    Ok(NamedExpr { expr: bound, name })
}

fn bind_agg_subtree(
    ast: &Ast,
    keys: &[NamedExpr],
    aggs: &[AggExpr],
    agg_asts: &[Ast],
    key_asts: &[Ast],
) -> Result<Expr, PlanError> {
    // 结构上等于某个分组键 → 键输出列
    if let Some(i) = key_asts.iter().position(|k| ast_same(k, ast)) {
        let key = &keys[i];
        return Ok(Expr::column(i, &key.name, key.expr.data_type()));
    }
    match ast {
        // 聚合调用 → 对应聚合输出列（收集阶段记录的 AST 按结构匹配）
        Ast::CountStar(_) | Ast::Aggregate { .. } => {
            let j = agg_asts
                .iter()
                .position(|a| ast_same(a, ast))
                .ok_or_else(|| PlanError::Internal("aggregate call not collected".into()))?;
            let idx = keys.len() + j;
            let agg = &aggs[j];
            Ok(Expr::column(idx, &agg.name, agg.data_type.clone()))
        }
        Ast::Column(ident) => Err(PlanError::Bind {
            span: ident.span,
            message: format!(
                "column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                ident.name
            ),
        }),
        other => bind_aggregates_only(other, keys, aggs, agg_asts, key_asts),
    }
}

fn bind_aggregates_only(
    ast: &Ast,
    keys: &[NamedExpr],
    aggs: &[AggExpr],
    agg_asts: &[Ast],
    key_asts: &[Ast],
) -> Result<Expr, PlanError> {
    match ast {
        Ast::Unary { op, expr, span } => {
            let inner = bind_agg_subtree(expr, keys, aggs, agg_asts, key_asts)?;
            match op {
                UnaryOp::Not => Expr::logical_not(inner).map_err(|m| PlanError::Bind {
                    span: *span,
                    message: m,
                }),
                UnaryOp::Neg => Expr::negate(inner).map_err(|m| PlanError::Bind {
                    span: *span,
                    message: m,
                }),
                UnaryOp::Pos => Ok(inner),
            }
        }
        Ast::Binary {
            left,
            op,
            right,
            span,
        } => {
            let l = bind_agg_subtree(left, keys, aggs, agg_asts, key_asts)?;
            let r = bind_agg_subtree(right, keys, aggs, agg_asts, key_asts)?;
            let op = map_op(*op);
            Expr::binary(op, l, r).map_err(|m| PlanError::Bind {
                span: *span,
                message: m,
            })
        }
        Ast::IsNull { expr, negated, .. } => Ok(Expr::is_null(
            bind_agg_subtree(expr, keys, aggs, agg_asts, key_asts)?,
            *negated,
        )),
        Ast::Between {
            expr,
            low,
            high,
            negated,
            span,
        } => desugar_between(
            *negated,
            *span,
            || bind_agg_subtree(expr, keys, aggs, agg_asts, key_asts),
            || bind_agg_subtree(low, keys, aggs, agg_asts, key_asts),
            || bind_agg_subtree(high, keys, aggs, agg_asts, key_asts),
        ),
        Ast::Column { .. } => unreachable!("由上层 bind_agg_subtree 处理"),
        // 字面量原样（date/interval/number 等无需 schema 上下文）
        ast if is_literal_ast(ast) => literal_ast(ast),
        other => Err(PlanError::Bind {
            span: other.span(),
            message: "expression is not valid in an aggregated SELECT list".into(),
        }),
    }
}

fn is_literal_ast(ast: &Ast) -> bool {
    matches!(
        ast,
        Ast::Number { .. }
            | Ast::String { .. }
            | Ast::Bool { .. }
            | Ast::Null(_)
            | Ast::Date { .. }
            | Ast::Interval { .. }
    )
}

fn literal_ast(ast: &Ast) -> Result<Expr, PlanError> {
    let schema = Arc::new(Schema::empty());
    bind_expr(ast, &schema)
}

fn map_op(op: pylon_sql::BinaryOp) -> pylon_expr::BinaryOp {
    use pylon_expr::BinaryOp as BO;
    match op {
        pylon_sql::BinaryOp::Add => BO::Add,
        pylon_sql::BinaryOp::Sub => BO::Sub,
        pylon_sql::BinaryOp::Mul => BO::Mul,
        pylon_sql::BinaryOp::Div => BO::Div,
        pylon_sql::BinaryOp::Eq => BO::Eq,
        pylon_sql::BinaryOp::NotEq => BO::NotEq,
        pylon_sql::BinaryOp::Lt => BO::Lt,
        pylon_sql::BinaryOp::LtEq => BO::LtEq,
        pylon_sql::BinaryOp::Gt => BO::Gt,
        pylon_sql::BinaryOp::GtEq => BO::GtEq,
        pylon_sql::BinaryOp::And => BO::And,
        pylon_sql::BinaryOp::Or => BO::Or,
    }
}

/// 聚合函数的输出类型（Trino 族规则，T6 硬化）。
fn aggregate_type(func: AggFunc, arg: Option<&Expr>) -> Result<DataType, String> {
    let arg_type = arg.map(|a| a.data_type());
    match func {
        AggFunc::Count => Ok(DataType::Int64),
        AggFunc::Sum => match arg_type {
            Some(DataType::Int64) => Ok(DataType::Int64),
            Some(DataType::Float64) => Ok(DataType::Float64),
            Some(DataType::Decimal128(_, s)) => Ok(DataType::Decimal128(38, s)),
            Some(t) => Err(format!("SUM expects a numeric argument, got {t}")),
            None => Err("SUM requires an argument".into()),
        },
        AggFunc::Min | AggFunc::Max => match arg_type {
            Some(t) => Ok(t),
            None => Err("MIN/MAX require an argument".into()),
        },
        AggFunc::Avg => match arg_type {
            Some(DataType::Int64) | Some(DataType::Float64) => Ok(DataType::Float64),
            Some(DataType::Decimal128(_, s)) => Ok(DataType::Decimal128(38, (s + 4).min(38))),
            Some(t) => Err(format!("AVG expects a numeric argument, got {t}")),
            None => Err("AVG requires an argument".into()),
        },
    }
}

/// 输出列名：列引用用列名，其余用绑定后表达式的 SQL 形态（Trino 同款）。
fn output_name(ast: &Ast, bound: &Expr) -> String {
    match ast {
        Ast::Column(ident) => ident.name.clone(),
        _ => bound.to_string(),
    }
}

/// BETWEEN 脱糖的共用实现：三个绑定闭包产出左/下/上界，类型错误统一指向整段。
fn desugar_between(
    negated: bool,
    span: Span,
    x: impl FnOnce() -> Result<Expr, PlanError>,
    lo: impl FnOnce() -> Result<Expr, PlanError>,
    hi: impl FnOnce() -> Result<Expr, PlanError>,
) -> Result<Expr, PlanError> {
    use pylon_expr::BinaryOp as BO;
    let x = x()?;
    let ge = Expr::binary(BO::GtEq, x.clone(), lo()?)
        .map_err(|m| PlanError::Bind { span, message: m })?;
    let le = Expr::binary(BO::LtEq, x, hi()?).map_err(|m| PlanError::Bind { span, message: m })?;
    let both = Expr::binary(BO::And, ge, le).map_err(|m| PlanError::Bind { span, message: m })?;
    if negated {
        Expr::logical_not(both).map_err(|m| PlanError::Bind { span, message: m })
    } else {
        Ok(both)
    }
}

/// span 无关的 AST 结构相等（GROUP BY 键/聚合调用与 SELECT 项对齐用）：
/// pylon_sql::Expr 的 PartialEq 含 span，同名列在不同位置并不相等。
fn ast_same(a: &Ast, b: &Ast) -> bool {
    use Ast::*;
    match (a, b) {
        (Column(x), Column(y)) => x.name == y.name,
        (Number { value: x, .. }, Number { value: y, .. }) => x == y,
        (String { value: x, .. }, String { value: y, .. }) => x == y,
        (Bool { value: x, .. }, Bool { value: y, .. }) => x == y,
        (Null(_), Null(_)) => true,
        (Date { value: x, .. }, Date { value: y, .. }) => x == y,
        (
            Interval {
                value: vx,
                unit: ux,
                ..
            },
            Interval {
                value: vy,
                unit: uy,
                ..
            },
        ) => vx == vy && ux == uy,
        (
            Unary {
                op: x, expr: xe, ..
            },
            Unary {
                op: y, expr: ye, ..
            },
        ) => x == y && ast_same(xe, ye),
        (
            Binary {
                op: x,
                left: xl,
                right: xr,
                ..
            },
            Binary {
                op: y,
                left: yl,
                right: yr,
                ..
            },
        ) => x == y && ast_same(xl, yl) && ast_same(xr, yr),
        (
            IsNull {
                expr: xe,
                negated: xn,
                ..
            },
            IsNull {
                expr: ye,
                negated: yn,
                ..
            },
        ) => xn == yn && ast_same(xe, ye),
        (
            Between {
                expr: xe,
                low: xl,
                high: xh,
                negated: xn,
                ..
            },
            Between {
                expr: ye,
                low: yl,
                high: yh,
                negated: yn,
                ..
            },
        ) => xn == yn && ast_same(xe, ye) && ast_same(xl, yl) && ast_same(xh, yh),
        (
            Aggregate {
                func: x, arg: xa, ..
            },
            Aggregate {
                func: y, arg: ya, ..
            },
        ) => x == y && ast_same(xa, ya),
        (CountStar(_), CountStar(_)) => true,
        _ => false,
    }
}

// ---- 聚合探测 ----

fn item_has_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expr(ast) => first_aggregate_span(ast).is_some(),
        SelectItem::Wildcard(_) => false,
    }
}

fn first_aggregate_span_of_items(items: &[SelectItem]) -> Option<Span> {
    items.iter().filter_map(item_has_aggregate_span).next()
}

fn item_has_aggregate_span(item: &SelectItem) -> Option<Span> {
    match item {
        SelectItem::Expr(ast) => first_aggregate_span(ast),
        SelectItem::Wildcard(_) => None,
    }
}

/// 先序遍历找第一个聚合调用的位置。
fn first_aggregate_span(ast: &Ast) -> Option<Span> {
    match ast {
        Ast::Aggregate { span, .. } | Ast::CountStar(span) => Some(*span),
        Ast::Unary { expr, .. } | Ast::IsNull { expr, .. } => first_aggregate_span(expr),
        Ast::Binary { left, right, .. } => {
            first_aggregate_span(left).or_else(|| first_aggregate_span(right))
        }
        Ast::Between {
            expr, low, high, ..
        } => first_aggregate_span(expr)
            .or_else(|| first_aggregate_span(low))
            .or_else(|| first_aggregate_span(high)),
        _ => None,
    }
}
