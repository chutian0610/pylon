//! 优化规则（M1/T3 三条）：常量折叠、投影裁剪、谓词下推。全部是
//! `Arc<LogicalPlan>` 上的纯函数；单表树（无 JOIN）是它们可以简洁的前提。
//!
//! 顺序：折叠（date/interval 谓词化为可下推的简单比较）→ 裁剪（Scan 只读被引用
//! 的列）→ 下推（可下推的合取项进入 `Scan.filters` 提示，残差 Filter 保留行级
//! 正确性）。

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use pylon_expr::{BinaryOp, Expr, fold};
use pylon_table::{CmpOp, ScanPredicate};

use crate::logical::{LogicalPlan, NamedExpr};

/// 依次应用三条规则。
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    let plan = fold_constants(plan);
    let plan = prune_columns(plan);
    pushdown_predicates(plan)
}

// ---- 1. 常量折叠 ----

pub fn fold_constants(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { .. } => plan,
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate: fold(&predicate),
            input: Arc::new(fold_constants((*input).clone())),
        },
        LogicalPlan::Project {
            exprs,
            schema,
            input,
        } => LogicalPlan::Project {
            exprs: exprs
                .into_iter()
                .map(|e| NamedExpr {
                    expr: fold(&e.expr),
                    name: e.name,
                })
                .collect(),
            schema,
            input: Arc::new(fold_constants((*input).clone())),
        },
        LogicalPlan::Aggregate {
            keys,
            aggs,
            schema,
            span,
            distinct,
            input,
        } => LogicalPlan::Aggregate {
            keys: keys
                .into_iter()
                .map(|e| NamedExpr {
                    expr: fold(&e.expr),
                    name: e.name,
                })
                .collect(),
            aggs: aggs
                .into_iter()
                .map(|mut a| {
                    if let Some(arg) = &a.arg {
                        a.arg = Some(fold(arg));
                    }
                    a
                })
                .collect(),
            schema,
            span,
            distinct,
            input: Arc::new(fold_constants((*input).clone())),
        },
        LogicalPlan::Sort { keys, span, input } => LogicalPlan::Sort {
            keys: keys
                .into_iter()
                .map(|k| crate::logical::SortKey {
                    expr: fold(&k.expr),
                    direction: k.direction,
                })
                .collect(),
            span,
            input: Arc::new(fold_constants((*input).clone())),
        },
        LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
            n,
            input: Arc::new(fold_constants((*input).clone())),
        },
    }
}

// ---- 2. 投影裁剪 ----

/// 自顶向下计算 Scan 实际被引用的列，收窄 Scan schema 并把路径上的表达式索引
/// 重映射。M1 单表：全树只有一套 scan 相对索引（聚合输出/投影输出之上的表达式
/// 不引用 scan 列，天然无需重映射）。
pub fn prune_columns(plan: LogicalPlan) -> LogicalPlan {
    let mut scan_needed: Option<BTreeSet<usize>> = None;
    collect_scan_needed(&plan, None, &mut scan_needed);
    let Some(mut needed) = scan_needed else {
        return plan; // 树里没有 Scan（不该发生，防御）
    };
    // 空引用（如 SELECT 1 FROM t / COUNT(*)）至少保一列：避免向 TableFormat 申请
    // 零列扫描（M1 fixture 不支持空投影流）
    if needed.is_empty() {
        needed.insert(0);
    }

    // 旧索引 → 新索引（Scan 输出保持表序子集）
    remap_plan(&plan, &needed)
}

/// `required`：上层对该节点输出列的索引需求；`None` = 全部需要。结果写入
/// `scan_needed`（唯一 Scan 的需求集合）。
fn collect_scan_needed(
    plan: &LogicalPlan,
    required: Option<&BTreeSet<usize>>,
    scan_needed: &mut Option<BTreeSet<usize>>,
) {
    match plan {
        LogicalPlan::Scan { schema, .. } => {
            *scan_needed = Some(match required {
                Some(set) => set.clone(),
                None => (0..schema.fields().len()).collect(),
            });
        }
        LogicalPlan::Filter { predicate, input } => {
            let mut need = required.cloned().unwrap_or_default();
            need.extend(predicate.column_refs());
            collect_scan_needed(input, Some(&need), scan_needed);
        }
        LogicalPlan::Project { exprs, input, .. } => {
            // Project 重写 schema：输入需求 = 表达式引用（未被上层引用的输出列也
            // 保留——M1 不做输出列剔除，schema 稳定）
            let mut need = BTreeSet::new();
            for named in exprs {
                need.extend(named.expr.column_refs());
            }
            collect_scan_needed(input, Some(&need), scan_needed);
        }
        LogicalPlan::Aggregate {
            keys, aggs, input, ..
        } => {
            let mut need = BTreeSet::new();
            for k in keys {
                need.extend(k.expr.column_refs());
            }
            for a in aggs {
                if let Some(arg) = &a.arg {
                    need.extend(arg.column_refs());
                }
            }
            collect_scan_needed(input, Some(&need), scan_needed);
        }
        LogicalPlan::Sort { keys, input, .. } => {
            let mut need = required.cloned().unwrap_or_default();
            for k in keys {
                need.extend(k.expr.column_refs());
            }
            collect_scan_needed(input, Some(&need), scan_needed);
        }
        LogicalPlan::Limit { input, .. } => {
            collect_scan_needed(input, required, scan_needed);
        }
    }
}

fn remap_plan(plan: &LogicalPlan, needed: &BTreeSet<usize>) -> LogicalPlan {
    let map = old_to_new_of(needed);
    match plan {
        LogicalPlan::Scan {
            table,
            table_schema,
            ..
        } => {
            // filters 的列索引是表 schema 索引（见 pushdown），裁剪不改变它
            let fields: Vec<Field> = needed
                .iter()
                .map(|&i| table_schema.fields()[i].as_ref().clone())
                .collect();
            let columns: Vec<usize> = needed.iter().copied().collect();
            LogicalPlan::Scan {
                table: table.clone(),
                columns,
                schema: Arc::new(Schema::new(fields)),
                table_schema: table_schema.clone(),
                filters: plan_filters(plan).clone(),
            }
        }
        // Filter 只出现在 Scan 之上（WHERE），谓词必然引用 scan 列
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate: remap_expr(predicate, &map),
            input: Arc::new(remap_plan(input, needed)),
        },
        // Project 表达式仅在输入侧无聚合时才引用 scan 列（聚合之上的投影引用
        // 聚合输出列，禁止重映射）
        LogicalPlan::Project {
            exprs,
            schema,
            input,
        } => {
            let scan_relative = !subtree_has_aggregate(input);
            LogicalPlan::Project {
                exprs: exprs
                    .iter()
                    .map(|e| NamedExpr {
                        expr: maybe_remap(&e.expr, &map, scan_relative),
                        name: e.name.clone(),
                    })
                    .collect(),
                schema: schema.clone(),
                input: Arc::new(remap_plan(input, needed)),
            }
        }
        // GROUP BY 聚合的键/参数引用 scan 列；DISTINCT 的键引用其输入（Project
        // 输出），不重映射
        LogicalPlan::Aggregate {
            keys,
            aggs,
            schema,
            span,
            distinct,
            input,
        } => {
            let scan_relative = !*distinct;
            LogicalPlan::Aggregate {
                keys: keys
                    .iter()
                    .map(|k| NamedExpr {
                        expr: maybe_remap(&k.expr, &map, scan_relative),
                        name: k.name.clone(),
                    })
                    .collect(),
                aggs: aggs
                    .iter()
                    .map(|a| {
                        let mut a = a.clone();
                        if let Some(arg) = &a.arg {
                            a.arg = Some(maybe_remap(arg, &map, scan_relative));
                        }
                        a
                    })
                    .collect(),
                schema: schema.clone(),
                span: *span,
                distinct: *distinct,
                input: Arc::new(remap_plan(input, needed)),
            }
        }
        // Sort 键永远引用 Project 输出（M1 里 Sort 总在投影之上），不重映射
        LogicalPlan::Sort { keys, span, input } => LogicalPlan::Sort {
            keys: keys.clone(),
            span: *span,
            input: Arc::new(remap_plan(input, needed)),
        },
        LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
            n: *n,
            input: Arc::new(remap_plan(input, needed)),
        },
    }
}

fn plan_filters(plan: &LogicalPlan) -> &Vec<ScanPredicate> {
    match plan {
        LogicalPlan::Scan { filters, .. } => filters,
        _ => unreachable!("caller matched Scan"),
    }
}

fn subtree_has_aggregate(plan: &LogicalPlan) -> bool {
    let mut node = Some(plan);
    while let Some(n) = node {
        if matches!(n, LogicalPlan::Aggregate { .. }) {
            return true;
        }
        node = n.input().map(|i| i.as_ref());
    }
    false
}

fn maybe_remap(expr: &Expr, map: &std::collections::HashMap<usize, usize>, yes: bool) -> Expr {
    if yes {
        remap_expr(expr, map)
    } else {
        expr.clone()
    }
}

fn old_to_new_of(needed: &BTreeSet<usize>) -> std::collections::HashMap<usize, usize> {
    needed
        .iter()
        .enumerate()
        .map(|(new, &old)| (old, new))
        .collect()
}

fn remap_expr(expr: &Expr, map: &std::collections::HashMap<usize, usize>) -> Expr {
    match expr {
        Expr::Column {
            index,
            name,
            data_type,
        } => {
            let index = map.get(index).copied().unwrap_or(*index);
            Expr::Column {
                index,
                name: name.clone(),
                data_type: data_type.clone(),
            }
        }
        Expr::Literal { .. } => expr.clone(),
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(remap_expr(expr, map)),
            to: to.clone(),
        },
        Expr::Not { expr } => Expr::Not {
            expr: Box::new(remap_expr(expr, map)),
        },
        Expr::Neg { expr, data_type } => Expr::Neg {
            expr: Box::new(remap_expr(expr, map)),
            data_type: data_type.clone(),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(remap_expr(expr, map)),
            negated: *negated,
        },
        Expr::Binary {
            op,
            left,
            right,
            data_type,
        } => Expr::Binary {
            op: *op,
            left: Box::new(remap_expr(left, map)),
            right: Box::new(remap_expr(right, map)),
            data_type: data_type.clone(),
        },
    }
}

// ---- 3. 谓词下推 ----

/// Filter(Scan) → 把 `列 op 字面量` 的合取项复制进 `Scan.filters` 提示。
/// 残差 Filter 原样保留（提示非精确，正确性由它保证）。
pub fn pushdown_predicates(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = pushdown_predicates((*input).clone());
            if let LogicalPlan::Scan {
                table,
                columns,
                schema,
                table_schema,
                filters: scan_filters,
            } = input
            {
                let scan_width = schema.fields().len();
                let mut filters = scan_filters;
                for mut pred in extract_pushable(&predicate) {
                    // 下推提示用表 schema 列索引（TableFormat 契约），从 scan 相对
                    // 索引换算；越界（不该发生）直接丢弃该提示
                    if pred.column >= scan_width {
                        continue;
                    }
                    pred.column = columns[pred.column];
                    if !filters.contains(&pred) {
                        filters.push(pred);
                    }
                }
                return LogicalPlan::Filter {
                    predicate,
                    input: Arc::new(LogicalPlan::Scan {
                        table,
                        columns,
                        schema,
                        table_schema,
                        filters,
                    }),
                };
            }
            LogicalPlan::Filter {
                predicate,
                input: Arc::new(input),
            }
        }
        other => rebuild_with_children(other, &pushdown_predicates),
    }
}

fn rebuild_with_children(
    plan: LogicalPlan,
    recurse: &dyn Fn(LogicalPlan) -> LogicalPlan,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { .. } => plan,
        LogicalPlan::Project {
            exprs,
            schema,
            input,
        } => LogicalPlan::Project {
            exprs,
            schema,
            input: Arc::new(recurse((*input).clone())),
        },
        LogicalPlan::Aggregate {
            keys,
            aggs,
            schema,
            span,
            distinct,
            input,
        } => LogicalPlan::Aggregate {
            keys,
            aggs,
            schema,
            span,
            distinct,
            input: Arc::new(recurse((*input).clone())),
        },
        LogicalPlan::Sort { keys, span, input } => LogicalPlan::Sort {
            keys,
            span,
            input: Arc::new(recurse((*input).clone())),
        },
        LogicalPlan::Limit { n, input } => LogicalPlan::Limit {
            n,
            input: Arc::new(recurse((*input).clone())),
        },
        LogicalPlan::Filter { .. } => unreachable!("Filter 由上层分支处理"),
    }
}

/// 拆开 AND 收集 `列 cmp 字面量`（或反序，翻转比较方向）的合取项。
fn extract_pushable(predicate: &Expr) -> Vec<ScanPredicate> {
    let mut conjuncts = Vec::new();
    split_conjuncts(predicate, &mut conjuncts);
    conjuncts
        .iter()
        .filter_map(|e| as_scan_predicate(e))
        .collect()
}

fn split_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
            ..
        } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

fn as_scan_predicate(expr: &Expr) -> Option<ScanPredicate> {
    let Expr::Binary {
        op, left, right, ..
    } = expr
    else {
        return None;
    };
    let cmp = |op: BinaryOp| match op {
        BinaryOp::Eq => Some(CmpOp::Eq),
        BinaryOp::NotEq => Some(CmpOp::NotEq),
        BinaryOp::Lt => Some(CmpOp::Lt),
        BinaryOp::LtEq => Some(CmpOp::LtEq),
        BinaryOp::Gt => Some(CmpOp::Gt),
        BinaryOp::GtEq => Some(CmpOp::GtEq),
        _ => None,
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column { index, .. }, Expr::Literal { value, .. }) => {
            if value.is_null() {
                return None;
            }
            Some(ScanPredicate {
                column: *index,
                op: cmp(*op)?,
                value: value.clone(),
            })
        }
        (Expr::Literal { value, .. }, Expr::Column { index, .. }) => {
            if value.is_null() {
                return None;
            }
            Some(ScanPredicate {
                column: *index,
                op: cmp(flip(*op)?)?,
                value: value.clone(),
            })
        }
        _ => None,
    }
}

fn flip(op: BinaryOp) -> Option<BinaryOp> {
    Some(match op {
        BinaryOp::Eq => BinaryOp::Eq,
        BinaryOp::NotEq => BinaryOp::NotEq,
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        _ => return None,
    })
}
