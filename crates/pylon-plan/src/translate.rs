//! SQL → LogicalPlan → PhysicalPlan.
//!
//! M1 supports:
//!   SELECT [* | <column>] FROM <table> [WHERE <col> <op> <literal>]
//!
//! M3 first cut adds:
//!   SELECT <group_by>, <aggs> FROM <table> [WHERE ...] GROUP BY <group_by>
//!
//! Supported aggregates: `COUNT(*)`, `SUM(<col>)`, `MIN(<col>)`, `MAX(<col>)`.
//! Anything more complex returns an error.

use crate::logical::{is_aggregate_expr, Expr as LExpr, LogicalPlan};
use crate::physical::physical_expr::{PhysicalExpr};
use crate::physical::PhysicalPlan;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use pylon_types::PylonError;
use sqlparser::ast::{
    BinaryOperator, Expr as AstExpr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, SelectItem, Statement, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

pub struct CatalogStub {
    schemas: HashMap<String, SchemaRef>,
    /// Logical table name → physical Parquet path (for M1; M3 swaps to Iceberg)
    paths: HashMap<String, String>,
}

impl CatalogStub {
    pub fn with_builtin() -> Self {
        let mut s = Self::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        s.schemas.insert("sample".into(), schema);
        s.paths.insert("sample".into(), "../data/sample.parquet".into());
        s
    }

    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            paths: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &str, schema: SchemaRef, parquet_path: &str) {
        self.schemas.insert(name.to_owned(), schema);
        self.paths.insert(name.to_owned(), parquet_path.to_owned());
    }

    pub fn get_schema(&self, table: &str) -> Result<SchemaRef, PylonError> {
        self.schemas
            .get(table)
            .cloned()
            .ok_or_else(|| PylonError::InvalidPlan(format!("table not found: {table}")))
    }

    pub fn get_path(&self, table: &str) -> Result<&str, PylonError> {
        self.paths
            .get(table)
            .map(|s| s.as_str())
            .ok_or_else(|| PylonError::InvalidPlan(format!("table not found: {table}")))
    }
}

pub fn parse_sql(sql: &str) -> Result<Statement, PylonError> {
    Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| PylonError::InvalidPlan(format!("sql parse error: {e}")))?
        .into_iter()
        .next()
        .ok_or_else(|| PylonError::InvalidPlan("empty sql".into()))
}

pub fn logical_from_sql(sql: &str, catalog: &CatalogStub) -> Result<LogicalPlan, PylonError> {
    let stmt = parse_sql(sql)?;
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return Err(PylonError::InvalidPlan("only SELECT supported".into())),
    };

    // FROM clause
    let from_tables: Vec<String> = query
        .with
        .map(|with| {
            with.cte_tables
                .iter()
                .map(|t| t.alias.name.value.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let select = match &*query.body {
        sqlparser::ast::SetExpr::Select(s) => s.clone(),
        _ => return Err(PylonError::InvalidPlan("only SELECT body supported".into())),
    };

    let base_table: String = if !from_tables.is_empty() {
        from_tables[0].clone()
    } else {
        select
            .from
            .first()
            .map(|t| t.relation.to_string())
            .ok_or_else(|| PylonError::InvalidPlan("FROM required".into()))?
    };

    let schema = catalog.get_schema(&base_table)?;
    let mut input = LogicalPlan::Scan {
        table: base_table,
        schema: schema.clone(),
    };

    // WHERE
    if let Some(where_expr) = &select.selection {
        let pred = translate_expr(where_expr, &input_schema(&input))?;
        input = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: pred,
        };
    }

    // GROUP BY? If present, wrap the projection in an Aggregate.
    let group_by_exprs: Option<Vec<AstExpr>> = match &select.group_by {
        GroupByExpr::Expressions(es, _) if !es.is_empty() => Some(es.clone()),
        GroupByExpr::Expressions(_, _) => None, // empty = no GROUP BY
        GroupByExpr::All(_) => {
            return Err(PylonError::InvalidPlan(
                "GROUP BY ALL not supported in M3 first cut".into(),
            ))
        }
    };

    // SELECT
    let has_wildcard = select
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard(_)));

    if has_wildcard && group_by_exprs.is_some() {
        return Err(PylonError::InvalidPlan(
            "SELECT * is incompatible with GROUP BY in M3 first cut".into(),
        ));
    }

    if has_wildcard {
        return Ok(input);
    }

    let pre_agg_schema = input_schema(&input);

    // Translate each projection item. For non-aggregate queries this
    // produces a Project; for aggregate queries we split into
    // (group_by, aggs) and wrap in Aggregate.
    let mut projs = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {}
            SelectItem::UnnamedExpr(e) => projs.push(translate_expr(e, &pre_agg_schema)?),
            SelectItem::ExprWithAlias { expr, alias } => {
                let mut p = translate_expr(expr, &pre_agg_schema)?;
                p = rename_expr(&p, &alias.value);
                projs.push(p);
            }
            _ => {
                warn!("select item not supported: {item:?}");
                return Err(PylonError::InvalidPlan(format!(
                    "select item not supported: {item:?}"
                )));
            }
        }
    }

    // Detect aggregation: either an explicit GROUP BY clause, or an
    // aggregate function call in the projection list (global aggregate).
    let has_agg = projs.iter().any(is_aggregate_expr);
    if has_agg || group_by_exprs.is_some() {
        // Split projections into group_by columns and aggregate calls.
        let mut group_by_lexprs = Vec::new();
        let mut agg_lexprs = Vec::new();
        for p in projs.iter() {
            if is_aggregate_expr(p) {
                agg_lexprs.push(p.clone());
            } else {
                group_by_lexprs.push(p.clone());
            }
        }
        // Translate the GROUP BY expressions themselves (they can be column
        // refs or expressions; M3 first cut only supports column refs).
        let mut group_by_from_ast = Vec::new();
        if let Some(grp_ast) = &group_by_exprs {
            for g in grp_ast {
                let le = translate_expr(g, &pre_agg_schema)?;
                if is_aggregate_expr(&le) {
                    return Err(PylonError::InvalidPlan(
                        "aggregate functions are not allowed in GROUP BY".into(),
                    ));
                }
                group_by_from_ast.push(le);
            }
        }
        // Sanity: every projected non-agg column must be in the
        // group_by list. SQL allows group_by columns to be omitted
        // from the projection (they're still computed but not emitted).
        for p in &group_by_lexprs {
            if !group_by_contains(&group_by_from_ast, p) {
                return Err(PylonError::InvalidPlan(format!(
                    "column {p:?} must appear in GROUP BY clause or inside an aggregate"
                )));
            }
        }
        // If no explicit GROUP BY was given but the projection
        // contains aggregates, this is a global aggregate. Reuse the
        // projected non-agg columns as the implicit group_by.
        if group_by_from_ast.is_empty() && has_agg && group_by_exprs.is_none() {
            group_by_from_ast = group_by_lexprs.clone();
        }
        let agg_schema = build_aggregate_schema(&group_by_from_ast, &agg_lexprs, &pre_agg_schema)?;
        return Ok(LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: group_by_from_ast,
            aggs: agg_lexprs,
            schema: agg_schema,
        });
    }

    Ok(LogicalPlan::Project {
        input: Box::new(input),
        projections: projs,
    })
}

pub fn physical_from_logical(logical: LogicalPlan) -> Result<PhysicalPlan, PylonError> {
    Ok(match logical {
        LogicalPlan::Scan { table, schema } => PhysicalPlan::SeqScan { table, schema },
        LogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(physical_from_logical(*input)?),
            predicate: physical_from_logical_expr(&predicate),
        },
        LogicalPlan::Project { input, projections } => {
            let projected_schema = compute_projected_schema(&projections, &input_schema(&input))?;
            PhysicalPlan::Project {
                input: Box::new(physical_from_logical(*input)?),
                projections: projections.iter().map(physical_from_logical_expr).collect(),
                schema: projected_schema,
            }
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggs,
            schema,
        } => {
            // A1-2 will replace this with proper aggregate lowering.
            // For A1-1 we plumb the new variant through so the rest of
            // the pipeline keeps compiling.
            PhysicalPlan::Aggregate {
                input: Box::new(physical_from_logical(*input)?),
                group_by: group_by.iter().map(physical_from_logical_expr).collect(),
                aggs: aggs.iter().map(physical_from_logical_expr).collect(),
                schema,
            }
        }
    })
}

fn compute_projected_schema(
    projections: &[LExpr],
    input_schema: &SchemaRef,
) -> Result<SchemaRef, PylonError> {
    let mut fields = Vec::new();
    for (i, p) in projections.iter().enumerate() {
        let (name, dt) = match p {
            LExpr::Column(f) => {
                let actual = input_schema
                    .field_with_name(f.name())
                    .ok()
                    .or_else(|| Some(f))
                    .cloned()
                    .ok_or_else(|| {
                        PylonError::InvalidPlan(format!(
                            "project: column {} not found in input schema",
                            f.name()
                        ))
                    })?;
                (actual.name().clone(), actual.data_type().clone())
            }
            LExpr::Literal(_) => (format!("col_{i}"), DataType::Utf8),
            _ => (format!("col_{i}"), DataType::Float64),
        };
        fields.push(Field::new(&name, dt, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Build the post-aggregation schema: one field per group_by column
/// (same name + type as the input), followed by one field per aggregate
/// (named `agg_name` or `agg_name(arg)` if no alias was supplied).
fn build_aggregate_schema(
    group_by: &[LExpr],
    aggs: &[LExpr],
    input_schema: &SchemaRef,
) -> Result<SchemaRef, PylonError> {
    let mut fields = Vec::new();
    for g in group_by {
        let f = match g {
            LExpr::Column(field) => input_schema
                .field_with_name(field.name())
                .map_err(|_| {
                    PylonError::InvalidPlan(format!(
                        "group by: column {} not found in input",
                        field.name()
                    ))
                })?
                .clone(),
            other => {
                return Err(PylonError::InvalidPlan(format!(
                    "group by expression must be a column reference in M3 first cut: {other:?}"
                )))
            }
        };
        fields.push(f);
    }
    for a in aggs {
        let field = match a {
            // `name` is either the user-supplied alias (from
            // `rename_expr`) or the default `agg_name` /
            // `agg_col` from `translate_aggregate_function`.
            LExpr::AggregateFunction { name, data_type, .. } => {
                Field::new(name, data_type.clone(), true)
            }
            _ => {
                return Err(PylonError::InvalidPlan(format!(
                    "aggregate expression expected, got: {a:?}"
                )))
            }
        };
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn group_by_contains(group_by: &[LExpr], target: &LExpr) -> bool {
    group_by.iter().any(|g| matches!((g, target),
        (LExpr::Column(a), LExpr::Column(b)) if a.name() == b.name()))
}

fn rename_expr(e: &LExpr, _alias: &str) -> LExpr {
    // M3 first cut: the field name is what propagates to the output
    // schema. We re-clone with the same data; downstream schema builder
    // uses column names from the input. To make aliases effective in
    // GROUP BY, we attach the alias as the renamed field.
    match e {
        LExpr::Column(f) => LExpr::Column(Field::new(_alias, f.data_type().clone(), f.is_nullable())),
        LExpr::AggregateFunction { func, name: _, args, data_type, input_data_types } => {
            // Aggregate function alias overrides the output field name.
            LExpr::AggregateFunction {
                func: func.clone(),
                name: _alias.to_string(),
                args: args.clone(),
                data_type: data_type.clone(),
                input_data_types: input_data_types.clone(),
            }
        }
        other => other.clone(),
    }
}

fn input_schema(p: &LogicalPlan) -> SchemaRef {
    match p {
        LogicalPlan::Scan { schema, .. } => schema.clone(),
        LogicalPlan::Filter { input, .. } => input_schema(input),
        LogicalPlan::Project { input, .. } => input_schema(input),
        LogicalPlan::Aggregate { schema, .. } => schema.clone(),
    }
}

fn translate_expr(e: &AstExpr, schema: &SchemaRef) -> Result<LExpr, PylonError> {
    Ok(match e {
        AstExpr::Identifier(ident) => {
            let name = ident.value.clone();
            let field = schema
                .field_with_name(&name)
                .map_err(|_| PylonError::InvalidPlan(format!("unknown column: {name}")))?;
            LExpr::Column(field.clone())
        }
        AstExpr::BinaryOp { left, op, right } => {
            let lhs = translate_expr(left, schema)?;
            let rhs = translate_expr(right, schema)?;
            let op_s = match op {
                BinaryOperator::Gt => ">",
                BinaryOperator::Lt => "<",
                BinaryOperator::GtEq => ">=",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "<>",
                _ => {
                    return Err(PylonError::InvalidPlan(format!(
                        "operator {op:?} not supported in M1"
                    )))
                }
            };
            LExpr::BinaryOp {
                left: Box::new(lhs),
                op: op_s.to_string(),
                right: Box::new(rhs),
            }
        }
        AstExpr::Value(v) => {
            let s = match &v.value {
                Value::Number(n, _) => n.clone(),
                Value::SingleQuotedString(s) => s.clone(),
                Value::Boolean(b) => b.to_string(),
                _ => format!("{:?}", v.value),
            };
            LExpr::Literal(s)
        }
        AstExpr::Function(func) => translate_aggregate_function(func, schema)?,
        _ => Err(PylonError::InvalidPlan(format!(
            "unsupported expression: {e:?}"
        )))?,
    })
}

/// Translate `COUNT(*)`, `SUM(col)`, `MIN(col)`, `MAX(col)` (and
/// any-DISTINCT variants) to `Expr::AggregateFunction`.
fn translate_aggregate_function(
    f: &Function,
    schema: &SchemaRef,
) -> Result<LExpr, PylonError> {
    let name = f
        .name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_lowercase())
        .ok_or_else(|| PylonError::InvalidPlan("malformed aggregate name".into()))?;

    // Reject window / filter / within-group for M3 first cut.
    if f.over.is_some() {
        return Err(PylonError::InvalidPlan(
            "window functions not supported in M3 first cut".into(),
        ));
    }
    if f.filter.is_some() {
        return Err(PylonError::InvalidPlan(
            "FILTER (WHERE ...) not supported in M3 first cut".into(),
        ));
    }
    if !f.within_group.is_empty() {
        return Err(PylonError::InvalidPlan(
            "WITHIN GROUP not supported in M3 first cut".into(),
        ));
    }
    if f.null_treatment.is_some() {
        return Err(PylonError::InvalidPlan(
            "IGNORE/RESPECT NULLS not supported in M3 first cut".into(),
        ));
    }

    let args_list = match &f.args {
        FunctionArguments::List(list) => &list.args,
        FunctionArguments::None => return Err(PylonError::InvalidPlan(
            "aggregate function called without arguments".into(),
        )),
        FunctionArguments::Subquery(_) => {
            return Err(PylonError::InvalidPlan(
                "subquery arguments to aggregates not supported in M3 first cut".into(),
            ))
        }
    };
    if let FunctionArguments::List(list) = &f.args {
        if list.duplicate_treatment.is_some() {
            return Err(PylonError::InvalidPlan(
                "DISTINCT inside aggregate not supported in M3 first cut".into(),
            ));
        }
    }

    let (args_lexpr, input_types) = match name.as_str() {
        "count" => {
            // COUNT(*) → args empty, COUNT(col) → args [col].
            if args_list.len() != 1 {
                return Err(PylonError::InvalidPlan(format!(
                    "COUNT expects 1 argument, got {}",
                    args_list.len()
                )));
            }
            let arg = &args_list[0];
            let (le, dt) = match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => (None, None),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                    let le = translate_expr(e, schema)?;
                    let dt = match &le {
                        LExpr::Column(f) => Some(f.data_type().clone()),
                        _ => {
                            return Err(PylonError::InvalidPlan(
                                "COUNT argument must be a column or *".into(),
                            ))
                        }
                    };
                    (Some(le), dt)
                }
                _ => {
                    return Err(PylonError::InvalidPlan(
                        "unsupported COUNT argument shape".into(),
                    ))
                }
            };
            (le.map(|x| vec![x]).unwrap_or_default(), dt.map(|x| vec![x]).unwrap_or_default())
        }
        "sum" | "min" | "max" => {
            if args_list.len() != 1 {
                return Err(PylonError::InvalidPlan(format!(
                    "{name} expects 1 argument, got {}",
                    args_list.len()
                )));
            }
            let arg = &args_list[0];
            let le = match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => translate_expr(e, schema)?,
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                    return Err(PylonError::InvalidPlan(format!(
                        "{name}(*) is not valid; supply a column"
                    )))
                }
                _ => {
                    return Err(PylonError::InvalidPlan(format!(
                        "unsupported {name} argument shape"
                    )))
                }
            };
            let dt = match &le {
                LExpr::Column(f) => f.data_type().clone(),
                _ => {
                    return Err(PylonError::InvalidPlan(format!(
                        "{name} argument must be a column"
                    )))
                }
            };
            if !is_numeric_or_orderable(&dt) {
                return Err(PylonError::InvalidPlan(format!(
                    "{name} does not support type {dt:?} in M3 first cut"
                )));
            }
            (vec![le], vec![dt])
        }
        other => {
            return Err(PylonError::InvalidPlan(format!(
                "aggregate function {other} not supported in M3 first cut"
            )))
        }
    };

    // Result type per aggregate:
    //   COUNT(*) / COUNT(any) → Int64 (count of non-null rows)
    //   SUM(int) → Int64, SUM(float) → Float64
    //   MIN / MAX → input type
    let result_type = match name.as_str() {
        "count" => DataType::Int64,
        "sum" => match &input_types[0] {
            DataType::Int64 => DataType::Int64,
            DataType::Float64 => DataType::Float64,
            DataType::Int32 => DataType::Int64,
            other => {
                return Err(PylonError::InvalidPlan(format!(
                    "SUM does not support type {other:?} in M3 first cut"
                )))
            }
        },
        "min" | "max" => input_types[0].clone(),
        _ => unreachable!(),
    };

    // Default field name = `func` (e.g. "count") for COUNT(*) or
    // `func_col` (e.g. "sum_amount") for `SUM(amount)`. When the user
    // supplies an `AS <alias>`, `rename_expr` overwrites `name` with the
    // alias.
    let default_name = match args_lexpr.first() {
        Some(LExpr::Column(c)) => format!("{name}_{}", c.name()),
        // COUNT(*) or other non-column arg → use the bare function name.
        _ => name.clone(),
    };
    Ok(LExpr::AggregateFunction {
        func: name,
        name: default_name,
        args: args_lexpr,
        data_type: result_type,
        input_data_types: input_types,
    })
}

fn is_numeric_or_orderable(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
    )
}

fn physical_from_logical_expr(e: &LExpr) -> PhysicalExpr {
    match e {
        LExpr::Column(field) => PhysicalExpr::Column {
            index: 0,
            field: field.clone(),
        },
        LExpr::Literal(s) => PhysicalExpr::Literal {
            value: s.clone(),
            data_type: DataType::Utf8,
        },
        LExpr::BinaryOp { left, op, right } => PhysicalExpr::BinaryOp {
            left: Box::new(physical_from_logical_expr(left)),
            op: op.clone(),
            right: Box::new(physical_from_logical_expr(right)),
        },
        LExpr::Wildcard => PhysicalExpr::Column {
            index: 0,
            field: Field::new("_", DataType::Null, true),
        },
        LExpr::AggregateFunction { func, name, args, data_type, input_data_types } => {
            PhysicalExpr::AggregateFunction {
                func: func.clone(),
                name: name.clone(),
                args: args.iter().map(physical_from_logical_expr).collect(),
                data_type: data_type.clone(),
                input_data_types: input_data_types.clone(),
            }
        }
    }
}

impl Clone for CatalogStub {
    fn clone(&self) -> Self {
        Self {
            schemas: self.schemas.clone(),
            paths: self.paths.clone(),
        }
    }
}
