//! SQL → LogicalPlan → PhysicalPlan.
//!
//! M1 supports only:
//!   SELECT [* | <column>] FROM <table> [WHERE <col> <op> <literal>]
//!
//! Anything more complex returns an error.

use crate::logical::{Expr as LExpr, LogicalPlan};
use crate::physical::physical_expr::{PhysicalExpr};
use crate::physical::PhysicalPlan;

use arrow_schema::{Schema, SchemaRef};
use pylon_types::PylonError;
use sqlparser::ast::{BinaryOperator, Expr as AstExpr, Statement, Value};
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
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("amount", arrow_schema::DataType::Float64, false),
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
        _ => return Err(PylonError::InvalidPlan("only SELECT supported in M1".into())),
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
        schema,
    };

    // WHERE
    if let Some(where_expr) = &select.selection {
        let pred = translate_expr(where_expr, &input_schema(&input))?;
        input = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: pred,
        };
    }

    // SELECT
    if !matches!(select.projection.as_slice(), [sqlparser::ast::SelectItem::Wildcard(_)]) {
        let mut projs = Vec::new();
        for item in &select.projection {
            match item {
                sqlparser::ast::SelectItem::Wildcard(_) => {}
                sqlparser::ast::SelectItem::UnnamedExpr(e) => {
                    let pe = translate_expr(e, &input_schema(&input))?;
                    projs.push(pe);
                }
                _ => {
                    warn!("unhandled projection item: {item:?}; skipping");
                }
            }
        }
        if !projs.is_empty() {
            input = LogicalPlan::Project {
                input: Box::new(input),
                projections: projs,
            };
        }
    }

    Ok(input)
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
                    .or_else(|| {
                        // Fall back to logical field's type (used when SQL ambiguous)
                        Some(f)
                    })
                    .cloned()
                    .ok_or_else(|| {
                        PylonError::InvalidPlan(format!(
                            "project: column {} not found in input schema",
                            f.name()
                        ))
                    })?;
                (actual.name().clone(), actual.data_type().clone())
            }
            LExpr::Literal(_) => (format!("col_{i}"), arrow_schema::DataType::Utf8),
            _ => (format!("col_{i}"), arrow_schema::DataType::Float64),
        };
        fields.push(arrow_schema::Field::new(&name, dt, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn input_schema(p: &LogicalPlan) -> SchemaRef {
    match p {
        LogicalPlan::Scan { schema, .. } => schema.clone(),
        LogicalPlan::Filter { input, .. } => input_schema(input),
        LogicalPlan::Project { input, .. } => input_schema(input),
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
        _ => Err(PylonError::InvalidPlan(format!(
            "unsupported expression: {e:?}"
        )))?,
    })
}

fn physical_from_logical_expr(e: &LExpr) -> PhysicalExpr {
    match e {
        LExpr::Column(field) => PhysicalExpr::Column {
            index: 0,
            field: field.clone(),
        },
        LExpr::Literal(s) => PhysicalExpr::Literal {
            value: s.clone(),
            data_type: arrow_schema::DataType::Utf8,
        },
        LExpr::BinaryOp { left, op, right } => PhysicalExpr::BinaryOp {
            left: Box::new(physical_from_logical_expr(left)),
            op: op.clone(),
            right: Box::new(physical_from_logical_expr(right)),
        },
        LExpr::Wildcard => PhysicalExpr::Column {
            index: 0,
            field: arrow_schema::Field::new("_", arrow_schema::DataType::Null, true),
        },
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
