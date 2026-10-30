//! Pylon SQL 前端：手写 lexer + nom 语法层，产出自有 AST（ADR-0001）。
//!
//! M1 SQL 面（#3）：SELECT（投影 / 算术 / 比较 / 逻辑 / IS NULL / BETWEEN /
//! date 与 interval 字面量及运算 / WHERE / GROUP BY / 聚合 COUNT SUM MIN MAX AVG /
//! DISTINCT / ORDER BY / LIMIT）+ SHOW TABLES + DESCRIBE。语法范围严格跟随里程碑
//! SQL 面，不预建通用 parser。
//!
//! 所有错误都携带源文本字节区间 [`Span`]，可经 [`SyntaxError::render`] 渲染为
//! 带行列与脱字符的用户可见错误。[`pretty`] 提供确定性 AST 文本渲染（`--parse`
//! 演示与 golden 快照测试共用）。

pub mod ast;
pub mod error;
pub mod lexer;
mod parser;
pub mod pretty;

pub use ast::{
    AggFunc, BinaryOp, Direction, Expr, Ident, IntervalField, Limit, OrderKey, Query, SelectItem,
    Span, Statement, UnaryOp,
};
pub use error::{LineCol, SyntaxError, line_col, render_at};
pub use parser::parse;
