//! Pylon SQL 前端：手写 lexer + nom 语法层，产出自有 AST（ADR-0001）。
//!
//! 当前语法面（M1/T1）：`SELECT <列, … | *> FROM <表> [LIMIT <n>]`。
//! 语法范围严格跟随里程碑 SQL 面，不预建通用 parser。
//!
//! 所有错误都携带源文本字节区间 [`Span`]，可经 [`SyntaxError::render`] 渲染为
//! 带行列与脱字符的用户可见错误。

pub mod ast;
pub mod error;
pub mod lexer;
mod parser;

pub use ast::{Ident, Limit, Query, SelectItem, Span};
pub use error::{LineCol, SyntaxError, line_col, render_at};
pub use parser::parse;
