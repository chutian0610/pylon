//! 自有 AST。节点都带 [`Span`]，供 planner 等下游在绑定错误里复用位置信息。

/// 源文本中的字节区间 `[start, end)`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

/// 标识符（列名、表名）。引号标识符已去引号，原文大小写保留。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ident {
    pub name: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    /// `*`
    Wildcard(Span),
    /// 单个列引用
    Column(Ident),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    pub n: u64,
    /// 数字字面量的位置
    pub span: Span,
}

/// 单条 SELECT 查询（T1 唯一语句形态）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub items: Vec<SelectItem>,
    pub from: Ident,
    pub limit: Option<Limit>,
}
