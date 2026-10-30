//! 自有 AST。节点都带 [`Span`]，供 planner 等下游在绑定错误里复用位置信息。
//!
//! 范围严格跟随 M1 SQL 面（ADR-0001）：SELECT 单表查询（表达式 / WHERE / GROUP BY /
//! 聚合 / DISTINCT / ORDER BY / LIMIT）+ SHOW TABLES + DESCRIBE。别名、JOIN、子查询、
//! WITH、HAVING、窗口函数明确不在 AST 里。

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

    pub fn merge(self, other: Span) -> Span {
        Span::new(self.start.min(other.start), self.end.max(other.end))
    }
}

/// 标识符（列名、表名）。引号标识符已去引号，原文大小写保留。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ident {
    pub name: String,
    pub span: Span,
}

/// 一条可执行的语句。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Query(Box<Query>),
    /// `SHOW TABLES`，span 覆盖整个短语
    ShowTables(Span),
    /// `DESCRIBE <表>`
    Describe {
        table: Ident,
    },
}

impl Statement {
    pub fn span(&self) -> Span {
        match self {
            Statement::Query(q) => q.span(),
            Statement::ShowTables(span) => *span,
            Statement::Describe { table } => table.span,
        }
    }
}

/// 单条 SELECT 查询。子句顺序即语法顺序：WHERE → GROUP BY → ORDER BY → LIMIT。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// `SELECT DISTINCT`：`Some` 存在，span 指向 DISTINCT 关键字
    pub distinct: Option<Span>,
    pub items: Vec<SelectItem>,
    pub from: Ident,
    /// WHERE 谓词
    pub selection: Option<Expr>,
    /// GROUP BY 键（空 = 无 GROUP BY，全局聚合留给 planner 判定合法性）
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderKey>,
    pub limit: Option<Limit>,
}

impl Query {
    /// 从首个投影项（或 DISTINCT 关键字）到最后一个子句末尾的区间，用于语句级
    /// 错误定位。SELECT 关键字本身不计入（AST 不保存它的位置）。
    pub fn span(&self) -> Span {
        let mut span = self.from.span;
        let parts = self
            .distinct
            .into_iter()
            .chain(self.items.iter().map(SelectItem::span))
            .chain(self.selection.iter().map(Expr::span))
            .chain(self.group_by.iter().map(Expr::span))
            .chain(self.order_by.iter().map(|k| k.expr.span()))
            .chain(self.limit.iter().map(|l| l.span));
        for part in parts {
            span = span.merge(part);
        }
        span
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem {
    /// `*`
    Wildcard(Span),
    /// 表达式投影（M1 无别名，列名由 planner 推导）
    Expr(Expr),
}

impl SelectItem {
    pub fn span(&self) -> Span {
        match self {
            SelectItem::Wildcard(span) => *span,
            SelectItem::Expr(e) => e.span(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderKey {
    pub expr: Expr,
    /// 默认 ASC
    pub direction: Direction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    pub n: u64,
    /// 数字字面量的位置
    pub span: Span,
}

/// 表达式。M1 面：字面量（数值/字符串/布尔/NULL/date/interval）、列引用、算术
/// （一元 ±、二元 + - * /）、比较、逻辑（AND/OR/NOT）、IS [NOT] NULL、
/// [NOT] BETWEEN、聚合函数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(Ident),
    /// 数值字面量原文（整数或小数），判型与精度处理在 planner
    Number {
        value: String,
        span: Span,
    },
    /// 字符串字面量（已去引号、`''` 还原）
    String {
        value: String,
        span: Span,
    },
    Bool {
        value: bool,
        span: Span,
    },
    Null(Span),
    /// `DATE 'yyyy-mm-dd'`，值为引号内原文，格式校验在 planner
    Date {
        value: String,
        span: Span,
    },
    /// `INTERVAL '<v>' <unit>`（单一单位；复合区间如 YEAR TO MONTH 不在 M1 面），
    /// 值为引号内原文
    Interval {
        value: String,
        unit: IntervalField,
        span: Span,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
        span: Span,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
        span: Span,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
        span: Span,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
        span: Span,
    },
    /// `COUNT/SUM/MIN/MAX/AVG(<表达式>)`；`COUNT(*)` 单独为 [`Expr::CountStar`]。
    /// 聚合内 DISTINCT 不在 M1 面（DISTINCT 只作 `SELECT DISTINCT`）。
    Aggregate {
        func: AggFunc,
        arg: Box<Expr>,
        span: Span,
    },
    CountStar(Span),
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Expr::Column(i) => i.span,
            Expr::Number { span, .. }
            | Expr::String { span, .. }
            | Expr::Bool { span, .. }
            | Expr::Date { span, .. }
            | Expr::Interval { span, .. }
            | Expr::Unary { span, .. }
            | Expr::Binary { span, .. }
            | Expr::IsNull { span, .. }
            | Expr::Between { span, .. }
            | Expr::Aggregate { span, .. } => *span,
            Expr::Null(span) | Expr::CountStar(span) => *span,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// 逻辑非 `NOT x`
    Not,
    /// 算术负号
    Neg,
    /// 算术正号
    Pos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// INTERVAL 的单位。M1 仅支持单一单位（TPC-H Q1 只需 DAY）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}
