//! nom 语法层：在 token 切片上组合解析，产出 [`Statement`]。
//!
//! 语句骨架（SELECT/SHOW/DESCRIBE 及各子句顺序）按序手工串接，以便每个失败点给出
//! 精确的 "expected X, found Y" 消息；表达式按 SQL 优先级递归下降：
//!
//! ```text
//! or        := and (OR and)*
//! and       := not (AND not)*
//! not       := NOT not | predicate
//! predicate := additive ( (=|<>|…) additive | IS [NOT] NULL | [NOT] BETWEEN additive AND additive )?
//! additive  := multiplicative ((+|-) multiplicative)*
//! multiplicative := unary ((*|/) unary)*
//! unary     := (-|+) unary | primary
//! primary   := number | string | TRUE | FALSE | NULL | DATE string | INTERVAL string unit
//!            | agg '(' expr ')' | COUNT '(' '*' ')' | '(' expr ')' | column
//! ```
//!
//! 回溯边界：可选前缀（子句关键字、运算符）失败是软错误；一旦消费了关键字或
//! 运算符，后续必填部分固化为硬错误（`nom::Err::Failure`），避免回退产生误导性
//! 消息。

use nom::combinator::{cut, map, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::separated_list1;
use nom::{IResult, Input, Needed, Parser};

use crate::ast::{
    AggFunc, BinaryOp, Direction, Expr, Ident, IntervalField, Limit, OrderKey, Query, SelectItem,
    Span, Statement, UnaryOp,
};
use crate::error::SyntaxError;
use crate::lexer::{Tok, Token, tokenize};

/// 解析单条语句（M1 面：SELECT 查询 / SHOW TABLES / DESCRIBE）。
pub fn parse(source: &str) -> Result<Statement, SyntaxError> {
    let tokens = tokenize(source)?;
    let (rest, stmt) = parse_statement(Tokens(&tokens)).map_err(|e| to_syntax_error(e, source))?;
    // 语句尾至多一个分号（SHOW/DESCRIBE 走到这里；查询的分号已在 parse_query 消费）
    let (rest, _) = opt(punct(Tok::Semi, "';'"))
        .parse(rest)
        .map_err(|e| to_syntax_error(e, source))?;
    if rest.first().tok != Tok::Eof {
        let e = PError::expected(rest.first(), "end of input");
        return Err(to_syntax_error(nom::Err::Error(e), source));
    }
    Ok(stmt)
}

/// nom 输入：token 切片。lexer 保证末尾恒有 `Eof`，且本模块的 token 解析器从不消费
/// `Eof`，因此切片在整个解析过程中永不为空。
#[derive(Debug, Clone, Copy)]
struct Tokens<'a>(&'a [Token]);

impl<'a> Tokens<'a> {
    fn first(&self) -> &'a Token {
        self.0.first().expect("token stream always ends with Eof")
    }

    fn second(&self) -> Option<&'a Token> {
        self.0.get(1)
    }
}

impl<'a> Input for Tokens<'a> {
    type Item = &'a Token;
    type Iter = std::slice::Iter<'a, Token>;
    type IterIndices = std::iter::Enumerate<std::slice::Iter<'a, Token>>;

    fn input_len(&self) -> usize {
        self.0.len()
    }

    fn take(&self, index: usize) -> Self {
        Tokens(&self.0[..index])
    }

    fn take_from(&self, index: usize) -> Self {
        Tokens(&self.0[index..])
    }

    fn take_split(&self, index: usize) -> (Self, Self) {
        let (prefix, suffix) = self.0.split_at(index);
        (Tokens(suffix), Tokens(prefix))
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.0.iter().position(predicate)
    }

    fn iter_elements(&self) -> Self::Iter {
        self.0.iter()
    }

    fn iter_indices(&self) -> Self::IterIndices {
        self.0.iter().enumerate()
    }

    fn slice_index(&self, count: usize) -> Result<usize, Needed> {
        if self.0.len() >= count {
            Ok(count)
        } else {
            Err(Needed::new(count - self.0.len()))
        }
    }
}

/// 语法层错误：定位到出错 token，附带期望描述。
#[derive(Debug)]
struct PError {
    span: Span,
    message: String,
}

impl<'a> ParseError<Tokens<'a>> for PError {
    fn from_error_kind(input: Tokens<'a>, _kind: ErrorKind) -> Self {
        let t = input.first();
        PError {
            span: t.span,
            message: format!("unexpected {}", t.describe()),
        }
    }

    fn append(_input: Tokens<'a>, _kind: ErrorKind, other: Self) -> Self {
        other
    }
}

type PResult<'a, T> = IResult<Tokens<'a>, T, PError>;

impl PError {
    fn expected(found: &Token, what: &str) -> Self {
        PError {
            span: found.span,
            message: format!("expected {what}, found {}", found.describe()),
        }
    }
}

fn expected<'a, T>(input: Tokens<'a>, what: &str) -> PResult<'a, T> {
    Err(nom::Err::Error(PError::expected(input.first(), what)))
}

/// 消费了必填前缀之后的解析：软失败按硬错误报 "expected {what}, found X"；
/// 解析器主动抛出的 `Failure`（如 LIMIT 的数字形态错误）保留原消息。
fn cut_after<'a, O>(
    what: &'static str,
    mut parser: impl Parser<Tokens<'a>, Output = O, Error = PError>,
) -> impl FnMut(Tokens<'a>) -> PResult<'a, O> {
    move |input| {
        parser.parse(input).map_err(|e| match e {
            nom::Err::Error(_) => nom::Err::Failure(PError::expected(input.first(), what)),
            deliberate => deliberate,
        })
    }
}

fn to_syntax_error(e: nom::Err<PError>, source: &str) -> SyntaxError {
    match e {
        nom::Err::Error(pe) | nom::Err::Failure(pe) => SyntaxError::new(pe.span, pe.message),
        nom::Err::Incomplete(_) => SyntaxError::new(
            Span::new(source.len(), source.len()),
            "unexpected end of input",
        ),
    }
}

/// 保留字：这些词不能做未加引号的标识符。SHOW/TABLES/DESCRIBE 只在语句开头按位置
/// 识别，聚合名、ASC/DESC、interval 单位也只在特定语法位置有意义——它们都不在列，
/// 别处可作列名（跟随 Trino 的非保留字处理）。
const KEYWORDS: &[&str] = &[
    "SELECT", "DISTINCT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "AND", "OR", "NOT",
    "IS", "NULL", "BETWEEN", "DATE", "INTERVAL", "TRUE", "FALSE",
];

fn is_keyword(word: &str) -> bool {
    KEYWORDS.iter().any(|k| k.eq_ignore_ascii_case(word))
}

fn is_word(t: &Token, kw: &str) -> bool {
    matches!(&t.tok, Tok::Word(w) if w.eq_ignore_ascii_case(kw))
}

/// 不区分大小写的关键字（未加引号的词）。
fn keyword<'a>(kw: &'static str) -> impl FnMut(Tokens<'a>) -> PResult<'a, &'a Token> {
    move |input| {
        let t = input.first();
        if is_word(t, kw) {
            Ok((input.take_from(1), t))
        } else {
            expected(input, kw)
        }
    }
}

/// 标识符：非保留字的词，或引号标识符。
fn ident<'a>(what: &'static str) -> impl FnMut(Tokens<'a>) -> PResult<'a, Ident> {
    move |input| {
        let t = input.first();
        let name = match &t.tok {
            Tok::Word(w) if !is_keyword(w) => w.clone(),
            Tok::QuotedIdent(q) => q.clone(),
            _ => return expected(input, what),
        };
        Ok((input.take_from(1), Ident { name, span: t.span }))
    }
}

fn punct<'a>(tok: Tok, what: &'static str) -> impl FnMut(Tokens<'a>) -> PResult<'a, &'a Token> {
    move |input| {
        let t = input.first();
        if t.tok == tok {
            Ok((input.take_from(1), t))
        } else {
            expected(input, what)
        }
    }
}

// ---- 语句层 ----

fn parse_statement(input: Tokens<'_>) -> PResult<'_, Statement> {
    let t = input.first();
    if is_word(t, "SELECT") {
        let (rest, q) = parse_query(input)?;
        Ok((rest, Statement::Query(Box::new(q))))
    } else if is_word(t, "SHOW") {
        let (rest, _) = keyword("SHOW").parse(input)?;
        let (rest, tables) = cut_after("TABLES after SHOW", keyword("TABLES")).parse(rest)?;
        Ok((
            rest,
            Statement::ShowTables(Span::new(t.span.start, tables.span.end)),
        ))
    } else if is_word(t, "DESCRIBE") {
        let (rest, _) = keyword("DESCRIBE").parse(input)?;
        let (rest, table) =
            cut_after("table name after DESCRIBE", ident("table name")).parse(rest)?;
        Ok((rest, Statement::Describe { table }))
    } else {
        Err(nom::Err::Error(PError::expected(
            t,
            "SELECT, SHOW TABLES, or DESCRIBE",
        )))
    }
}

fn parse_query(input: Tokens<'_>) -> PResult<'_, Query> {
    let (rest, _) = keyword("SELECT").parse(input)?;
    let (rest, distinct) = opt(map(keyword("DISTINCT"), |t: &Token| t.span)).parse(rest)?;
    let (rest, items) = select_list(rest)?;
    let (rest, _) = from_keyword(rest)?;
    let (rest, from) = ident("table name").parse(rest)?;
    let (rest, selection) = opt(where_clause).parse(rest)?;
    let (rest, group_by) = opt(group_by_clause).parse(rest)?;
    let (rest, order_by) = opt(order_by_clause).parse(rest)?;
    let (rest, limit) = opt(limit_clause).parse(rest)?;
    let (rest, _) = opt(punct(Tok::Semi, "';'")).parse(rest)?;

    let trailing = rest.first();
    if trailing.tok != Tok::Eof {
        // 子句有序：只有排在一个已消费子句之后的子句仍可出现
        const CLAUSES: [&str; 4] = ["WHERE", "GROUP BY", "ORDER BY", "LIMIT"];
        let consumed = [
            selection.is_some(),
            group_by.is_some(),
            order_by.is_some(),
            limit.is_some(),
        ];
        let next = consumed.iter().rposition(|&c| c).map_or(0, |last| last + 1);
        let mut what: Vec<&str> = CLAUSES[next..].to_vec();
        what.push("end of input");
        let what = match what.len() {
            1 => what[0].to_string(),
            2 => format!("{} or {}", what[0], what[1]),
            n => format!("{}, or {}", what[..n - 1].join(", "), what[n - 1]),
        };
        return Err(nom::Err::Error(PError::expected(trailing, &what)));
    }

    Ok((
        rest,
        Query {
            distinct,
            items,
            from,
            selection,
            group_by: group_by.unwrap_or_default(),
            order_by: order_by.unwrap_or_default(),
            limit,
        },
    ))
}

/// FROM 关键字；错误消息覆盖逗号分隔的另一种合法延续。
fn from_keyword(input: Tokens<'_>) -> PResult<'_, ()> {
    match keyword("FROM").parse(input) {
        Ok((rest, _)) => Ok((rest, ())),
        Err(_) => expected(input, "',' or FROM"),
    }
}

fn select_item(input: Tokens<'_>) -> PResult<'_, SelectItem> {
    let t = input.first();
    if t.tok == Tok::Star {
        return Ok((input.take_from(1), SelectItem::Wildcard(t.span)));
    }
    // 仅包装软失败；内部硬错误（如聚合缺 ')'）原样传播
    let (rest, e) = match expr(input) {
        Ok(ok) => ok,
        Err(nom::Err::Error(_)) => return expected(input, "an expression or '*'"),
        Err(deliberate) => return Err(deliberate),
    };
    Ok((rest, SelectItem::Expr(e)))
}

fn select_list(input: Tokens<'_>) -> PResult<'_, Vec<SelectItem>> {
    // 分隔符之后的项失败视为硬错误，避免回退到逗号处产生误导性消息
    separated_list1(punct(Tok::Comma, "','"), cut(select_item)).parse(input)
}

fn where_clause(input: Tokens<'_>) -> PResult<'_, Expr> {
    let (rest, _) = keyword("WHERE").parse(input)?;
    cut_after("an expression after WHERE", expr).parse(rest)
}

fn group_by_clause(input: Tokens<'_>) -> PResult<'_, Vec<Expr>> {
    let (rest, _) = keyword("GROUP").parse(input)?;
    let (rest, _) = cut_after("BY after GROUP", keyword("BY")).parse(rest)?;
    cut_after("an expression after GROUP BY", expr_list).parse(rest)
}

fn order_by_clause(input: Tokens<'_>) -> PResult<'_, Vec<OrderKey>> {
    let (rest, _) = keyword("ORDER").parse(input)?;
    let (rest, _) = cut_after("BY after ORDER", keyword("BY")).parse(rest)?;
    cut_after("an ordering expression after ORDER BY", order_key_list).parse(rest)
}

fn expr_list(input: Tokens<'_>) -> PResult<'_, Vec<Expr>> {
    separated_list1(punct(Tok::Comma, "','"), cut(expr)).parse(input)
}

fn order_key(input: Tokens<'_>) -> PResult<'_, OrderKey> {
    let (rest, e) = expr(input)?;
    let t = rest.first();
    let (rest, direction) = if is_word(t, "ASC") {
        (rest.take_from(1), Direction::Asc)
    } else if is_word(t, "DESC") {
        (rest.take_from(1), Direction::Desc)
    } else {
        (rest, Direction::Asc)
    };
    Ok((rest, OrderKey { expr: e, direction }))
}

fn order_key_list(input: Tokens<'_>) -> PResult<'_, Vec<OrderKey>> {
    separated_list1(punct(Tok::Comma, "','"), cut(order_key)).parse(input)
}

fn limit_count(input: Tokens<'_>) -> PResult<'_, Limit> {
    let t = input.first();
    match &t.tok {
        Tok::Number(n) => match n.parse::<u64>() {
            Ok(v) => Ok((input.take_from(1), Limit { n: v, span: t.span })),
            Err(_) => Err(nom::Err::Failure(PError {
                span: t.span,
                message: format!("LIMIT expects a non-negative integer, found '{n}'"),
            })),
        },
        _ => expected(input, "a non-negative integer after LIMIT"),
    }
}

fn limit_clause(input: Tokens<'_>) -> PResult<'_, Limit> {
    let (rest, _) = keyword("LIMIT").parse(input)?;
    cut_after("a non-negative integer after LIMIT", limit_count).parse(rest)
}

// ---- 表达式层：优先级自低到高 ----

fn expr(input: Tokens<'_>) -> PResult<'_, Expr> {
    expr_or(input)
}

fn expr_or(input: Tokens<'_>) -> PResult<'_, Expr> {
    binary_level(input, expr_and, |t| match t {
        Tok::Word(w) if w.eq_ignore_ascii_case("OR") => Some(BinaryOp::Or),
        _ => None,
    })
}

fn expr_and(input: Tokens<'_>) -> PResult<'_, Expr> {
    binary_level(input, expr_not, |t| match t {
        Tok::Word(w) if w.eq_ignore_ascii_case("AND") => Some(BinaryOp::And),
        _ => None,
    })
}

fn expr_not(input: Tokens<'_>) -> PResult<'_, Expr> {
    let t = input.first();
    if is_word(t, "NOT") {
        let rest = input.take_from(1);
        let (rest, e) = cut_after("an expression after NOT", expr_not).parse(rest)?;
        let span = t.span.merge(e.span());
        return Ok((
            rest,
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(e),
                span,
            },
        ));
    }
    expr_predicate(input)
}

fn expr_predicate(input: Tokens<'_>) -> PResult<'_, Expr> {
    let (rest, left) = expr_additive(input)?;
    let t = rest.first();

    let cmp = match &t.tok {
        Tok::Eq => Some(BinaryOp::Eq),
        Tok::NotEq => Some(BinaryOp::NotEq),
        Tok::Lt => Some(BinaryOp::Lt),
        Tok::LtEq => Some(BinaryOp::LtEq),
        Tok::Gt => Some(BinaryOp::Gt),
        Tok::GtEq => Some(BinaryOp::GtEq),
        _ => None,
    };
    if let Some(op) = cmp {
        let after = rest.take_from(1);
        let (rest, right) =
            cut_after("an expression after the comparison", expr_additive).parse(after)?;
        let span = left.span().merge(right.span());
        return Ok((
            rest,
            Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
                span,
            },
        ));
    }

    if is_word(t, "IS") {
        let after_is = rest.take_from(1);
        let (rest, negated) = if is_word(after_is.first(), "NOT") {
            (after_is.take_from(1), true)
        } else {
            (after_is, false)
        };
        let (rest, null_tok) = cut_after("NULL after IS", keyword("NULL")).parse(rest)?;
        let span = left.span().merge(null_tok.span);
        return Ok((
            rest,
            Expr::IsNull {
                expr: Box::new(left),
                negated,
                span,
            },
        ));
    }

    if is_word(t, "BETWEEN") || is_word(t, "NOT") {
        let (rest, negated) = if is_word(t, "BETWEEN") {
            (rest.take_from(1), false)
        } else {
            let after_not = rest.take_from(1);
            let (rest, _) = cut_after("BETWEEN after NOT", keyword("BETWEEN")).parse(after_not)?;
            (rest, true)
        };
        let (rest, low) = cut_after("an expression after BETWEEN", expr_additive).parse(rest)?;
        let (rest, _) = cut_after("AND between the BETWEEN bounds", keyword("AND")).parse(rest)?;
        let (rest, high) =
            cut_after("an expression after BETWEEN AND", expr_additive).parse(rest)?;
        let span = left.span().merge(high.span());
        return Ok((
            rest,
            Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated,
                span,
            },
        ));
    }

    Ok((rest, left))
}

fn expr_additive(input: Tokens<'_>) -> PResult<'_, Expr> {
    binary_level(input, expr_multiplicative, |t| match t {
        Tok::Plus => Some(BinaryOp::Add),
        Tok::Minus => Some(BinaryOp::Sub),
        _ => None,
    })
}

fn expr_multiplicative(input: Tokens<'_>) -> PResult<'_, Expr> {
    binary_level(input, expr_unary, |t| match t {
        Tok::Star => Some(BinaryOp::Mul),
        Tok::Slash => Some(BinaryOp::Div),
        _ => None,
    })
}

/// 左结合二元层：`ops` 把当前 token 映射为运算符；运算符后的右操作数是硬错误。
fn binary_level<'a>(
    input: Tokens<'a>,
    next: fn(Tokens<'a>) -> PResult<'a, Expr>,
    ops: impl Fn(&Tok) -> Option<BinaryOp>,
) -> PResult<'a, Expr> {
    let (mut rest, mut left) = next(input)?;
    while let Some(op) = ops(&rest.first().tok) {
        let after = rest.take_from(1);
        let (r, right) = cut_after("an expression after the operator", next).parse(after)?;
        let span = left.span().merge(right.span());
        left = Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
            span,
        };
        rest = r;
    }
    Ok((rest, left))
}

fn expr_unary(input: Tokens<'_>) -> PResult<'_, Expr> {
    let t = input.first();
    let op = match t.tok {
        Tok::Minus => Some(UnaryOp::Neg),
        Tok::Plus => Some(UnaryOp::Pos),
        _ => None,
    };
    match op {
        Some(op) => {
            let after = input.take_from(1);
            let (rest, e) = cut_after("an expression after the sign", expr_unary).parse(after)?;
            let span = t.span.merge(e.span());
            Ok((
                rest,
                Expr::Unary {
                    op,
                    expr: Box::new(e),
                    span,
                },
            ))
        }
        None => expr_primary(input),
    }
}

/// 聚合函数名（后跟 `(` 才视为聚合，否则按普通词处理为列名）。
fn agg_func(word: &str) -> Option<AggFunc> {
    match word.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        "AVG" => Some(AggFunc::Avg),
        _ => None,
    }
}

/// interval 单位词表：解析与错误消息共用同一来源，新增单位只改这里。
const INTERVAL_UNITS: &[(&str, IntervalField)] = &[
    ("YEAR", IntervalField::Year),
    ("MONTH", IntervalField::Month),
    ("DAY", IntervalField::Day),
    ("HOUR", IntervalField::Hour),
    ("MINUTE", IntervalField::Minute),
    ("SECOND", IntervalField::Second),
];

fn interval_field(word: &str) -> Option<IntervalField> {
    INTERVAL_UNITS
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(word))
        .map(|(_, unit)| *unit)
}

fn interval_unit_expectation() -> String {
    let names: Vec<&str> = INTERVAL_UNITS.iter().map(|(name, _)| *name).collect();
    format!("an interval unit ({})", names.join(", "))
}

fn expr_primary(input: Tokens<'_>) -> PResult<'_, Expr> {
    let t = input.first();
    match &t.tok {
        Tok::Number(n) => {
            let e = Expr::Number {
                value: n.clone(),
                span: t.span,
            };
            Ok((input.take_from(1), e))
        }
        Tok::Str(s) => {
            let e = Expr::String {
                value: s.clone(),
                span: t.span,
            };
            Ok((input.take_from(1), e))
        }
        Tok::QuotedIdent(q) => {
            let e = Expr::Column(Ident {
                name: q.clone(),
                span: t.span,
            });
            Ok((input.take_from(1), e))
        }
        Tok::LParen => {
            let after = input.take_from(1);
            let (rest, e) = cut_after("an expression after '('", expr).parse(after)?;
            let (rest, _) = cut_after("')'", punct(Tok::RParen, "')'")).parse(rest)?;
            // 括号不产生节点：返回内部表达式本身（span 仍指向内部构造）
            Ok((rest, e))
        }
        Tok::Word(w) => {
            if w.eq_ignore_ascii_case("TRUE") || w.eq_ignore_ascii_case("FALSE") {
                let e = Expr::Bool {
                    value: w.eq_ignore_ascii_case("TRUE"),
                    span: t.span,
                };
                return Ok((input.take_from(1), e));
            }
            if w.eq_ignore_ascii_case("NULL") {
                return Ok((input.take_from(1), Expr::Null(t.span)));
            }
            if w.eq_ignore_ascii_case("DATE") {
                return date_literal(input);
            }
            if w.eq_ignore_ascii_case("INTERVAL") {
                return interval_literal(input);
            }
            if agg_func(w).is_some() && input.second().is_some_and(|s| s.tok == Tok::LParen) {
                return aggregate(input);
            }
            if is_keyword(w) {
                return expected(input, "an expression");
            }
            let e = Expr::Column(Ident {
                name: w.clone(),
                span: t.span,
            });
            Ok((input.take_from(1), e))
        }
        _ => expected(input, "an expression"),
    }
}

fn date_literal(input: Tokens<'_>) -> PResult<'_, Expr> {
    let date_tok = input.first();
    let rest = input.take_from(1);
    let t = rest.first();
    match &t.tok {
        Tok::Str(s) => {
            let e = Expr::Date {
                value: s.clone(),
                span: date_tok.span.merge(t.span),
            };
            Ok((rest.take_from(1), e))
        }
        _ => Err(nom::Err::Failure(PError::expected(
            t,
            "a string after DATE",
        ))),
    }
}

fn interval_literal(input: Tokens<'_>) -> PResult<'_, Expr> {
    let kw_tok = input.first();
    let rest = input.take_from(1);

    let t = rest.first();
    let Tok::Str(value) = &t.tok else {
        return Err(nom::Err::Failure(PError::expected(
            t,
            "a string after INTERVAL",
        )));
    };
    let value = value.clone();
    let rest = rest.take_from(1);

    // 单一单位；复合区间（YEAR TO MONTH 等）不在 M1 面
    let t = rest.first();
    let unit = match &t.tok {
        Tok::Word(w) => interval_field(w),
        _ => None,
    };
    let Some(unit) = unit else {
        return Err(nom::Err::Failure(PError::expected(
            t,
            &interval_unit_expectation(),
        )));
    };

    let e = Expr::Interval {
        value,
        unit,
        span: kw_tok.span.merge(t.span),
    };
    Ok((rest.take_from(1), e))
}

fn aggregate(input: Tokens<'_>) -> PResult<'_, Expr> {
    let func_tok = input.first();
    let Tok::Word(w) = &func_tok.tok else {
        unreachable!("caller checked a word token")
    };
    let func = agg_func(w).expect("caller checked aggregate name");
    let rest = input.take_from(2); // 函数名 + '('（调用方已确认）

    if func == AggFunc::Count && rest.first().tok == Tok::Star {
        let rest = rest.take_from(1);
        let (rest, close) =
            cut_after("')' after COUNT(*)", punct(Tok::RParen, "')'")).parse(rest)?;
        return Ok((rest, Expr::CountStar(func_tok.span.merge(close.span))));
    }

    let (rest, arg) = cut_after("an aggregate argument", expr).parse(rest)?;
    let (rest, close) = cut_after(
        "')' after the aggregate argument",
        punct(Tok::RParen, "')'"),
    )
    .parse(rest)?;
    let span = func_tok.span.merge(close.span);
    Ok((
        rest,
        Expr::Aggregate {
            func,
            arg: Box::new(arg),
            span,
        },
    ))
}
