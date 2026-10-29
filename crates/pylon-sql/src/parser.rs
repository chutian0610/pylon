//! nom 语法层：在 token 切片上组合解析，产出 [`Query`]。
//!
//! 语句骨架（SELECT … FROM … [LIMIT …]）按顺序手工串接，以便每个失败点给出精确的
//! "expected X, found Y" 消息；列表、可选项、分支等用 nom 组合子表达。

use nom::branch::alt;
use nom::combinator::{cut, map, opt};
use nom::error::{ErrorKind, ParseError};
use nom::multi::separated_list1;
use nom::sequence::preceded;
use nom::{IResult, Input, Needed, Parser};

use crate::ast::{Ident, Limit, Query, SelectItem, Span};
use crate::error::SyntaxError;
use crate::lexer::{Tok, Token, tokenize};

/// 解析单条 SELECT 查询。
pub fn parse(source: &str) -> Result<Query, SyntaxError> {
    let tokens = tokenize(source)?;
    parse_query(Tokens(&tokens)).map_err(|e| match e {
        nom::Err::Error(pe) | nom::Err::Failure(pe) => SyntaxError::new(pe.span, pe.message),
        nom::Err::Incomplete(_) => SyntaxError::new(
            Span::new(source.len(), source.len()),
            "unexpected end of input",
        ),
    })
}

/// nom 输入：token 切片。lexer 保证末尾恒有 `Eof`，且本模块的 token 解析器从不消费
/// `Eof`，因此切片在整个解析过程中永不为空。
#[derive(Debug, Clone, Copy)]
struct Tokens<'a>(&'a [Token]);

impl<'a> Tokens<'a> {
    fn first(&self) -> &'a Token {
        self.0.first().expect("token stream always ends with Eof")
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

/// 覆写子解析器失败时的消息（位置保留）。
fn expecting<'a, O>(
    what: &'static str,
    mut parser: impl Parser<Tokens<'a>, Output = O, Error = PError>,
) -> impl FnMut(Tokens<'a>) -> PResult<'a, O> {
    move |input| {
        parser
            .parse(input)
            .map_err(|e| e.map(|_| PError::expected(input.first(), what)))
    }
}

const KEYWORDS: &[&str] = &["SELECT", "FROM", "LIMIT"];

fn is_keyword(word: &str) -> bool {
    KEYWORDS.iter().any(|k| word.eq_ignore_ascii_case(k))
}

/// 不区分大小写的关键字（未加引号的词）。
fn keyword<'a>(kw: &'static str) -> impl FnMut(Tokens<'a>) -> PResult<'a, &'a Token> {
    move |input| {
        let t = input.first();
        match &t.tok {
            Tok::Word(w) if w.eq_ignore_ascii_case(kw) => Ok((input.take_from(1), t)),
            _ => expected(input, kw),
        }
    }
}

/// 标识符：非关键字的词，或引号标识符。
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

fn select_item(input: Tokens<'_>) -> PResult<'_, SelectItem> {
    expecting(
        "column name or '*'",
        alt((
            map(punct(Tok::Star, "'*'"), |t: &Token| {
                SelectItem::Wildcard(t.span)
            }),
            map(ident("column name"), SelectItem::Column),
        )),
    )(input)
}

fn select_list(input: Tokens<'_>) -> PResult<'_, Vec<SelectItem>> {
    // 分隔符之后的项失败视为硬错误（cut），避免回退到逗号处产生误导性消息
    separated_list1(punct(Tok::Comma, "','"), cut(select_item)).parse(input)
}

fn limit_clause(input: Tokens<'_>) -> PResult<'_, Limit> {
    preceded(keyword("LIMIT"), cut(limit_count)).parse(input)
}

fn parse_query(input: Tokens<'_>) -> Result<Query, nom::Err<PError>> {
    let (rest, _) = keyword("SELECT")(input)?;
    let (rest, items) = select_list(rest)?;
    let (rest, _) = expecting("',' or FROM", keyword("FROM"))(rest)?;
    let (rest, from) = ident("table name")(rest)?;
    let (rest, limit) = opt(limit_clause).parse(rest)?;

    let trailing = rest.first();
    if trailing.tok != Tok::Eof {
        let what = if limit.is_none() {
            "LIMIT or end of input"
        } else {
            "end of input"
        };
        return Err(nom::Err::Error(PError::expected(trailing, what)));
    }

    Ok(Query { items, from, limit })
}
