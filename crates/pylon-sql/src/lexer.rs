//! 手写 lexer：SQL 文本 → 带位置 token 流。
//!
//! 末尾恒有 [`Tok::Eof`] 哨兵（span 指向文本末尾），语法层因此永远能拿到出错位置。
//! 数字以原文保存（不转 f64），精度与形态（整数/小数）留给 planner 判型。

use crate::ast::Span;
use crate::error::SyntaxError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tok {
    /// 未加引号的词：标识符或关键字，由语法层按位置判定
    Word(String),
    /// 双引号包裹的标识符（已去引号、`""` 还原为 `"`）
    QuotedIdent(String),
    /// 单引号字符串字面量（已去引号、`''` 还原为 `'`）
    Str(String),
    /// 数字字面量原文（整数或 `整数.小数`，不做任何数值转换）
    Number(String),
    Comma,
    Star,
    LParen,
    RParen,
    Plus,
    Minus,
    Slash,
    Eq,
    /// `<>` 或 `!=`
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Semi,
    Eof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub tok: Tok,
    pub span: Span,
}

impl Token {
    /// 错误消息用的自描述。
    pub fn describe(&self) -> String {
        match &self.tok {
            Tok::Word(w) => format!("'{w}'"),
            Tok::QuotedIdent(q) => format!("identifier \"{q}\""),
            Tok::Str(s) => format!("string '{s}'"),
            Tok::Number(n) => format!("'{n}'"),
            Tok::Comma => "','".into(),
            Tok::Star => "'*'".into(),
            Tok::LParen => "'('".into(),
            Tok::RParen => "')'".into(),
            Tok::Plus => "'+'".into(),
            Tok::Minus => "'-'".into(),
            Tok::Slash => "'/'".into(),
            Tok::Eq => "'='".into(),
            Tok::NotEq => "'<>'".into(),
            Tok::Lt => "'<'".into(),
            Tok::LtEq => "'<='".into(),
            Tok::Gt => "'>'".into(),
            Tok::GtEq => "'>='".into(),
            Tok::Semi => "';'".into(),
            Tok::Eof => "end of input".into(),
        }
    }
}

pub fn tokenize(source: &str) -> Result<Vec<Token>, SyntaxError> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            // `--` 行注释
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = source[i..]
                    .find('\n')
                    .map_or(bytes.len(), |rel| i + rel + 1);
            }
            // `/* … */` 块注释（不嵌套）
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let Some(rel) = source[i + 2..].find("*/") else {
                    return Err(SyntaxError::new(
                        Span::new(i, bytes.len()),
                        "unterminated block comment",
                    ));
                };
                i = i + 2 + rel + 2;
            }
            b'\'' => {
                let (value, next) = lex_string(source, i)?;
                tokens.push(Token {
                    tok: Tok::Str(value),
                    span: Span::new(i, next),
                });
                i = next;
            }
            b'"' => {
                let (ident, next) = lex_quoted_ident(source, i)?;
                tokens.push(Token {
                    tok: Tok::QuotedIdent(ident),
                    span: Span::new(i, next),
                });
                i = next;
            }
            b',' => i = push1(&mut tokens, Tok::Comma, i),
            b'*' => i = push1(&mut tokens, Tok::Star, i),
            b'(' => i = push1(&mut tokens, Tok::LParen, i),
            b')' => i = push1(&mut tokens, Tok::RParen, i),
            b'+' => i = push1(&mut tokens, Tok::Plus, i),
            b'-' => i = push1(&mut tokens, Tok::Minus, i),
            b'/' => i = push1(&mut tokens, Tok::Slash, i),
            b';' => i = push1(&mut tokens, Tok::Semi, i),
            b'=' => i = push1(&mut tokens, Tok::Eq, i),
            b'<' => match bytes.get(i + 1) {
                Some(b'>') => i = push(&mut tokens, Tok::NotEq, i, 2),
                Some(b'=') => i = push(&mut tokens, Tok::LtEq, i, 2),
                _ => i = push1(&mut tokens, Tok::Lt, i),
            },
            b'>' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    i = push(&mut tokens, Tok::GtEq, i, 2);
                } else {
                    i = push1(&mut tokens, Tok::Gt, i);
                }
            }
            b'!' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    i = push(&mut tokens, Tok::NotEq, i, 2);
                } else {
                    return Err(unexpected_char(source, i));
                }
            }
            b'0'..=b'9' => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                // 小数部分：点后紧跟数字才消费，孤立点号留给“意外字符”报错
                if j < bytes.len()
                    && bytes[j] == b'.'
                    && bytes.get(j + 1).is_some_and(u8::is_ascii_digit)
                {
                    j += 1;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                }
                tokens.push(Token {
                    tok: Tok::Number(source[i..j].to_string()),
                    span: Span::new(i, j),
                });
                i = j;
            }
            c if c == b'_' || c.is_ascii_alphabetic() => {
                let mut j = i + 1;
                while j < bytes.len() && (bytes[j] == b'_' || bytes[j].is_ascii_alphanumeric()) {
                    j += 1;
                }
                tokens.push(Token {
                    tok: Tok::Word(source[i..j].to_string()),
                    span: Span::new(i, j),
                });
                i = j;
            }
            _ => return Err(unexpected_char(source, i)),
        }
    }

    tokens.push(Token {
        tok: Tok::Eof,
        span: Span::new(bytes.len(), bytes.len()),
    });
    Ok(tokens)
}

/// 推入一个定长 token，返回其后的偏移（调用方必须用它推进游标）。
fn push1(tokens: &mut Vec<Token>, tok: Tok, start: usize) -> usize {
    push(tokens, tok, start, 1)
}

fn push(tokens: &mut Vec<Token>, tok: Tok, start: usize, len: usize) -> usize {
    tokens.push(Token {
        tok,
        span: Span::new(start, start + len),
    });
    start + len
}

fn unexpected_char(source: &str, i: usize) -> SyntaxError {
    let ch = source[i..].chars().next().expect("i is at a char boundary");
    SyntaxError::new(
        Span::new(i, i + ch.len_utf8()),
        format!("unexpected character '{ch}'"),
    )
}

/// 从 `source[start]` 的开引号起解析字符串字面量，返回 (内容, 结束偏移)。
/// `''` 转义为单个 `'`；未闭合报错。内容可含换行。
fn lex_string(source: &str, start: usize) -> Result<(String, usize), SyntaxError> {
    lex_delimited(source, start, '\'', "string literal")
}

/// 从 `source[start]` 的开引号起解析引号标识符，返回 (内容, 结束偏移)。
/// `""` 转义为单个 `"`；未闭合或空内容报错。
fn lex_quoted_ident(source: &str, start: usize) -> Result<(String, usize), SyntaxError> {
    let (name, end) = lex_delimited(source, start, '"', "quoted identifier")?;
    if name.is_empty() {
        return Err(SyntaxError::new(
            Span::new(start, end),
            "zero-length quoted identifier",
        ));
    }
    Ok((name, end))
}

/// 以 `quote` 为界的字面量：两个连续 `quote` 还原为一个；未闭合报
/// "unterminated {what}"，span 从开引号到文本末尾。
fn lex_delimited(
    source: &str,
    start: usize,
    quote: char,
    what: &str,
) -> Result<(String, usize), SyntaxError> {
    let mut value = String::new();
    let mut i = start + quote.len_utf8();
    loop {
        let rest = &source[i..];
        let Some(ch) = rest.chars().next() else {
            return Err(SyntaxError::new(
                Span::new(start, source.len()),
                format!("unterminated {what}"),
            ));
        };
        if ch == quote {
            if rest[ch.len_utf8()..].starts_with(quote) {
                value.push(quote);
                i += 2 * quote.len_utf8();
            } else {
                i += quote.len_utf8();
                break;
            }
        } else {
            value.push(ch);
            i += ch.len_utf8();
        }
    }
    Ok((value, i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn word_number_punctuation() {
        let toks = tokenize("SELECT a1, 42 FROM t").unwrap();
        let kinds: Vec<&Tok> = toks.iter().map(|t| &t.tok).collect();
        assert_eq!(
            kinds,
            vec![
                &Tok::Word("SELECT".into()),
                &Tok::Word("a1".into()),
                &Tok::Comma,
                &Tok::Number("42".into()),
                &Tok::Word("FROM".into()),
                &Tok::Word("t".into()),
                &Tok::Eof,
            ]
        );
    }

    #[test]
    fn decimal_number_takes_fraction() {
        let toks = tokenize("1.5 10").unwrap();
        assert_eq!(toks[0].tok, Tok::Number("1.5".into()));
        assert_eq!(toks[1].tok, Tok::Number("10".into()));
    }

    #[test]
    fn number_text_is_preserved_verbatim() {
        // 精度与形态不做任何规整：长整数不丢位，尾零保留
        let toks = tokenize("123456789012345678901234567890.098765432100 0.10 0.0").unwrap();
        assert_eq!(
            toks[0].tok,
            Tok::Number("123456789012345678901234567890.098765432100".into())
        );
        assert_eq!(toks[1].tok, Tok::Number("0.10".into()));
        assert_eq!(toks[2].tok, Tok::Number("0.0".into()));
    }

    #[test]
    fn dangling_dot_is_unexpected_character() {
        let err = tokenize("10.").unwrap_err();
        assert_eq!(err.span, Span::new(2, 3));
        assert_eq!(err.message, "unexpected character '.'");

        let err = tokenize("SELECT .5 FROM t").unwrap_err();
        assert_eq!(err.message, "unexpected character '.'");
    }

    #[test]
    fn string_literal_unescapes_doubled_quote() {
        let toks = tokenize("'it''s', '', 'a\\b'").unwrap();
        assert_eq!(toks[0].tok, Tok::Str("it's".into()));
        assert_eq!(toks[1].tok, Tok::Comma);
        assert_eq!(toks[2].tok, Tok::Str("".into()));
        assert_eq!(toks[3].tok, Tok::Comma);
        // 反斜杠不是转义字符，按原字符保留
        assert_eq!(toks[4].tok, Tok::Str("a\\b".into()));
    }

    #[test]
    fn string_literal_can_span_lines() {
        let toks = tokenize("'a\nb'").unwrap();
        assert_eq!(toks[0].tok, Tok::Str("a\nb".into()));
        assert_eq!(toks[0].span, Span::new(0, 5));
    }

    #[test]
    fn unterminated_string_literal() {
        let err = tokenize("SELECT 'abc").unwrap_err();
        assert_eq!(err.message, "unterminated string literal");
        assert_eq!(err.span, Span::new(7, 11));
    }

    #[test]
    fn comparison_operators() {
        let toks = tokenize("= <> != < <= > >=").unwrap();
        let kinds: Vec<&Tok> = toks.iter().map(|t| &t.tok).collect();
        assert_eq!(
            kinds[..6],
            [
                &Tok::Eq,
                &Tok::NotEq,
                &Tok::NotEq,
                &Tok::Lt,
                &Tok::LtEq,
                &Tok::Gt,
            ]
        );
        assert_eq!(kinds[6], &Tok::GtEq);
    }

    #[test]
    fn two_char_operator_spans_cover_both_chars() {
        let toks = tokenize("a <= 10").unwrap();
        assert_eq!(toks[1].span, Span::new(2, 4));
    }

    #[test]
    fn bang_alone_is_unexpected() {
        let err = tokenize("SELECT a ! b").unwrap_err();
        assert_eq!(err.span, Span::new(9, 10));
        assert_eq!(err.message, "unexpected character '!'");
    }

    #[test]
    fn parens_arithmetic_and_semicolon() {
        let toks = tokenize("(a + b) * -c / 2;").unwrap();
        let kinds: Vec<&Tok> = toks.iter().map(|t| &t.tok).collect();
        assert_eq!(
            kinds,
            vec![
                &Tok::LParen,
                &Tok::Word("a".into()),
                &Tok::Plus,
                &Tok::Word("b".into()),
                &Tok::RParen,
                &Tok::Star,
                &Tok::Minus,
                &Tok::Word("c".into()),
                &Tok::Slash,
                &Tok::Number("2".into()),
                &Tok::Semi,
                &Tok::Eof,
            ]
        );
    }

    #[test]
    fn minus_minus_is_a_comment_not_two_operators() {
        let toks = tokenize("a -- b\n+ c").unwrap();
        let kinds: Vec<&Tok> = toks.iter().map(|t| &t.tok).collect();
        assert_eq!(
            kinds,
            vec![
                &Tok::Word("a".into()),
                &Tok::Plus,
                &Tok::Word("c".into()),
                &Tok::Eof
            ]
        );
    }

    #[test]
    fn quoted_identifier_unescapes() {
        let toks = tokenize(r#"SELECT "a""b", "From" FROM t"#).unwrap();
        assert_eq!(toks[1].tok, Tok::QuotedIdent(r#"a"b"#.into()));
        assert_eq!(toks[3].tok, Tok::QuotedIdent("From".into()));
    }

    #[test]
    fn line_comment_skipped() {
        let toks = tokenize("SELECT -- comment\n a FROM t").unwrap();
        assert_eq!(toks[1].tok, Tok::Word("a".into()));
    }

    #[test]
    fn block_comment_skipped_even_across_lines() {
        let toks = tokenize("SELECT /* a\nmulti\nline */ a, /* x*y */ b FROM t").unwrap();
        let words: Vec<&Tok> = toks.iter().map(|t| &t.tok).collect();
        assert_eq!(
            words,
            vec![
                &Tok::Word("SELECT".into()),
                &Tok::Word("a".into()),
                &Tok::Comma,
                &Tok::Word("b".into()),
                &Tok::Word("FROM".into()),
                &Tok::Word("t".into()),
                &Tok::Eof,
            ]
        );
    }

    #[test]
    fn unterminated_block_comment() {
        let err = tokenize("SELECT a /* oops").unwrap_err();
        assert_eq!(err.message, "unterminated block comment");
        assert_eq!(err.span, Span::new(9, 16));
    }

    #[test]
    fn eof_sentinel_points_at_end() {
        let toks = tokenize("").unwrap();
        assert_eq!(
            toks,
            vec![Token {
                tok: Tok::Eof,
                span: Span::new(0, 0)
            }]
        );

        let toks = tokenize("t\n").unwrap();
        let last = toks.last().unwrap();
        assert_eq!(last.tok, Tok::Eof);
        assert_eq!(last.span, Span::new(2, 2));
    }

    #[test]
    fn unexpected_character_has_position() {
        let err = tokenize("SELECT a FROM t#").unwrap_err();
        assert_eq!(err.span, Span::new(15, 16));
        assert_eq!(err.message, "unexpected character '#'");
    }

    #[test]
    fn unterminated_quoted_identifier() {
        let err = tokenize("SELECT \"abc").unwrap_err();
        assert_eq!(err.message, "unterminated quoted identifier");
    }
}
