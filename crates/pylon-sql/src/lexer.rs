//! 手写 lexer：SQL 文本 → 带位置 token 流。
//!
//! 末尾恒有 [`Tok::Eof`] 哨兵（span 指向文本末尾），语法层因此永远能拿到出错位置。

use crate::ast::Span;
use crate::error::SyntaxError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tok {
    /// 未加引号的词：标识符或关键字，由语法层按位置判定
    Word(String),
    /// 双引号包裹的标识符（已去引号、`""` 还原为 `"`）
    QuotedIdent(String),
    /// 数字字面量原文
    Number(String),
    Comma,
    Star,
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
            Tok::Number(n) => format!("'{n}'"),
            Tok::Comma => "','".into(),
            Tok::Star => "'*'".into(),
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
            b',' => {
                tokens.push(Token {
                    tok: Tok::Comma,
                    span: Span::new(i, i + 1),
                });
                i += 1;
            }
            b'*' => {
                tokens.push(Token {
                    tok: Tok::Star,
                    span: Span::new(i, i + 1),
                });
                i += 1;
            }
            b'"' => {
                let (ident, next) = lex_quoted_ident(source, i)?;
                tokens.push(Token {
                    tok: Tok::QuotedIdent(ident),
                    span: Span::new(i, next),
                });
                i = next;
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
            _ => {
                let ch = source[i..].chars().next().expect("i is at a char boundary");
                return Err(SyntaxError::new(
                    Span::new(i, i + ch.len_utf8()),
                    format!("unexpected character '{ch}'"),
                ));
            }
        }
    }

    tokens.push(Token {
        tok: Tok::Eof,
        span: Span::new(bytes.len(), bytes.len()),
    });
    Ok(tokens)
}

/// 从 `source[start]` 的开引号起解析引号标识符，返回 (内容, 结束偏移)。
/// `""` 转义为单个 `"`；未闭合报错。
fn lex_quoted_ident(source: &str, start: usize) -> Result<(String, usize), SyntaxError> {
    let mut name = String::new();
    let mut i = start + 1;
    loop {
        let rest = &source[i..];
        let Some(ch) = rest.chars().next() else {
            return Err(SyntaxError::new(
                Span::new(start, source.len()),
                "unterminated quoted identifier",
            ));
        };
        if ch == '"' {
            if rest.starts_with("\"\"") {
                name.push('"');
                i += 2;
            } else {
                i += 1;
                break;
            }
        } else {
            name.push(ch);
            i += ch.len_utf8();
        }
    }
    if name.is_empty() {
        return Err(SyntaxError::new(
            Span::new(start, i),
            "zero-length quoted identifier",
        ));
    }
    Ok((name, i))
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
    fn dangling_dot_is_unexpected_character() {
        let err = tokenize("10.").unwrap_err();
        assert_eq!(err.span, Span::new(2, 3));
        assert_eq!(err.message, "unexpected character '.'");
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
