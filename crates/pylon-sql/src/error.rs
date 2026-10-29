//! 带位置的语法错误，以及可被其他层复用的源码定位渲染。

use std::fmt;

use crate::ast::Span;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxError {
    pub span: Span,
    pub message: String,
}

/// 1-based 行列号。列按字符计数（非字节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineCol {
    pub line: usize,
    pub column: usize,
}

impl SyntaxError {
    pub fn new(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            message: message.into(),
        }
    }

    pub fn position(&self, source: &str) -> LineCol {
        line_col(source, self.span.start)
    }

    /// 渲染为多行用户可见错误：首行消息，随后是行列与带脱字符的源码行。
    pub fn render(&self, source: &str) -> String {
        render_at(
            source,
            self.span,
            &format!("syntax error: {}", self.message),
        )
    }
}

impl fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "syntax error: {}", self.message)
    }
}

impl std::error::Error for SyntaxError {}

/// 把字节偏移换算为行列。偏移超出源文本长度时按文本末尾处理。
pub fn line_col(source: &str, offset: usize) -> LineCol {
    let offset = offset.min(source.len());
    let before = &source[..offset];
    let line = before.matches('\n').count() + 1;
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    let column = before[line_start..].chars().count() + 1;
    LineCol { line, column }
}

/// 以 `headline` 为首行，附上 `span` 所在源码行和下划线脱字符。
///
/// ```text
/// syntax error: expected FROM, found 'LIMIT'
///   --> line 1, column 13
///    | SELECT a, b LIMIT 10
///    |             ^^^^^
/// ```
pub fn render_at(source: &str, span: Span, headline: &str) -> String {
    let start = line_col(source, span.start);
    let end = line_col(source, span.end.max(span.start));
    let line_text = source.lines().nth(start.line - 1).unwrap_or("");
    let line_width = line_text.chars().count();

    let caret_width = if end.line == start.line {
        end.column.saturating_sub(start.column)
    } else {
        line_width.saturating_sub(start.column - 1)
    }
    .max(1);

    let mut out = String::new();
    out.push_str(headline);
    out.push('\n');
    out.push_str(&format!(
        "  --> line {}, column {}\n",
        start.line, start.column
    ));
    out.push_str(&format!("   | {line_text}\n"));
    out.push_str(&format!(
        "   | {}{}",
        " ".repeat(start.column - 1),
        "^".repeat(caret_width)
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_col_counts_lines_and_chars() {
        let src = "ab\ncdé\nf";
        assert_eq!(line_col(src, 0), LineCol { line: 1, column: 1 });
        assert_eq!(line_col(src, 3), LineCol { line: 2, column: 1 });
        // 'é' 占两个字节，列按字符数计
        assert_eq!(line_col(src, 7), LineCol { line: 2, column: 4 });
        assert_eq!(line_col(src, 8), LineCol { line: 3, column: 1 });
        assert_eq!(line_col(src, 999), LineCol { line: 3, column: 2 });
    }

    #[test]
    fn render_marks_span_width() {
        let src = "SELECT a, b LIMIT 10";
        let text = render_at(src, Span::new(12, 17), "oops");
        assert_eq!(
            text,
            "oops\n  --> line 1, column 13\n   | SELECT a, b LIMIT 10\n   |             ^^^^^"
        );
    }

    #[test]
    fn render_at_end_of_input_uses_single_caret() {
        let src = "SELECT a FROM";
        let text = render_at(src, Span::new(13, 13), "eof");
        assert!(
            text.ends_with("   | SELECT a FROM\n   |              ^"),
            "{text}"
        );
    }
}
