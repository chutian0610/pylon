//! 语法层验收：SQL 文本 → AST，错误必须带位置（issue #2 验收项 4）。

use pylon_sql::{Ident, Limit, Query, SelectItem, Span, SyntaxError, parse};

fn ident(name: &str, start: usize) -> Ident {
    Ident {
        name: name.into(),
        span: Span::new(start, start + name.len()),
    }
}

fn column(name: &str, start: usize) -> SelectItem {
    SelectItem::Column(ident(name, start))
}

#[test]
fn parses_projection_with_limit() {
    let q = parse("SELECT a, b FROM t LIMIT 10").unwrap();
    assert_eq!(
        q,
        Query {
            items: vec![column("a", 7), column("b", 10)],
            from: ident("t", 17),
            limit: Some(Limit {
                n: 10,
                span: Span::new(25, 27)
            }),
        }
    );
}

#[test]
fn keywords_case_insensitive_and_limit_optional() {
    let q = parse("select * from T").unwrap();
    assert_eq!(q.items, vec![SelectItem::Wildcard(Span::new(7, 8))]);
    assert_eq!(q.from, ident("T", 14));
    assert_eq!(q.limit, None);
}

#[test]
fn quoted_identifiers_keep_case_and_admit_keywords() {
    let q = parse(r#"SELECT "From", "a""b" FROM "My Table""#).unwrap();
    // 引号标识符的 span 覆盖含引号的完整 token 文本
    assert_eq!(
        q.items,
        vec![
            SelectItem::Column(Ident {
                name: "From".into(),
                span: Span::new(7, 13)
            }),
            SelectItem::Column(Ident {
                name: "a\"b".into(),
                span: Span::new(15, 21)
            }),
        ]
    );
    assert_eq!(
        q.from,
        Ident {
            name: "My Table".into(),
            span: Span::new(27, 37)
        }
    );
}

#[test]
fn line_comments_are_skipped() {
    let q = parse("-- lead\nSELECT a -- trailing\nFROM t").unwrap();
    assert_eq!(q.items, vec![column("a", 15)]);
    assert_eq!(q.from, ident("t", 34));
}

// ---- 错误路径：位置与消息都参与验收 ----

fn err(sql: &str) -> SyntaxError {
    parse(sql).unwrap_err()
}

#[test]
fn error_missing_select() {
    let e = err("SELEC a FROM t");
    assert_eq!(e.span, Span::new(0, 5));
    assert_eq!(e.message, "expected SELECT, found 'SELEC'");
    assert_eq!(
        e.position("SELEC a FROM t"),
        pylon_sql::LineCol { line: 1, column: 1 }
    );
}

#[test]
fn error_missing_from_reports_comma_or_from() {
    let e = err("SELECT a b FROM t");
    assert_eq!(e.span, Span::new(9, 10));
    assert_eq!(e.message, "expected ',' or FROM, found 'b'");
}

#[test]
fn error_select_list_cannot_be_empty() {
    let e = err("SELECT FROM t");
    assert_eq!(e.span, Span::new(7, 11));
    assert_eq!(e.message, "expected column name or '*', found 'FROM'");
}

#[test]
fn error_missing_table_name() {
    let e = err("SELECT a FROM");
    assert_eq!(e.span, Span::new(13, 13));
    assert_eq!(e.message, "expected table name, found end of input");
}

#[test]
fn error_limit_requires_integer() {
    let e = err("SELECT a FROM t LIMIT x");
    assert_eq!(e.span, Span::new(22, 23));
    assert_eq!(
        e.message,
        "expected a non-negative integer after LIMIT, found 'x'"
    );

    let e = err("SELECT a FROM t LIMIT 1.5");
    assert_eq!(
        e.message,
        "LIMIT expects a non-negative integer, found '1.5'"
    );
}

#[test]
fn error_trailing_input() {
    let e = err("SELECT a FROM t LIMIT 10 extra");
    assert_eq!(e.span, Span::new(25, 30));
    assert_eq!(e.message, "expected end of input, found 'extra'");

    let e = err("SELECT a FROM t x");
    assert_eq!(e.message, "expected LIMIT or end of input, found 'x'");
}

#[test]
fn error_position_is_line_and_column_aware() {
    let e = err("SELECT a\nFROM t\nLIMIT ?");
    assert_eq!(e.message, "unexpected character '?'");
    let pos = e.position("SELECT a\nFROM t\nLIMIT ?");
    assert_eq!((pos.line, pos.column), (3, 7));

    let rendered = e.render("SELECT a\nFROM t\nLIMIT ?");
    assert!(rendered.contains("line 3, column 7"), "{rendered}");
    assert!(rendered.contains("LIMIT ?"), "{rendered}");
}

#[test]
fn error_empty_input() {
    let e = err("");
    assert_eq!(e.message, "expected SELECT, found end of input");
    assert_eq!(e.position(""), pylon_sql::LineCol { line: 1, column: 1 });
}

#[test]
fn render_points_at_offending_token() {
    let sql = "SELECT a, b LIMIT 10";
    let e = err(sql);
    assert_eq!(e.message, "expected ',' or FROM, found 'LIMIT'");
    let rendered = e.render(sql);
    assert!(
        rendered.starts_with("syntax error: expected ',' or FROM, found 'LIMIT'\n"),
        "{rendered}"
    );
    assert!(rendered.contains("line 1, column 13"), "{rendered}");
    assert!(
        rendered.ends_with("   | SELECT a, b LIMIT 10\n   |             ^^^^^"),
        "{rendered}"
    );
}
