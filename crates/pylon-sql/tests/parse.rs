//! 语法层验收：SQL 文本 → AST，错误必须带位置（issue #3 验收项 4）。
//!
//! 这里做 AST 结构与 span 的定点断言；每个语法构造的整体形态由 tests/golden.rs 的
//! 快照覆盖。

use pylon_sql::{
    AggFunc, BinaryOp, Direction, Expr, Ident, IntervalField, Limit, OrderKey, Query, SelectItem,
    Span, Statement, SyntaxError, UnaryOp, parse,
};

fn ident(name: &str, start: usize) -> Ident {
    Ident {
        name: name.into(),
        span: Span::new(start, start + name.len()),
    }
}

fn column(name: &str, start: usize) -> Expr {
    Expr::Column(ident(name, start))
}

fn number(value: &str, start: usize) -> Expr {
    Expr::Number {
        value: value.into(),
        span: Span::new(start, start + value.len()),
    }
}

fn binary(left: Expr, op: BinaryOp, right: Expr, span: (usize, usize)) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
        span: Span::new(span.0, span.1),
    }
}

/// 断言 SQL 解析出的查询各子句形状（绕开整树构造）。
fn query(sql: &str) -> Query {
    match parse(sql).unwrap() {
        Statement::Query(q) => *q,
        other => panic!("expected a query, got {other:?}"),
    }
}

// ---- 基础：投影 / LIMIT / 大小写 / 注释 ----

#[test]
fn parses_projection_with_limit() {
    let q = query("SELECT a, b FROM t LIMIT 10");
    assert_eq!(
        q.items,
        vec![
            SelectItem::Expr(column("a", 7)),
            SelectItem::Expr(column("b", 10)),
        ]
    );
    assert_eq!(q.from, ident("t", 17));
    assert_eq!(
        q.limit,
        Some(Limit {
            n: 10,
            span: Span::new(25, 27)
        })
    );
    assert!(q.distinct.is_none());
    assert!(q.selection.is_none());
    assert!(q.group_by.is_empty());
    assert!(q.order_by.is_empty());
}

#[test]
fn keywords_case_insensitive_and_limit_optional() {
    let q = query("select * from T");
    assert_eq!(q.items, vec![SelectItem::Wildcard(Span::new(7, 8))]);
    assert_eq!(q.from, ident("T", 14));
    assert_eq!(q.limit, None);
}

#[test]
fn quoted_identifiers_keep_case_and_admit_keywords() {
    let q = query(r#"SELECT "From", "a""b" FROM "My Table""#);
    // 引号标识符的 span 覆盖含引号的完整 token 文本
    assert_eq!(
        q.items,
        vec![
            SelectItem::Expr(Expr::Column(Ident {
                name: "From".into(),
                span: Span::new(7, 13)
            })),
            SelectItem::Expr(Expr::Column(Ident {
                name: "a\"b".into(),
                span: Span::new(15, 21)
            })),
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
    let q = query("-- lead\nSELECT a -- trailing\nFROM t");
    assert_eq!(q.items, vec![SelectItem::Expr(column("a", 15))]);
    assert_eq!(q.from, ident("t", 34));
}

#[test]
fn trailing_semicolon_is_accepted() {
    let q = query("SELECT a FROM t;");
    assert_eq!(q.from, ident("t", 14));
}

// ---- 表达式：优先级与 span ----

#[test]
fn arithmetic_precedence_mul_binds_tighter_than_add() {
    let q = query("SELECT a + b * c - d FROM t");
    // a + (b*c) - d  ⇒  Sub( Add(a, Mul(b,c)), d )
    let mul = binary(column("b", 11), BinaryOp::Mul, column("c", 15), (11, 16));
    let add = binary(column("a", 7), BinaryOp::Add, mul, (7, 16));
    let sub = binary(add, BinaryOp::Sub, column("d", 19), (7, 20));
    assert_eq!(q.items, vec![SelectItem::Expr(sub)]);
}

#[test]
fn parentheses_override_precedence_without_new_node() {
    let q = query("SELECT (a + b) * c FROM t");
    // 括号不产生节点：Mul( Add(a,b), c )，Add 保留内部 span
    let add = binary(column("a", 8), BinaryOp::Add, column("b", 12), (8, 13));
    let mul = binary(add, BinaryOp::Mul, column("c", 17), (8, 18));
    assert_eq!(q.items, vec![SelectItem::Expr(mul)]);
}

#[test]
fn unary_minus_carries_sign_into_span() {
    let q = query("SELECT -a, -1 FROM t");
    assert_eq!(
        q.items,
        vec![
            SelectItem::Expr(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(column("a", 8)),
                span: Span::new(7, 9),
            }),
            SelectItem::Expr(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(number("1", 12)),
                span: Span::new(11, 13),
            }),
        ]
    );
}

#[test]
fn logic_precedence_and_binds_tighter_than_or() {
    let q = query("SELECT a = 1 OR b = 2 AND c = 3 FROM t");
    // Or( Eq(a,1), And(Eq(b,2), Eq(c,3)) )
    let eq1 = binary(column("a", 7), BinaryOp::Eq, number("1", 11), (7, 12));
    let eq2 = binary(column("b", 16), BinaryOp::Eq, number("2", 20), (16, 21));
    let eq3 = binary(column("c", 26), BinaryOp::Eq, number("3", 30), (26, 31));
    let and = binary(eq2, BinaryOp::And, eq3, (16, 31));
    let or = binary(eq1, BinaryOp::Or, and, (7, 31));
    assert_eq!(q.items, vec![SelectItem::Expr(or)]);
}

#[test]
fn not_takes_a_predicate_not_a_operand() {
    let q = query("SELECT NOT a = 1 AND b = 2 FROM t");
    // And( Not(Eq(a,1)), Eq(b,2) )：NOT 先于 AND、后于比较
    let eq1 = binary(column("a", 11), BinaryOp::Eq, number("1", 15), (11, 16));
    let not = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(eq1),
        span: Span::new(7, 16),
    };
    let eq2 = binary(column("b", 21), BinaryOp::Eq, number("2", 25), (21, 26));
    let and = binary(not, BinaryOp::And, eq2, (7, 26));
    assert_eq!(q.items, vec![SelectItem::Expr(and)]);
}

#[test]
fn is_null_and_is_not_null() {
    let q = query("SELECT a IS NULL, b IS NOT NULL FROM t");
    assert_eq!(
        q.items,
        vec![
            SelectItem::Expr(Expr::IsNull {
                expr: Box::new(column("a", 7)),
                negated: false,
                span: Span::new(7, 16),
            }),
            SelectItem::Expr(Expr::IsNull {
                expr: Box::new(column("b", 18)),
                negated: true,
                span: Span::new(18, 31),
            }),
        ]
    );
}

#[test]
fn between_spans_cover_bounds_and_and_belongs_to_between() {
    let q = query("SELECT a FROM t WHERE a BETWEEN 1 AND 10 AND b = 2");
    // WHERE: And( Between(a,1,10), Eq(b,2) )——BETWEEN 的 AND 优先吃掉边界
    let sel = Expr::Between {
        expr: Box::new(column("a", 22)),
        low: Box::new(number("1", 32)),
        high: Box::new(number("10", 38)),
        negated: false,
        span: Span::new(22, 40),
    };
    let eq = binary(column("b", 45), BinaryOp::Eq, number("2", 49), (45, 50));
    let and = binary(sel, BinaryOp::And, eq, (22, 50));
    assert_eq!(q.selection, Some(and));
}

#[test]
fn not_between_sets_negated() {
    let q = query("SELECT a FROM t WHERE a NOT BETWEEN 1 AND 10");
    let sel = q.selection.unwrap();
    assert!(matches!(
        &sel,
        Expr::Between {
            negated: true,
            low,
            high,
            ..
        } if **low == number("1", 36) && **high == number("10", 42)
    ));
    assert_eq!(sel.span(), Span::new(22, 44));
}

// ---- date 与 interval 字面量及运算 ----

#[test]
fn date_minus_interval_day() {
    let q = query("SELECT DATE '1998-12-01' - INTERVAL '90' DAY FROM t");
    let date = Expr::Date {
        value: "1998-12-01".into(),
        span: Span::new(7, 24),
    };
    let interval = Expr::Interval {
        value: "90".into(),
        unit: IntervalField::Day,
        span: Span::new(27, 44),
    };
    assert_eq!(
        q.items,
        vec![SelectItem::Expr(binary(
            date,
            BinaryOp::Sub,
            interval,
            (7, 44)
        ))]
    );
}

#[test]
fn compound_interval_is_outside_m1_face() {
    // YEAR TO MONTH 等复合区间不在 M1 面：单位后的 TO 按多余输入报错并带位置
    let e = err("SELECT INTERVAL '1-6' YEAR TO MONTH FROM t");
    assert_eq!(e.message, "expected ',' or FROM, found 'TO'");
    assert_eq!(e.span, Span::new(27, 29));
}

#[test]
fn string_and_bool_and_null_literals() {
    let q = query("SELECT 'it''s', true, false, NULL FROM t");
    assert_eq!(
        q.items,
        vec![
            SelectItem::Expr(Expr::String {
                value: "it's".into(),
                span: Span::new(7, 14),
            }),
            SelectItem::Expr(Expr::Bool {
                value: true,
                span: Span::new(16, 20),
            }),
            SelectItem::Expr(Expr::Bool {
                value: false,
                span: Span::new(22, 27),
            }),
            SelectItem::Expr(Expr::Null(Span::new(29, 33))),
        ]
    );
}

// ---- 聚合 ----

#[test]
fn count_star_and_aggregate_with_expression() {
    let q = query("SELECT COUNT(*), SUM(a * b) FROM t");
    assert_eq!(
        q.items[0],
        SelectItem::Expr(Expr::CountStar(Span::new(7, 15)))
    );
    let mul = binary(column("a", 21), BinaryOp::Mul, column("b", 25), (21, 26));
    assert_eq!(
        q.items[1],
        SelectItem::Expr(Expr::Aggregate {
            func: AggFunc::Sum,
            arg: Box::new(mul),
            span: Span::new(17, 27),
        })
    );
}

#[test]
fn aggregate_distinct_is_outside_m1_face() {
    // DISTINCT 只作 SELECT DISTINCT（父 spec：以全列 GROUP BY 实现），聚合内不接受
    let e = err("SELECT COUNT(DISTINCT a) FROM t");
    assert_eq!(
        e.message,
        "expected an aggregate argument, found 'DISTINCT'"
    );
    assert_eq!(e.span, Span::new(13, 21));
}

#[test]
fn statement_words_are_not_reserved_as_columns() {
    // SHOW/TABLES/DESCRIBE 只在语句开头按位置识别（Trino 亦不保留）
    let q = query("SELECT tables, show, describe FROM t");
    assert_eq!(
        q.items,
        vec![
            SelectItem::Expr(column("tables", 7)),
            SelectItem::Expr(column("show", 15)),
            SelectItem::Expr(column("describe", 21)),
        ]
    );
}

#[test]
fn aggregate_name_alone_is_a_column() {
    // 聚合名不跟 `(` 按普通列处理；ASC/DESC/单位词同理不作保留字
    let q = query("SELECT count, day, min FROM t");
    assert_eq!(
        q.items,
        vec![
            SelectItem::Expr(column("count", 7)),
            SelectItem::Expr(column("day", 14)),
            SelectItem::Expr(column("min", 19)),
        ]
    );
}

// ---- WHERE / GROUP BY / ORDER BY / DISTINCT ----

#[test]
fn where_group_by_order_by_limit_clause_shapes() {
    let q =
        query("SELECT a, SUM(b) FROM t WHERE c > 10 GROUP BY a, d ORDER BY a DESC, b ASC LIMIT 5");
    assert_eq!(
        q.selection,
        Some(binary(
            column("c", 30),
            BinaryOp::Gt,
            number("10", 34),
            (30, 36)
        ))
    );
    assert_eq!(q.group_by, vec![column("a", 46), column("d", 49)]);
    assert_eq!(
        q.order_by,
        vec![
            OrderKey {
                expr: column("a", 60),
                direction: Direction::Desc,
            },
            OrderKey {
                expr: column("b", 68),
                direction: Direction::Asc,
            },
        ]
    );
    assert_eq!(q.limit.map(|l| l.n), Some(5));
}

#[test]
fn order_by_defaults_to_asc() {
    let q = query("SELECT a FROM t ORDER BY a");
    assert_eq!(
        q.order_by,
        vec![OrderKey {
            expr: column("a", 25),
            direction: Direction::Asc,
        }]
    );
}

#[test]
fn select_distinct_records_span() {
    let q = query("SELECT DISTINCT a, b FROM t");
    assert_eq!(q.distinct, Some(Span::new(7, 15)));
}

// ---- SHOW TABLES / DESCRIBE ----

#[test]
fn show_tables_statement() {
    assert_eq!(
        parse("SHOW TABLES").unwrap(),
        Statement::ShowTables(Span::new(0, 11))
    );
    assert_eq!(
        parse("show tables").unwrap(),
        Statement::ShowTables(Span::new(0, 11))
    );
}

#[test]
fn describe_statement() {
    assert_eq!(
        parse("DESCRIBE t").unwrap(),
        Statement::Describe {
            table: ident("t", 9)
        }
    );
}

// ---- 错误路径：位置与消息都参与验收 ----

fn err(sql: &str) -> SyntaxError {
    parse(sql).unwrap_err()
}

#[test]
fn error_missing_select_lists_all_statement_forms() {
    let e = err("SELEC a FROM t");
    assert_eq!(e.span, Span::new(0, 5));
    assert_eq!(
        e.message,
        "expected SELECT, SHOW TABLES, or DESCRIBE, found 'SELEC'"
    );
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
    assert_eq!(e.message, "expected an expression or '*', found 'FROM'");
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
fn error_trailing_input_lists_remaining_clauses() {
    let e = err("SELECT a FROM t x");
    assert_eq!(e.span, Span::new(16, 17));
    assert_eq!(
        e.message,
        "expected WHERE, GROUP BY, ORDER BY, LIMIT, or end of input, found 'x'"
    );

    // 全部子句已消费后期望收缩为 end of input
    let e = err("SELECT a FROM t LIMIT 10 extra");
    assert_eq!(e.message, "expected end of input, found 'extra'");

    let e = err("SELECT a FROM t WHERE a = 1 x");
    assert_eq!(
        e.message,
        "expected GROUP BY, ORDER BY, LIMIT, or end of input, found 'x'"
    );
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
    assert_eq!(
        e.message,
        "expected SELECT, SHOW TABLES, or DESCRIBE, found end of input"
    );
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

// ---- 新增构造的错误路径 ----

#[test]
fn error_show_and_describe_shapes() {
    let e = err("SHOW");
    assert_eq!(e.message, "expected TABLES after SHOW, found end of input");

    let e = err("SHOW ME");
    assert_eq!(e.message, "expected TABLES after SHOW, found 'ME'");

    let e = err("DESCRIBE");
    assert_eq!(
        e.message,
        "expected table name after DESCRIBE, found end of input"
    );
}

#[test]
fn error_where_requires_expression() {
    let e = err("SELECT a FROM t WHERE");
    assert_eq!(e.span, Span::new(21, 21));
    assert_eq!(
        e.message,
        "expected an expression after WHERE, found end of input"
    );
}

#[test]
fn error_operator_requires_right_operand() {
    let e = err("SELECT a + FROM t");
    assert_eq!(e.span, Span::new(11, 15));
    assert_eq!(
        e.message,
        "expected an expression after the operator, found 'FROM'"
    );
}

#[test]
fn error_between_requires_and_between_bounds() {
    let e = err("SELECT a FROM t WHERE a BETWEEN 1");
    assert_eq!(
        e.message,
        "expected AND between the BETWEEN bounds, found end of input"
    );

    let e = err("SELECT a FROM t WHERE a NOT 5");
    assert_eq!(e.message, "expected BETWEEN after NOT, found '5'");
}

#[test]
fn error_paren_requires_closing() {
    let e = err("SELECT (a FROM t");
    assert_eq!(e.message, "expected ')', found 'FROM'");
}

#[test]
fn error_date_and_interval_literals() {
    let e = err("SELECT DATE 5 FROM t");
    assert_eq!(e.message, "expected a string after DATE, found '5'");

    let e = err("SELECT INTERVAL 5 DAY FROM t");
    assert_eq!(e.message, "expected a string after INTERVAL, found '5'");

    let e = err("SELECT INTERVAL '1' banana FROM t");
    assert_eq!(
        e.message,
        "expected an interval unit (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND), found 'banana'"
    );
}

#[test]
fn error_aggregate_parentheses() {
    let e = err("SELECT SUM(a FROM t");
    assert_eq!(
        e.message,
        "expected ')' after the aggregate argument, found 'FROM'"
    );

    let e = err("SELECT COUNT(* FROM t");
    assert_eq!(e.message, "expected ')' after COUNT(*), found 'FROM'");
}

#[test]
fn error_group_by_requires_by() {
    let e = err("SELECT a FROM t GROUP a");
    assert_eq!(e.message, "expected BY after GROUP, found 'a'");
}

#[test]
fn reserved_keyword_cannot_be_a_column() {
    let e = err("SELECT from FROM t");
    assert_eq!(e.message, "expected an expression or '*', found 'from'");
}
