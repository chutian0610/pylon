//! AST golden 快照：SQL 文本 → pretty 渲染，锁住每个 M1 语法构造的解析形态
//! （issue #3 验收项 1）。span 的正确性由 tests/parse.rs 定点断言，不进快照。
//!
//! 期望文件在 `tests/golden/<name>.txt`（首行为 SQL 原文注释）。改语法或改渲染后
//! 用 `UPDATE_GOLDENS=1 cargo test -p pylon-sql` 重新生成并人工核验 diff。

use std::fs;
use std::path::PathBuf;

use pylon_sql::{parse, pretty};

/// (名字, SQL)。名字即快照文件名；一个名字对应恰好一个语法构造（综合形态除外）。
const CASES: &[(&str, &str)] = &[
    ("projection_limit", "SELECT a, b FROM t LIMIT 10"),
    ("wildcard", "SELECT * FROM t"),
    ("wildcard_mixed", "SELECT *, a FROM t"),
    ("arithmetic", "SELECT a + b * c - d / e, (a + b) * c FROM t"),
    ("unary_signs", "SELECT -a, +2, -(a + b) FROM t"),
    (
        "comparison_family",
        "SELECT a = b, a <> c, a != d, a < e, a <= f, a > g, a >= h FROM t",
    ),
    (
        "logic_precedence",
        "SELECT a = 1 OR b = 2 AND NOT c = 3 FROM t",
    ),
    ("is_null", "SELECT a IS NULL, b IS NOT NULL FROM t"),
    ("between", "SELECT a FROM t WHERE a BETWEEN 1 AND 10"),
    (
        "not_between",
        "SELECT a FROM t WHERE a NOT BETWEEN 1 AND 10",
    ),
    (
        "date_interval_ops",
        "SELECT DATE '1998-12-01' - INTERVAL '90' DAY, DATE '2024-01-31' + INTERVAL '1' MONTH FROM t",
    ),
    (
        "where_logic",
        "SELECT a FROM t WHERE a > 5 AND b = 'x' OR c IS NULL",
    ),
    ("group_by_single", "SELECT a, COUNT(*) FROM t GROUP BY a"),
    ("group_by_multi", "SELECT a, b, SUM(c) FROM t GROUP BY a, b"),
    ("global_aggregate", "SELECT SUM(a * b) FROM t"),
    (
        "aggregates",
        "SELECT COUNT(a), SUM(a), MIN(a), MAX(a), AVG(a + b) FROM t",
    ),
    ("select_distinct", "SELECT DISTINCT a, b FROM t"),
    ("order_by_no_limit", "SELECT a FROM t ORDER BY a DESC"),
    (
        "order_by_multi_limit",
        "SELECT a, b FROM t ORDER BY a DESC, b ASC LIMIT 5",
    ),
    ("show_tables", "SHOW TABLES"),
    ("describe", "DESCRIBE t"),
    (
        "literals",
        "SELECT 'it''s', 0.10, 123456789012345678901234567890, true, false, NULL FROM t",
    ),
    (
        "quoted_and_comments",
        "SELECT \"My Col\" /* block */ FROM \"My Table\" -- tail",
    ),
    (
        "case_insensitive",
        "select distinct A from T order by A desc limit 3",
    ),
    ("trailing_semicolon", "SELECT a FROM t;"),
    // TPC-H Q1 去别名形态：M1 面的综合弹道（date/interval 运算、聚合参数表达式、
    // 多列 GROUP BY、无 LIMIT 的 ORDER BY）
    (
        "tpch_q1_shape",
        "SELECT l_returnflag, l_linestatus, SUM(l_quantity), \
         SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)), AVG(l_quantity), COUNT(*), \
         MIN(l_quantity), MAX(l_quantity) \
         FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY \
         GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
    ),
    // TPC-H Q6 去别名形态：全局聚合 + BETWEEN/比较混合谓词
    (
        "tpch_q6_shape",
        "SELECT SUM(l_extendedprice * l_quantity) FROM lineitem \
         WHERE l_shipdate > DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' \
         AND l_discount BETWEEN 0.05 - 0.01 AND 0.05 + 0.01 AND l_quantity < 24",
    ),
];

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn expected_text(sql: &str, rendered: &str) -> String {
    format!("-- {sql}\n{rendered}\n")
}

#[test]
fn golden_ast_snapshots() {
    let update = std::env::var_os("UPDATE_GOLDENS").is_some();
    let dir = golden_dir();
    fs::create_dir_all(&dir).unwrap();

    for (name, sql) in CASES {
        let stmt = parse(sql).unwrap_or_else(|e| panic!("{name}: 必须可解析，却得到 {e:?}"));
        let want = expected_text(sql, &pretty::render(&stmt));
        let path = dir.join(format!("{name}.txt"));

        if update || !path.exists() {
            fs::write(&path, &want).unwrap();
            continue;
        }
        let got = fs::read_to_string(&path).unwrap();
        assert!(
            got == want,
            "{name} 快照不符（改语法或渲染后用 UPDATE_GOLDENS=1 重生成并核验 diff）\n\
             ===== 期望 =====\n{want}\n===== 实际 =====\n{got}"
        );
    }

    if update {
        eprintln!("goldens updated: {} cases", CASES.len());
    }
}

/// 快照目录里不允许有失去用例的孤儿文件（防止改名后留下陈旧快照）。
#[test]
fn no_orphaned_golden_files() {
    let dir = golden_dir();
    if !dir.exists() {
        return;
    }
    let mut names: Vec<String> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    for name in &names {
        let stem = name.strip_suffix(".txt").unwrap_or(name);
        assert!(
            CASES.iter().any(|(n, _)| *n == stem),
            "tests/golden/{name} 没有对应用例，请删除或改名"
        );
    }
    assert_eq!(
        names.len(),
        CASES.len(),
        "快照文件数 {} 与用例数 {} 不一致",
        names.len(),
        CASES.len()
    );
}
