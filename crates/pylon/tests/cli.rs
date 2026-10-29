//! 规格级接缝的 e2e 验收：SQL 文本进 → 表格/Arrow 出，或带位置的用户可见错误。
//!
//! 含 DuckDB 对拍（issue #2 验收项 1）：PATH 上有 `duckdb` 时读同一目录执行等价 SQL，
//! 期望结果零手写；没有时跳过并打印提示（设 `PYLON_REQUIRE_DUCKDB=1` 可改为失败）。

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use arrow::array::AsArray;
use arrow::compute::concat_batches;
use arrow::datatypes::{Float64Type, Int64Type};
use pylon::run_query;
use pylon_table::testing::{FixtureSpec, expected_rows, fixture_row, write_fixture_table};

/// 2 文件 × 5 行 = 10 行：`LIMIT 10` 恰好覆盖全表，结果集与 oracle 可逐行比较。
const SPEC: FixtureSpec = FixtureSpec {
    files: 2,
    rows_per_file: 5,
};

fn fixture(spec: FixtureSpec) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    write_fixture_table(dir.path(), "t", spec).unwrap();
    dir
}

fn pylon(data_dir: &Path, sql: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pylon"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg(sql)
        .output()
        .unwrap()
}

fn stdout(out: &Output) -> String {
    String::from_utf8(out.stdout.clone()).unwrap()
}

fn stderr(out: &Output) -> String {
    String::from_utf8(out.stderr.clone()).unwrap()
}

/// 解析 pylon 的表格输出为 (表头, 数据行)。
fn parse_table(text: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines.len() >= 3, "not a table:\n{text}");
    let header = split_cells(lines[0]);
    let rows = lines[2..lines.len() - 1]
        .iter()
        .map(|l| split_cells(l))
        .collect();
    (header, rows)
}

fn split_cells(line: &str) -> Vec<String> {
    line.split('|').map(|c| c.trim().to_string()).collect()
}

fn assert_user_error(out: &Output, fragments: &[&str]) {
    assert_eq!(out.status.code(), Some(1), "stderr:\n{}", stderr(out));
    let err = stderr(out);
    for fragment in fragments {
        assert!(err.contains(fragment), "missing {fragment:?} in:\n{err}");
    }
    assert!(!err.contains("panicked"), "process panicked:\n{err}");
    assert!(stdout(out).is_empty());
}

// ---- 正常路径 ----

#[test]
fn select_with_limit_prints_table_matching_fixture() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "SELECT a, b FROM t LIMIT 10");
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    let (header, rows) = parse_table(&text);
    assert_eq!(header, ["a", "b"]);
    assert_eq!(rows.len(), 10);
    assert!(text.trim_end().ends_with("(10 rows)"), "{text}");

    let mut got: Vec<(i64, String)> = rows
        .iter()
        .map(|r| (r[0].parse().unwrap(), r[1].clone()))
        .collect();
    got.sort();
    let mut want: Vec<(i64, String)> = expected_rows(SPEC)
        .into_iter()
        .map(|r| (r.a, r.b.unwrap_or_else(|| "NULL".into())))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn exact_rendering_of_small_result() {
    let dir = fixture(FixtureSpec {
        files: 1,
        rows_per_file: 2,
    });
    let out = pylon(dir.path(), "SELECT a, b, c FROM t");
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "  a | b  |    c\n\
         ----+----+------\n\
         \x20-7 | s0 |  0.0\n\
         \x20-4 | s1 | 0.25\n\
         (2 rows)\n"
    );
}

#[test]
fn null_is_rendered_as_null() {
    let dir = fixture(FixtureSpec {
        files: 1,
        rows_per_file: 7,
    });
    let out = pylon(dir.path(), "SELECT b FROM t");
    assert!(out.status.success(), "{}", stderr(&out));
    let (_, rows) = parse_table(&stdout(&out));
    assert!(rows.iter().any(|r| r == &["NULL"]), "{rows:?}");
}

#[test]
fn limit_zero_prints_empty_table() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "SELECT a FROM t LIMIT 0");
    assert!(out.status.success(), "{}", stderr(&out));
    assert_eq!(stdout(&out), " a\n---\n(0 rows)\n");
}

#[test]
fn data_dir_can_come_from_env() {
    let dir = fixture(SPEC);
    let out = Command::new(env!("CARGO_BIN_EXE_pylon"))
        .env("PYLON_DATA_DIR", dir.path())
        .arg("SELECT a FROM t LIMIT 3")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(stdout(&out).ends_with("(3 rows)\n"));
}

#[test]
fn library_seam_sql_in_arrow_out() {
    let dir = fixture(SPEC);
    let result = run_query("SELECT c, a FROM t LIMIT 10", dir.path()).unwrap();
    let names: Vec<&str> = result
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["c", "a"]);
    assert_eq!(result.num_rows(), 10);

    let batch = concat_batches(&result.schema, &result.batches).unwrap();
    let c = batch.column(0).as_primitive::<Float64Type>();
    let a = batch.column(1).as_primitive::<Int64Type>();
    for row in 0..batch.num_rows() {
        let g = ((a.value(row) + 7) / 3) as usize;
        assert_eq!(c.value(row), fixture_row(g).c);
    }
}

// ---- 错误路径：带位置、退出码 1、不 panic（验收项 4）----

#[test]
fn syntax_error_has_position() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "SELECT a, b LIMIT 10");
    assert_user_error(
        &out,
        &[
            "syntax error: expected ',' or FROM, found 'LIMIT'",
            "line 1, column 13",
            "   | SELECT a, b LIMIT 10\n   |             ^^^^^",
        ],
    );
}

#[test]
fn lexer_error_has_position() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "SELECT a FROM t LIMIT ?");
    assert_user_error(&out, &["unexpected character '?'", "line 1, column 23"]);
}

#[test]
fn multiline_sql_error_points_at_right_line() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "SELECT a\nFROM t\nLIMIT ten");
    assert_user_error(
        &out,
        &[
            "expected a non-negative integer after LIMIT, found 'ten'",
            "line 3, column 7",
            "   | LIMIT ten\n   |       ^^^",
        ],
    );
}

#[test]
fn unknown_table_is_a_user_error_with_position() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "SELECT a FROM nope");
    assert_user_error(&out, &["error: table not found: nope", "line 1, column 15"]);
}

#[test]
fn unknown_column_is_a_user_error_with_position() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "SELECT zz FROM t");
    assert_user_error(
        &out,
        &[
            "error: column not found: zz",
            "line 1, column 8",
            "   | SELECT zz FROM t\n   |        ^^",
        ],
    );
}

#[test]
fn missing_data_dir_is_a_user_error() {
    let out = pylon(Path::new("/definitely/not/here"), "SELECT a FROM t");
    assert_user_error(&out, &["table not found: t"]);
}

#[test]
fn empty_sql_is_a_user_error() {
    let dir = fixture(SPEC);
    let out = pylon(dir.path(), "");
    assert_user_error(
        &out,
        &["expected SELECT, found end of input", "line 1, column 1"],
    );
}

// ---- DuckDB 对拍（验收项 1）----

fn duckdb_binary() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("PYLON_DUCKDB") {
        return Some(PathBuf::from(explicit));
    }
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|p| p.join("duckdb"))
            .find(|p| p.is_file())
    })
}

/// 用 duckdb CLI 读同一目录执行 `sql`，返回无表头 CSV 的单元格（NULL 显示为 `NULL`）。
fn duckdb_rows(duckdb: &Path, sql: &str) -> Vec<Vec<String>> {
    let out = Command::new(duckdb)
        .args(["-csv", "-noheader", "-nullvalue", "NULL", "-c", sql])
        .output()
        .unwrap();
    assert!(out.status.success(), "duckdb failed: {}", stderr(&out));
    stdout(&out)
        .lines()
        .map(|l| l.split(',').map(|c| c.trim().to_string()).collect())
        .collect()
}

/// 小数在两边的文本形态可能不同（`0.0` vs `0`），统一用同一套 f64 格式化比较。
fn normalize(cell: &str) -> String {
    match cell.parse::<f64>() {
        Ok(v) if cell.contains('.') => format!("{v}"),
        _ => cell.to_string(),
    }
}

fn normalized_sorted(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|r| r.iter().map(|c| normalize(c)).collect())
        .collect();
    rows.sort();
    rows
}

#[test]
fn duckdb_oracle_agrees_on_same_parquet_directory() {
    let Some(duckdb) = duckdb_binary() else {
        if std::env::var_os("PYLON_REQUIRE_DUCKDB").is_some() {
            panic!("PYLON_REQUIRE_DUCKDB is set but duckdb was not found");
        }
        eprintln!("skipping DuckDB oracle: `duckdb` not on PATH (set PYLON_DUCKDB to point at it)");
        return;
    };

    let dir = fixture(SPEC);
    let glob = dir.path().join("t").join("*.parquet");
    let glob = glob.to_str().unwrap();

    let cases = [
        (
            "SELECT a, b FROM t LIMIT 10",
            format!("SELECT a, b FROM read_parquet('{glob}') LIMIT 10"),
        ),
        (
            "SELECT c, a, c FROM t LIMIT 10",
            format!("SELECT c, a, c FROM read_parquet('{glob}') LIMIT 10"),
        ),
        (
            "SELECT * FROM t",
            format!("SELECT * FROM read_parquet('{glob}')"),
        ),
        (
            "SELECT b FROM t LIMIT 10",
            format!("SELECT b FROM read_parquet('{glob}') LIMIT 10"),
        ),
    ];

    for (pylon_sql, duck_sql) in &cases {
        let out = pylon(dir.path(), pylon_sql);
        assert!(out.status.success(), "{pylon_sql}: {}", stderr(&out));
        let (_, ours) = parse_table(&stdout(&out));
        let theirs = duckdb_rows(&duckdb, duck_sql);
        assert_eq!(ours.len(), 10, "{pylon_sql}");
        assert_eq!(
            normalized_sorted(ours),
            normalized_sorted(theirs),
            "{pylon_sql}"
        );
    }
    eprintln!("DuckDB oracle: {} queries agreed", cases.len());
}
