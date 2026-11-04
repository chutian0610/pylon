//! 表达式层验收：类型强制、求值语义（NULL 三值逻辑/Kleene）、常量折叠、字面量解析。

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};

use pylon_expr::{
    BinaryOp, Expr, IntervalUnit, Scalar, date32_to_ymd, eval, fold, parse_interval, parse_number,
    ymd_to_date32,
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("f", DataType::Float64, false),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Date32, false),
    ]))
}

fn batch() -> RecordBatch {
    let schema = schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![0.5, 1.5, 2.5])),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("y")])),
            Arc::new(Date32Array::from(vec![0, 100, 365])),
        ],
    )
    .unwrap()
}

fn col(i: usize, name: &str, t: DataType) -> Expr {
    Expr::column(i, name, t)
}

fn a() -> Expr {
    col(0, "a", DataType::Int64)
}
fn f() -> Expr {
    col(1, "f", DataType::Float64)
}
fn s() -> Expr {
    col(2, "s", DataType::Utf8)
}

fn eval_bool(e: &Expr, b: &RecordBatch) -> Vec<Option<bool>> {
    let arr = eval(e, b).unwrap();
    let arr = arr
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            }
        })
        .collect()
}

fn eval_f64(e: &Expr, b: &RecordBatch) -> Vec<Option<f64>> {
    let arr = eval(e, b).unwrap();
    let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            }
        })
        .collect()
}

fn eval_i64(e: &Expr, b: &RecordBatch) -> Vec<Option<i64>> {
    let arr = eval(e, b).unwrap();
    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            }
        })
        .collect()
}

// ---- 字面量解析 ----

#[test]
fn number_literals_keep_precision() {
    assert_eq!(parse_number("42").unwrap(), Scalar::Int64(42));
    assert_eq!(parse_number("-7").unwrap(), Scalar::Int64(-7));
    assert_eq!(
        parse_number("0.05").unwrap(),
        Scalar::Decimal(5, 2, 2),
        "0.05 → Decimal(值=5, p=2, s=2)（前导零不计入精度）"
    );
    assert_eq!(parse_number("0.10").unwrap(), Scalar::Decimal(10, 2, 2));
    let big = parse_number("12345678901234567890").unwrap();
    assert_eq!(big, Scalar::Decimal(12345678901234567890, 20, 0));
    assert!(parse_number(&"9".repeat(39)).is_err());
}

#[test]
fn date_parsing_round_trips() {
    assert_eq!(date32_to_ymd(0), "1970-01-01");
    assert_eq!(date32_to_ymd(-1), "1969-12-31");
    for days in [-1000, 0, 1, 10592, 20000, 999999] {
        let ymd = date32_to_ymd(days);
        assert_eq!(ymd_to_date32(&ymd), Some(days), "{ymd}");
    }
    assert_eq!(ymd_to_date32("2024-02-29"), ymd_to_date32("2024-02-29"));
    assert!(ymd_to_date32("2023-02-29").is_none(), "2023 非闰年");
    assert!(ymd_to_date32("199-12-01").is_none());
    assert!(ymd_to_date32("2024-13-01").is_none());
}

#[test]
fn interval_parsing_normalizes_units() {
    assert_eq!(
        parse_interval("90", IntervalUnit::Day).unwrap(),
        Scalar::IntervalDayTime(arrow_buffer::IntervalDayTime::new(90, 0))
    );
    assert_eq!(
        parse_interval("3", IntervalUnit::Hour).unwrap(),
        Scalar::IntervalDayTime(arrow_buffer::IntervalDayTime::new(0, 3 * 3_600_000))
    );
    assert_eq!(
        parse_interval("-30", IntervalUnit::Day).unwrap(),
        Scalar::IntervalDayTime(arrow_buffer::IntervalDayTime::new(-30, 0))
    );
    match parse_interval("1", IntervalUnit::Year).unwrap() {
        Scalar::IntervalMonthDayNano(iv) => assert_eq!(iv.months, 12),
        other => panic!("{other:?}"),
    }
    assert!(parse_interval("1.5", IntervalUnit::Day).is_err());
    assert!(parse_interval("x", IntervalUnit::Day).is_err());
}

// ---- 构造器（类型裁决） ----

#[test]
fn int_plus_float_promotes_to_float() {
    let e = Expr::binary(BinaryOp::Add, a(), f()).unwrap();
    assert_eq!(e.data_type(), DataType::Float64);
    // 左侧被插入 Cast
    let Expr::Binary { left, .. } = &e else {
        panic!()
    };
    assert!(matches!(**left, Expr::Cast { .. }));
    assert_eq!(
        eval_f64(&e, &batch()),
        vec![Some(1.5), Some(3.5), Some(5.5)]
    );
}

#[test]
fn int_plus_decimal_is_decimal() {
    let dec = Expr::literal(Scalar::Decimal(5, 2, 2)); // 0.05
    let e = Expr::binary(BinaryOp::Add, a(), dec).unwrap();
    assert_eq!(
        e.data_type(),
        DataType::Decimal128(22, 2),
        "Hive：s=max(0,2)，p=2+max(19,1)+1"
    );
    let arr: ArrayRef = eval(&e, &batch()).unwrap();
    let arr = arr
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap();
    assert_eq!(arr.value(0).to_string(), "105"); // 1.05 = 105 * 10^-2
}

#[test]
fn incompatible_operand_types_are_rejected() {
    let err = Expr::binary(BinaryOp::Add, a(), s()).unwrap_err();
    assert!(err.contains("cannot apply + to Int64 and Utf8"), "{err}");
    let err = Expr::binary(BinaryOp::Eq, s(), a()).unwrap_err();
    assert!(err.contains("cannot compare Utf8 with Int64"), "{err}");
    let err = Expr::logical_not(a()).unwrap_err();
    assert!(err.contains("NOT expects BOOLEAN"), "{err}");
}

// ---- 求值语义 ----

#[test]
fn integer_arithmetic_and_comparison() {
    let b = batch();
    let add = Expr::binary(BinaryOp::Add, a(), Expr::literal(Scalar::Int64(10))).unwrap();
    assert_eq!(eval_i64(&add, &b), vec![Some(11), Some(12), Some(13)]);

    let gt = Expr::binary(BinaryOp::Gt, a(), Expr::literal(Scalar::Int64(1))).unwrap();
    assert_eq!(
        eval_bool(&gt, &b),
        vec![Some(false), Some(true), Some(true)]
    );

    let mul = Expr::binary(BinaryOp::Mul, a(), a()).unwrap();
    assert_eq!(eval_i64(&mul, &b), vec![Some(1), Some(4), Some(9)]);
}

#[test]
fn null_comparisons_stay_null() {
    let b = batch();
    // s[1] 为 NULL：NULL = 'x' → NULL
    let eq = Expr::binary(BinaryOp::Eq, s(), Expr::literal(Scalar::Utf8("x".into()))).unwrap();
    assert_eq!(eval_bool(&eq, &b), vec![Some(true), None, Some(false)]);
}

#[test]
fn kleene_logic_semantics() {
    let b = batch();
    // 用 s IS NULL 作为可空布尔：[false, true, false]
    let sn = Expr::is_null(s(), false);
    // (s IS NULL) AND TRUE → [false, true, false]
    let t = Expr::literal(Scalar::Boolean(true));
    let both = Expr::binary(BinaryOp::And, sn.clone(), t).unwrap();
    assert_eq!(
        eval_bool(&both, &b),
        vec![Some(false), Some(true), Some(false)]
    );

    // 三值逻辑：NULL AND FALSE = FALSE（Kleene，非 SQL 朴素 AND）
    let null_bool = Expr::Cast {
        expr: Box::new(Expr::literal(Scalar::Null(DataType::Null))),
        to: DataType::Boolean,
    };
    let kleene = Expr::binary(
        BinaryOp::And,
        null_bool.clone(),
        Expr::literal(Scalar::Boolean(false)),
    )
    .unwrap();
    assert_eq!(eval_bool(&kleene, &b), vec![Some(false); 3]);
    let null_or_true = Expr::binary(
        BinaryOp::Or,
        null_bool,
        Expr::literal(Scalar::Boolean(true)),
    )
    .unwrap();
    assert_eq!(eval_bool(&null_or_true, &b), vec![Some(true); 3]);
}

#[test]
fn not_null_is_null() {
    let b = batch();
    let sn = Expr::is_null(s(), false); // [false, true, false]
    let not = Expr::logical_not(sn).unwrap();
    assert_eq!(
        eval_bool(&not, &b),
        vec![Some(true), Some(false), Some(true)]
    );

    // 直接对可空布尔取 NOT：NULL 传播
    let null_bool = Expr::literal(Scalar::Null(DataType::Boolean));
    let not_null = Expr::logical_not(null_bool).unwrap();
    assert_eq!(eval_bool(&not_null, &b), vec![None; 3]);
}

#[test]
fn is_null_predicates() {
    let b = batch();
    assert_eq!(
        eval_bool(&Expr::is_null(s(), false), &b),
        vec![Some(false), Some(true), Some(false)]
    );
    assert_eq!(
        eval_bool(&Expr::is_null(s(), true), &b),
        vec![Some(true), Some(false), Some(true)]
    );
}

#[test]
fn date_column_plus_interval() {
    let b = batch();
    let iv = Expr::literal(parse_interval("30", IntervalUnit::Day).unwrap());
    let e = Expr::binary(BinaryOp::Add, col(3, "d", DataType::Date32), iv).unwrap();
    assert_eq!(e.data_type(), DataType::Date32);
    let arr = eval(&e, &b).unwrap();
    let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(arr.value(0), 30);
    assert_eq!(arr.value(2), 395);

    // interval + date 交换律在构造器归一
    let iv2 = Expr::literal(parse_interval("30", IntervalUnit::Day).unwrap());
    let swapped = Expr::binary(BinaryOp::Add, iv2, col(3, "d", DataType::Date32)).unwrap();
    assert_eq!(
        eval(&swapped, &b).unwrap().as_ref(),
        eval(&e, &b).unwrap().as_ref()
    );
}

// ---- 常量折叠 ----

fn folded_literal(e: &Expr) -> Scalar {
    match fold(e) {
        Expr::Literal { value, .. } => value,
        other => panic!("not folded: {other:?}"),
    }
}

#[test]
fn arithmetic_constants_fold() {
    let one = Expr::literal(Scalar::Int64(1));
    let two = Expr::literal(Scalar::Int64(2));
    let three = Expr::literal(Scalar::Int64(3));
    let e = Expr::binary(
        BinaryOp::Add,
        one,
        Expr::binary(BinaryOp::Mul, two, three).unwrap(),
    )
    .unwrap();
    assert_eq!(folded_literal(&e), Scalar::Int64(7));
}

#[test]
fn date_minus_interval_folds_like_tpch_q1() {
    let date = Expr::literal(ymd_to_date32("1998-12-01").map(Scalar::Date32).unwrap());
    let iv = Expr::literal(parse_interval("90", IntervalUnit::Day).unwrap());
    let e = Expr::binary(BinaryOp::Sub, date, iv).unwrap();
    assert_eq!(
        folded_literal(&e),
        Scalar::Date32(ymd_to_date32("1998-09-02").unwrap())
    );
}

#[test]
fn division_by_zero_is_left_unfolded() {
    let e = Expr::binary(
        BinaryOp::Div,
        Expr::literal(Scalar::Int64(1)),
        Expr::literal(Scalar::Int64(0)),
    )
    .unwrap();
    assert!(
        matches!(fold(&e), Expr::Binary { .. }),
        "运行期报错，不在计划期失败"
    );
}

#[test]
fn column_refs_block_folding() {
    let e = Expr::binary(BinaryOp::Add, a(), Expr::literal(Scalar::Int64(0))).unwrap();
    assert!(matches!(fold(&e), Expr::Binary { .. }));
}

#[test]
fn cast_of_literal_folds_with_type() {
    let e = Expr::Cast {
        expr: Box::new(Expr::literal(Scalar::Int64(3))),
        to: DataType::Float64,
    };
    assert_eq!(folded_literal(&e), Scalar::Float64(3.0));
}

// ---- 展示 ----

#[test]
fn display_is_sqlish() {
    let e = Expr::binary(BinaryOp::Add, a(), Expr::literal(Scalar::Int64(1))).unwrap();
    assert_eq!(e.to_string(), "a + 1");
    let c = Expr::Cast {
        expr: Box::new(a()),
        to: DataType::Float64,
    };
    assert_eq!(c.to_string(), "CAST(a AS Float64)");
    assert_eq!(Expr::literal(Scalar::Decimal(5, 2, 2)).to_string(), "0.05");
    assert_eq!(
        Expr::literal(ymd_to_date32("1998-12-01").map(Scalar::Date32).unwrap()).to_string(),
        "DATE '1998-12-01'"
    );
}

#[test]
fn untyped_null_in_arithmetic_casts_to_operand_type() {
    // a + NULL：NULL 侧 Cast 到 Int64，运行期得到全 NULL 的 Int64 而非 kernel 报错
    let b = batch();
    let e = Expr::binary(
        BinaryOp::Add,
        a(),
        Expr::literal(Scalar::Null(DataType::Null)),
    )
    .unwrap();
    assert_eq!(e.data_type(), DataType::Int64);
    assert_eq!(eval_i64(&e, &b), vec![None; 3]);
}
