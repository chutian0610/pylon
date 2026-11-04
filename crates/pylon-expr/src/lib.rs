//! Pylon 表达式层：planner 绑定后的类型化表达式（ADR-0005：Arrow 即逻辑即物理类型）。
//!
//! AST 的 [`pylon_sql::Expr`] 只描述语法；本 crate 的 [`Expr`] 是列名已解析（按输入
//! schema 的列索引）、类型已知、隐式转换已物化为 [`Expr::Cast`] 的可求值形态。
//! planner 在这里做类型推断与常量折叠，执行器在这里对每个 Morsel 求值——两侧共用
//! 同一套 kernel 语义，计划期与运行期不会各说各话。
//!
//! M1 规则面（T6 硬化前的最小集）：整数提升（Int64）、Decimal128 按 Hive 精度规则、
//! Float64 同化、date 与 interval 运算、同型比较、字符串等值。

pub mod coerce;
pub mod eval;
pub mod fold;

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};

pub use coerce::{arithmetic_result_type, comparison_common_type};
pub use eval::eval;
pub use fold::fold;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct ExprError(pub String);

impl From<arrow::error::ArrowError> for ExprError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ExprError(e.to_string())
    }
}

/// 绑定后的二元运算符。与 AST 的区别：`Between` 已在绑定期脱糖为两次比较。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

impl BinaryOp {
    /// 是否比较/逻辑（结果 Boolean）；否则算术。
    pub fn is_predicate(self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or
        )
    }

    pub fn symbol(self) -> &'static str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
        }
    }
}

/// 标量值。`Null` 携带类型（typed null），供比较/转换时定型。
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Null(DataType),
    Int64(i64),
    Float64(f64),
    /// (值, 精度, 规模)，与 Decimal128(p, s) 对应
    Decimal(i128, u8, i8),
    Utf8(String),
    Boolean(bool),
    /// 自 1970-01-01 的天数
    Date32(i32),
    /// DAY/HOUR/MINUTE/SECOND 单位的 interval 归一到 (days, milliseconds)
    IntervalDayTime(IntervalDayTime),
    /// YEAR/MONTH 单位的 interval 归一到 (months, days=0, nanos=0)
    IntervalMonthDayNano(IntervalMonthDayNano),
}

impl Scalar {
    pub fn data_type(&self) -> DataType {
        match self {
            Scalar::Null(t) => t.clone(),
            Scalar::Int64(_) => DataType::Int64,
            Scalar::Float64(_) => DataType::Float64,
            Scalar::Decimal(_, p, s) => DataType::Decimal128(*p, *s),
            Scalar::Utf8(_) => DataType::Utf8,
            Scalar::Boolean(_) => DataType::Boolean,
            Scalar::Date32(_) => DataType::Date32,
            Scalar::IntervalDayTime(_) => {
                DataType::Interval(arrow::datatypes::IntervalUnit::DayTime)
            }
            Scalar::IntervalMonthDayNano(_) => {
                DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
            }
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Scalar::Null(_))
    }
}

/// 绑定后的表达式：列索引相对输入 schema，类型在节点上，隐式转换已物化为 Cast。
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column {
        index: usize,
        /// 展示名（列名或聚合输出名），仅用于渲染与输出列命名
        name: Arc<str>,
        data_type: DataType,
    },
    Literal {
        value: Scalar,
        data_type: DataType,
    },
    Cast {
        expr: Box<Expr>,
        to: DataType,
    },
    Not {
        expr: Box<Expr>,
    },
    Neg {
        expr: Box<Expr>,
        data_type: DataType,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
        data_type: DataType,
    },
}

impl Expr {
    pub fn data_type(&self) -> DataType {
        match self {
            Expr::Column { data_type, .. } => data_type.clone(),
            Expr::Literal { data_type, .. } | Expr::Neg { data_type, .. } => data_type.clone(),
            Expr::Cast { to, .. } => to.clone(),
            Expr::Not { .. } | Expr::IsNull { .. } => DataType::Boolean,
            Expr::Binary { data_type, .. } => data_type.clone(),
        }
    }

    pub fn column(index: usize, name: &str, data_type: DataType) -> Expr {
        Expr::Column {
            index,
            name: Arc::from(name),
            data_type,
        }
    }

    pub fn literal(value: Scalar) -> Expr {
        let data_type = value.data_type();
        Expr::Literal { value, data_type }
    }

    /// 按类型规则构造二元节点：比较/逻辑两侧统一到公共类型（自动插入
    /// [`Expr::Cast`]），算术得出结果类型；类型不兼容返回 `Err(message)`。
    /// `date + interval` 交换到 `interval + date` 之外不改变语义。
    pub fn binary(op: BinaryOp, left: Expr, right: Expr) -> Result<Expr, String> {
        use arrow::datatypes::DataType;
        if matches!(op, BinaryOp::And | BinaryOp::Or) {
            for side in [&left, &right] {
                if side.data_type() != DataType::Boolean {
                    return Err(format!(
                        "{} expects BOOLEAN operands, got {}",
                        op.symbol(),
                        side.data_type()
                    ));
                }
            }
            return Ok(Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                data_type: DataType::Boolean,
            });
        }
        if op.is_predicate() {
            let c = comparison_common_type(&left.data_type(), &right.data_type())?;
            let left = wrap_cast(left, c.left);
            let right = wrap_cast(right, c.right);
            return Ok(Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                data_type: DataType::Boolean,
            });
        }
        // 算术：加法把 interval + date 归一为 date + interval（kernel 只认后者）
        let (op, left, right) = if op == BinaryOp::Add
            && matches!(left.data_type(), DataType::Interval(_))
            && right.data_type() == DataType::Date32
        {
            (op, right, left)
        } else {
            (op, left, right)
        };
        let c = arithmetic_result_type(op, &left.data_type(), &right.data_type())?;
        let left = wrap_cast(left, c.left);
        let right = wrap_cast(right, c.right);
        Ok(Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
            data_type: c.result,
        })
    }

    /// 逻辑非：仅接受 BOOLEAN。
    pub fn logical_not(expr: Expr) -> Result<Expr, String> {
        if expr.data_type() != DataType::Boolean {
            return Err(format!("NOT expects BOOLEAN, got {}", expr.data_type()));
        }
        Ok(Expr::Not {
            expr: Box::new(expr),
        })
    }

    /// 算术负号：仅接受数值，结果同型。
    pub fn negate(expr: Expr) -> Result<Expr, String> {
        let dt = expr.data_type();
        if !matches!(
            dt,
            DataType::Int64
                | DataType::Float64
                | DataType::Decimal128(..)
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
        ) {
            return Err(format!("cannot negate {dt}"));
        }
        Ok(Expr::Neg {
            expr: Box::new(expr),
            data_type: dt,
        })
    }

    /// IS [NOT] NULL：任意类型。
    pub fn is_null(expr: Expr, negated: bool) -> Expr {
        Expr::IsNull {
            expr: Box::new(expr),
            negated,
        }
    }

    /// 引用的输入列索引（含 Cast/谓词内部），升序去重。
    pub fn column_refs(&self) -> Vec<usize> {
        let mut out = Vec::new();
        collect_refs(self, &mut out);
        out.sort_unstable();
        out.dedup();
        out
    }
}

fn wrap_cast(expr: Expr, to: Option<DataType>) -> Expr {
    match to {
        Some(to) => Expr::Cast {
            expr: Box::new(expr),
            to,
        },
        None => expr,
    }
}

fn collect_refs(expr: &Expr, out: &mut Vec<usize>) {
    match expr {
        Expr::Column { index, .. } => out.push(*index),
        Expr::Literal { .. } => {}
        Expr::Cast { expr, .. } | Expr::Not { expr } | Expr::Neg { expr, .. } => {
            collect_refs(expr, out)
        }
        Expr::IsNull { expr, .. } => collect_refs(expr, out),
        Expr::Binary { left, right, .. } => {
            collect_refs(left, out);
            collect_refs(right, out);
        }
    }
}

/// 渲染为接近 SQL 的文本（explain 与输出列命名共用）。
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column { name, .. } => write!(f, "{name}"),
            Expr::Literal { value, .. } => write!(f, "{value}"),
            Expr::Cast { expr, to } => write!(f, "CAST({expr} AS {to})"),
            Expr::Not { expr } => write!(f, "NOT ({expr})"),
            Expr::Neg { expr, .. } => write!(f, "-({expr})"),
            Expr::IsNull { expr, negated } => {
                write!(f, "({expr} IS {}NULL)", if *negated { "NOT " } else { "" })
            }
            Expr::Binary {
                op, left, right, ..
            } => match op {
                // 左结合渲染：右侧 AND/OR 加括号；AND 的左侧若为 OR 也加括号
                BinaryOp::And | BinaryOp::Or => {
                    let lp = match left.as_ref() {
                        Expr::Binary {
                            op: BinaryOp::Or, ..
                        } if *op == BinaryOp::And => format!("({left})"),
                        _ => left.to_string(),
                    };
                    let rp = match right.as_ref() {
                        Expr::Binary {
                            op: BinaryOp::And | BinaryOp::Or,
                            ..
                        } => format!("({right})"),
                        _ => right.to_string(),
                    };
                    write!(f, "{lp} {} {rp}", op.symbol())
                }
                _ => write!(f, "{left} {} {right}", op.symbol()),
            },
        }
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Null(_) => write!(f, "NULL"),
            Scalar::Int64(v) => write!(f, "{v}"),
            Scalar::Float64(v) => write!(f, "{v}"),
            Scalar::Decimal(v, _, s) => write!(f, "{}", format_decimal(*v, *s)),
            Scalar::Utf8(v) => write!(f, "'{}'", v.replace('\'', "''")),
            Scalar::Boolean(v) => write!(f, "{v}"),
            Scalar::Date32(days) => write!(f, "DATE '{}'", date32_to_ymd(*days)),
            Scalar::IntervalDayTime(iv) => {
                write!(
                    f,
                    "INTERVAL '{},{}' DAY TO SECOND",
                    iv.days, iv.milliseconds
                )
            }
            Scalar::IntervalMonthDayNano(iv) => {
                write!(f, "INTERVAL '{},{}' YEAR TO MONTH", iv.months, iv.days)
            }
        }
    }
}

/// Decimal 文本形态：按规模保留小数位（`0.05` 不丢尾零）。
fn format_decimal(v: i128, scale: i8) -> String {
    if scale == 0 {
        return v.to_string();
    }
    let neg = v < 0;
    let mag = v.unsigned_abs();
    let ten: u128 = 10;
    let mut frac = String::new();
    let mut rem = mag;
    for _ in 0..scale {
        frac.insert(0, char::from(b'0' + (rem % ten) as u8));
        rem /= ten;
    }
    if neg {
        format!("-{rem}.{frac}")
    } else {
        format!("{rem}.{frac}")
    }
}

/// 数字字面量原文 → 标量：整数为 Int64（超界落 Decimal128(digits, 0)），
/// 小数为 Decimal128(有效位数, 小数位数)。精度超 38 报错。
pub fn parse_number(text: &str) -> Result<Scalar, String> {
    let (int_part, frac_part) = match text.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (text, None),
    };
    let Some(frac) = frac_part else {
        if let Ok(v) = int_part.parse::<i64>() {
            return Ok(Scalar::Int64(v));
        }
        let digits = int_part.len() as u8;
        if digits > 38 {
            return Err(format!(
                "number '{text}' exceeds maximum precision of 38 digits"
            ));
        }
        let v: i128 = int_part
            .parse()
            .map_err(|_| format!("invalid number '{text}'"))?;
        return Ok(Scalar::Decimal(v, digits, 0));
    };
    // 前导零不计入精度（0.05 → (2,2)，与 DuckDB/Trino 一致）；整数部分全零时精度=小数位数
    let significant_int = int_part.trim_start_matches('0').len();
    let precision = (significant_int + frac.len()).max(1) as u8;
    if precision > 38 {
        return Err(format!(
            "number '{text}' exceeds maximum precision of 38 digits"
        ));
    }
    let scale = frac.len() as i8;
    let v: i128 = format!("{int_part}{frac}")
        .parse()
        .map_err(|_| format!("invalid number '{text}'"))?;
    Ok(Scalar::Decimal(v, precision, scale))
}

/// interval 单位的中立表示（不含 SQL 语法；planner 负责 AST 单位到这里的映射）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalUnit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

/// INTERVAL 字面量 → 标量。数值必须是整数（允许负号）；YEAR/MONTH 记为月数，
/// DAY/HOUR/MINUTE/SECOND 归一到 (days, milliseconds)——Date32 运算只关心到天。
pub fn parse_interval(value: &str, unit: IntervalUnit) -> Result<Scalar, String> {
    let n: i64 = value
        .parse()
        .map_err(|_| format!("interval value '{value}' must be an integer"))?;
    let scalar = match unit {
        IntervalUnit::Year => Scalar::IntervalMonthDayNano(IntervalMonthDayNano::new(
            i32::try_from(
                n.checked_mul(12)
                    .ok_or_else(|| format!("interval value '{value}' out of range"))?,
            )
            .map_err(|_| format!("interval value '{value}' out of range"))?,
            0,
            0,
        )),
        IntervalUnit::Month => Scalar::IntervalMonthDayNano(IntervalMonthDayNano::new(
            i32::try_from(n).map_err(|_| format!("interval value '{value}' out of range"))?,
            0,
            0,
        )),
        IntervalUnit::Day => Scalar::IntervalDayTime(IntervalDayTime::new(
            i32::try_from(n).map_err(|_| format!("interval value '{value}' out of range"))?,
            0,
        )),
        IntervalUnit::Hour | IntervalUnit::Minute | IntervalUnit::Second => {
            let millis_per = match unit {
                IntervalUnit::Hour => 3_600_000,
                IntervalUnit::Minute => 60_000,
                IntervalUnit::Second => 1_000,
                _ => unreachable!(),
            };
            let total = n
                .checked_mul(millis_per)
                .ok_or_else(|| format!("interval value '{value}' out of range"))?;
            let days = i32::try_from(total / 86_400_000)
                .map_err(|_| format!("interval value '{value}' out of range"))?;
            let ms = i32::try_from(total % 86_400_000)
                .map_err(|_| format!("interval value '{value}' out of range"))?;
            Scalar::IntervalDayTime(IntervalDayTime::new(days, ms))
        }
    };
    Ok(scalar)
}

/// 天数 → `yyyy-mm-dd`（民用历，1970-01-01 = 第 0 天；Howard Hinnant 的
/// civil_from_days 算法，对负天数一致成立）。仅用于展示与 date 字面量解析的逆操作。
pub fn date32_to_ymd(days: i32) -> String {
    let z = i64::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

/// `yyyy-mm-dd` → 天数；非严格日期（如 2 月 30 日）返回 `None`。 [`date32_to_ymd`]
/// 的逆：先算天数再用逆运算校验一致性。
pub fn ymd_to_date32(ymd: &str) -> Option<i32> {
    let bytes = ymd.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let part = |r: std::ops::Range<usize>| -> Option<i32> {
        let s = ymd.get(r)?;
        if !s.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        s.parse().ok()
    };
    let (y, m, d) = (part(0..4)?, part(5..7)?, part(8..10)?);
    if !(1..=12).contains(&m) {
        return None;
    }
    // days_from_civil 的逆：直接换算再回验
    let yy = if m <= 2 { y - 1 } else { y };
    let era = yy.div_euclid(400);
    let yoe = yy.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    if date32_to_ymd(days) == ymd {
        Some(days)
    } else {
        None
    }
}
