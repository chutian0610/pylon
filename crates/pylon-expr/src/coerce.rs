//! M1 最小类型强制规则（父 spec：整数提升、decimal 精度、date 比较、字符串等值；
//! 完整硬化在 T6）。
//!
//! 规则只产出"哪一侧需要 Cast 到什么类型 + 结果类型"，不执行转换；求值由
//! arrow kernel 完成，Decimal 结果精度/规模按 arrow 的 Hive 规则公式推导，保证
//! 计划期类型与运行期数组一致。

use crate::BinaryOp;
use arrow::datatypes::DataType;

const MAX_PRECISION: u8 = 38;

/// 一条二元运算的类型裁决：`left`/`right` 为 `Some` 时该侧需要 Cast。
pub struct Coercion {
    pub left: Option<DataType>,
    pub right: Option<DataType>,
    pub result: DataType,
}

/// 算术（Add/Sub/Mul/Div）的结果类型与两侧提升。
pub fn arithmetic_result_type(
    op: BinaryOp,
    l: &DataType,
    r: &DataType,
) -> Result<Coercion, String> {
    use DataType::*;

    // date 与 interval：只允许 date ± interval（interval + date 交换律由调用方归一）
    if is_date_interval(l, r) || is_date_interval(r, l) {
        if op == BinaryOp::Sub && is_interval(l) {
            return Err(format!(
                "cannot subtract a date from an interval: {l} - {r}"
            ));
        }
        return Ok(Coercion {
            left: None,
            right: None,
            result: Date32,
        });
    }

    let l_null = matches!(l, Null);
    let r_null = matches!(r, Null);
    let (l, r) = unify_null(l, r);
    // 无类型 NULL 侧必须 Cast 到统一类型，否则运行期会以 Null 数组撞 kernel
    let null_cast = |coercion: &mut Coercion| {
        if l_null {
            coercion.left = Some(l.clone());
        }
        if r_null {
            coercion.right = Some(r.clone());
        }
    };

    // Float64 同化（含 Decimal → Float64）
    if (*l == Float64 || *r == Float64) && is_numeric(l) && is_numeric(r) {
        let mut c = Coercion {
            left: (l != &Float64).then_some(Float64),
            right: (r != &Float64).then_some(Float64),
            result: Float64,
        };
        null_cast(&mut c);
        return Ok(c);
    }

    // Decimal 与 Decimal/Int64：Hive 精度规则
    if let (Decimal128(p1, s1), Decimal128(p2, s2)) = (l, r) {
        let (p, s) = decimal_result(op, *p1, *s1, *p2, *s2)?;
        let mut c = Coercion {
            left: None,
            right: None,
            result: Decimal128(p, s),
        };
        null_cast(&mut c);
        return Ok(c);
    }
    if let (Decimal128(p1, s1), Int64) | (Int64, Decimal128(p1, s1)) = (l, r) {
        let decimal_int = Decimal128(19, 0);
        let (p, s) = decimal_result(op, *p1, *s1, 19, 0)?;
        let (cast_l, cast_r) = if l == &Int64 {
            (Some(decimal_int), None)
        } else {
            (None, Some(decimal_int))
        };
        return Ok(Coercion {
            left: cast_l,
            right: cast_r,
            result: Decimal128(p, s),
        });
    }

    if l == r && is_numeric(l) {
        let mut c = Coercion {
            left: None,
            right: None,
            result: l.clone(),
        };
        null_cast(&mut c);
        return Ok(c);
    }

    Err(format!("cannot apply {} to {l} and {r}", op.symbol()))
}

/// 比较（Eq/NotEq/Lt/LtEq/Gt/GtEq）两侧的公共类型。
pub fn comparison_common_type(l: &DataType, r: &DataType) -> Result<Coercion, String> {
    use DataType::*;

    let l_null = matches!(l, DataType::Null);
    let r_null = matches!(r, DataType::Null);
    let (l, r) = unify_null(l, r);

    if l == r {
        let mut c = Coercion {
            left: None,
            right: None,
            result: l.clone(),
        };
        if l_null {
            c.left = Some(l.clone());
        }
        if r_null {
            c.right = Some(r.clone());
        }
        return Ok(c);
    }

    if is_numeric(l) && is_numeric(r) {
        if l == &Float64 || r == &Float64 {
            return Ok(Coercion {
                left: (l != &Float64).then_some(Float64),
                right: (r != &Float64).then_some(Float64),
                result: Float64,
            });
        }
        if let (Int64, Decimal128(..)) | (Decimal128(..), Int64) = (l, r) {
            let decimal_int = Decimal128(19, 0);
            let (cast_l, cast_r) = if l == &Int64 {
                (Some(decimal_int.clone()), None)
            } else {
                (None, Some(decimal_int.clone()))
            };
            return Ok(Coercion {
                left: cast_l,
                right: cast_r,
                result: decimal_int,
            });
        }
    }

    Err(format!("cannot compare {l} with {r}"))
}

/// arrow Decimal128 算术的 Hive 结果精度/规模（与 arrow-arith `decimal_op` 一致）。
fn decimal_result(op: BinaryOp, p1: u8, s1: i8, p2: u8, s2: i8) -> Result<(u8, i8), String> {
    match op {
        BinaryOp::Add | BinaryOp::Sub => {
            let s = s1.max(s2);
            let p = (i16::from(s)
                + (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2))
                + 1) as u8;
            Ok((p.min(MAX_PRECISION), s))
        }
        BinaryOp::Mul => {
            let p = u16::from(p1) + u16::from(p2) + 1;
            let s = i16::from(s1) + i16::from(s2);
            if s > i16::from(MAX_PRECISION) {
                return Err(format!("scale of {p1},{s1} * {p2},{s2} would exceed 38"));
            }
            Ok((p.min(u16::from(MAX_PRECISION)) as u8, s as i8))
        }
        BinaryOp::Div => {
            let s = i16::from(s1) + 4;
            let mul_pow = s - i16::from(s1) + i16::from(s2);
            let p = mul_pow + i16::from(p1);
            Ok((p.min(38) as u8, s.min(38) as i8))
        }
        op => Err(format!("{} is not an arithmetic operator", op.symbol())),
    }
}

fn unify_null<'a>(l: &'a DataType, r: &'a DataType) -> (&'a DataType, &'a DataType) {
    match (l, r) {
        // 无类型 NULL 提升为另一侧的类型（两侧都 NULL 则保持，由后续规则报错）
        (DataType::Null, other) | (other, DataType::Null) => (other, other),
        _ => (l, r),
    }
}

fn is_numeric(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

fn is_interval(t: &DataType) -> bool {
    matches!(t, DataType::Interval(_))
}

fn is_date_interval(l: &DataType, r: &DataType) -> bool {
    matches!(l, DataType::Date32) && is_interval(r)
}
