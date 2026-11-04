//! 对一个 Morsel（RecordBatch）求绑定表达式。全部走 arrow kernel，不在执行路径上
//! 自写算术——计划期的类型裁决（coerce）与这里的运行期行为因此同源。

use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Date32Array;
use arrow::array::Decimal128Array;
use arrow::array::Float64Array;
use arrow::array::Int64Array;
use arrow::array::IntervalDayTimeArray;
use arrow::array::IntervalMonthDayNanoArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::compute::cast;
use arrow::compute::kernels::boolean;
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::numeric;
use arrow::datatypes::DataType;

use crate::{BinaryOp, Expr, ExprError, Scalar};

/// 对一个 Morsel 逐行求值，返回等长数组。
pub fn eval(expr: &Expr, morsel: &RecordBatch) -> Result<ArrayRef, ExprError> {
    match expr {
        Expr::Column { index, name, .. } => morsel
            .columns()
            .get(*index)
            .cloned()
            .ok_or_else(|| ExprError(format!("column {name} (index {index}) not in morsel"))),

        Expr::Literal { value, .. } => Ok(literal_array(value, morsel.num_rows())),

        Expr::Cast { expr, to } => {
            let value = eval(expr, morsel)?;
            cast(&value, to).map_err(|e| ExprError(format!("cast to {to}: {e}")))
        }

        Expr::Not { expr } => {
            let value = eval(expr, morsel)?;
            let bools = value
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExprError(format!("NOT expects BOOLEAN, got {}", value.data_type()))
                })?;
            Ok(Arc::new(boolean::not(bools)?))
        }

        Expr::Neg { expr, .. } => {
            let value = eval(expr, morsel)?;
            numeric::neg(&value).map_err(|e| ExprError(format!("negate: {e}")))
        }

        Expr::IsNull { expr, negated } => {
            let value = eval(expr, morsel)?;
            let out = if *negated {
                boolean::is_not_null(value.as_ref())?
            } else {
                boolean::is_null(value.as_ref())?
            };
            Ok(Arc::new(out))
        }

        Expr::Binary {
            op, left, right, ..
        } => {
            let l = eval(left, morsel)?;
            let r = eval(right, morsel)?;
            binary_op(*op, l.as_ref(), r.as_ref())
        }
    }
}

fn binary_op(op: BinaryOp, l: &dyn Array, r: &dyn Array) -> Result<ArrayRef, ExprError> {
    let err = |e: arrow::error::ArrowError| ExprError(format!("{}: {e}", op.symbol()));
    match op {
        BinaryOp::And => {
            let l = as_boolean(l)?;
            let r = as_boolean(r)?;
            Ok(Arc::new(boolean::and_kleene(l, r)?))
        }
        BinaryOp::Or => {
            let l = as_boolean(l)?;
            let r = as_boolean(r)?;
            Ok(Arc::new(boolean::or_kleene(l, r)?))
        }
        BinaryOp::Eq => Ok(Arc::new(cmp::eq(&l, &r)?)),
        BinaryOp::NotEq => Ok(Arc::new(cmp::neq(&l, &r)?)),
        BinaryOp::Lt => Ok(Arc::new(cmp::lt(&l, &r)?)),
        BinaryOp::LtEq => Ok(Arc::new(cmp::lt_eq(&l, &r)?)),
        BinaryOp::Gt => Ok(Arc::new(cmp::gt(&l, &r)?)),
        BinaryOp::GtEq => Ok(Arc::new(cmp::gt_eq(&l, &r)?)),
        BinaryOp::Add => numeric::add(&l, &r).map_err(err),
        BinaryOp::Sub => numeric::sub(&l, &r).map_err(err),
        BinaryOp::Mul => numeric::mul(&l, &r).map_err(err),
        BinaryOp::Div => numeric::div(&l, &r).map_err(err),
    }
}

fn as_boolean(a: &dyn Array) -> Result<&BooleanArray, ExprError> {
    a.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ExprError(format!("expects BOOLEAN, got {}", a.data_type())))
}

fn literal_array(value: &Scalar, len: usize) -> ArrayRef {
    match value {
        Scalar::Null(t) => arrow::array::new_null_array(t, len),
        Scalar::Int64(v) => Arc::new(Int64Array::from(vec![*v; len])),
        Scalar::Float64(v) => Arc::new(Float64Array::from(vec![*v; len])),
        Scalar::Utf8(v) => Arc::new(StringArray::from(vec![Some(v.as_str()); len])),
        Scalar::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; len])),
        Scalar::Date32(v) => Arc::new(Date32Array::from(vec![*v; len])),
        Scalar::IntervalDayTime(v) => Arc::new(IntervalDayTimeArray::from(vec![*v; len])),
        Scalar::IntervalMonthDayNano(v) => Arc::new(IntervalMonthDayNanoArray::from(vec![*v; len])),
        Scalar::Decimal(v, p, s) => Arc::new(
            Decimal128Array::from(vec![*v; len])
                .with_precision_and_scale(*p, *s)
                .expect("literal precision/scale valid by construction"),
        ),
    }
}

/// 从单元素数组取出标量（折叠用）。null 元素归一为 typed null。
pub(crate) fn scalar_at(array: &dyn Array, index: usize) -> Scalar {
    use arrow::datatypes::IntervalUnit;
    let null_of = |t: DataType| Scalar::Null(t.clone());
    match array.data_type() {
        DataType::Null => Scalar::Null(DataType::Null),
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            if a.is_null(index) {
                null_of(DataType::Boolean)
            } else {
                Scalar::Boolean(a.value(index))
            }
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            if a.is_null(index) {
                null_of(DataType::Int64)
            } else {
                Scalar::Int64(a.value(index))
            }
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            if a.is_null(index) {
                null_of(DataType::Float64)
            } else {
                Scalar::Float64(a.value(index))
            }
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            if a.is_null(index) {
                null_of(DataType::Date32)
            } else {
                Scalar::Date32(a.value(index))
            }
        }
        DataType::Decimal128(p, s) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let t = DataType::Decimal128(*p, *s);
            if a.is_null(index) {
                null_of(t)
            } else {
                Scalar::Decimal(a.value(index), *p, *s)
            }
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            if a.is_null(index) {
                null_of(DataType::Utf8)
            } else {
                Scalar::Utf8(a.value(index).to_string())
            }
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let a = array
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .unwrap();
            if a.is_null(index) {
                null_of(array.data_type().clone())
            } else {
                Scalar::IntervalDayTime(a.value(index))
            }
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let a = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap();
            if a.is_null(index) {
                null_of(array.data_type().clone())
            } else {
                Scalar::IntervalMonthDayNano(a.value(index))
            }
        }
        t => Scalar::Null(t.clone()),
    }
}
