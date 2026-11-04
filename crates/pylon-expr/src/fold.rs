//! 常量折叠：自底向上重写，子树不含列引用时在 1 行空 schema 上求值并替换为字面量。
//!
//! 求值失败（如除零、溢出）的子树原样保留——折叠是优化不是校验，错误留给运行期，
//! 保证优化器不会让本来能报用户错误的查询在计划期死掉。

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::array::RecordBatchOptions;
use arrow::datatypes::Schema;

use crate::eval::scalar_at;
use arrow::datatypes::DataType;

use crate::{BinaryOp, Expr, Scalar, eval};

/// 折叠后的表达式（不保证全部常量化；失败处保留原形）。
pub fn fold(expr: &Expr) -> Expr {
    let folded = match expr {
        Expr::Column { .. } | Expr::Literal { .. } => return expr.clone(),
        Expr::Cast { expr, to } => Expr::Cast {
            expr: Box::new(fold(expr)),
            to: to.clone(),
        },
        Expr::Not { expr } => Expr::Not {
            expr: Box::new(fold(expr)),
        },
        Expr::Neg { expr, data_type } => Expr::Neg {
            expr: Box::new(fold(expr)),
            data_type: data_type.clone(),
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(fold(expr)),
            negated: *negated,
        },
        Expr::Binary {
            op,
            left,
            right,
            data_type,
        } => {
            let l = fold(left);
            let r = fold(right);
            // 布尔字面量简化（Kleene 三值逻辑下成立：FALSE 支配 AND、TRUE 支配 OR、
            // 单位元消除）。其余情形按常量子树折叠。
            let lit_bool = |b| Expr::Literal {
                value: Scalar::Boolean(b),
                data_type: DataType::Boolean,
            };
            let is_bool = |e: &Expr, b| matches!(e, Expr::Literal { value: Scalar::Boolean(v), .. } if *v == b);
            match (*op, l, r) {
                (BinaryOp::And, l, r) if is_bool(&l, false) || is_bool(&r, false) => {
                    lit_bool(false)
                }
                (BinaryOp::And, l, _) if is_bool(&l, true) => l,
                (BinaryOp::And, l, r) if is_bool(&r, true) => l,
                (BinaryOp::Or, l, r) if is_bool(&l, true) || is_bool(&r, true) => lit_bool(true),
                (BinaryOp::Or, l, r) if is_bool(&l, false) => r,
                (BinaryOp::Or, _, r) if is_bool(&r, false) => r,
                (op, l, r) => Expr::Binary {
                    op,
                    left: Box::new(l),
                    right: Box::new(r),
                    data_type: data_type.clone(),
                },
            }
        }
    };
    literalize(folded)
}

/// 若 `expr` 不引用任何列且非字面量，则求值替换为字面量；失败原样返回。
fn literalize(expr: Expr) -> Expr {
    if matches!(expr, Expr::Literal { .. }) || !expr.column_refs().is_empty() {
        return expr;
    }
    let schema = Arc::new(Schema::empty());
    let morsel = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .expect("empty schema with explicit row count");
    match eval(&expr, &morsel) {
        Ok(array) => Expr::Literal {
            value: scalar_at(array.as_ref(), 0),
            data_type: array.data_type().clone(),
        },
        Err(_) => expr,
    }
}
