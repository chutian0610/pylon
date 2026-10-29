//! 计算侧：算子求值与 Pipeline 终点 Sink。全部同步代码。

use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use pylon_plan::Operator;

use crate::ExecError;

/// 对单个 Morsel 执行一个算子。
pub(crate) fn eval(op: &Operator, morsel: &RecordBatch) -> Result<RecordBatch, ExecError> {
    match op {
        Operator::Project { columns, schema } => {
            let projected = columns
                .iter()
                .map(|&i| {
                    morsel.columns().get(i).cloned().ok_or_else(|| {
                        ExecError::Operator(format!(
                            "Project: column index {i} out of range for morsel with {} columns",
                            morsel.num_columns()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            RecordBatch::try_new(schema.clone(), projected)
                .map_err(|e| ExecError::Operator(format!("Project: {e}")))
        }
    }
}

/// Pipeline 终点：收集结果并执行 LIMIT。多个计算线程并发推入。
pub(crate) struct Sink {
    limit: Option<u64>,
    /// 已认领的行数（含为满足 LIMIT 而截掉的部分），单调递增。
    claimed: AtomicU64,
    morsels: Mutex<Vec<RecordBatch>>,
}

impl Sink {
    pub(crate) fn new(limit: Option<u64>) -> Self {
        Self {
            limit,
            claimed: AtomicU64::new(0),
            morsels: Mutex::new(Vec::new()),
        }
    }

    /// 推入一个 Morsel。返回 `true` 表示 LIMIT 已满足，调用方应停止认领。
    pub(crate) fn push(&self, morsel: RecordBatch) -> bool {
        let rows = morsel.num_rows() as u64;
        if rows == 0 {
            return false;
        }
        let Some(limit) = self.limit else {
            self.store(morsel);
            return false;
        };
        // 原子认领行区间 [start, start + rows)，只保留落在 limit 以内的前缀；
        // 并发推入时各线程的保留区间互不重叠，总行数恰为 limit。
        let start = self.claimed.fetch_add(rows, Ordering::AcqRel);
        if start >= limit {
            return true;
        }
        let keep = (limit - start).min(rows);
        let kept = if keep < rows {
            morsel.slice(0, keep as usize)
        } else {
            morsel
        };
        self.store(kept);
        start + keep >= limit
    }

    fn store(&self, morsel: RecordBatch) {
        self.morsels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(morsel);
    }

    pub(crate) fn take_morsels(&self) -> Vec<RecordBatch> {
        std::mem::take(&mut *self.morsels.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    #[test]
    fn sink_without_limit_keeps_everything() {
        let sink = Sink::new(None);
        assert!(!sink.push(batch(&[1, 2])));
        assert!(!sink.push(batch(&[3])));
        let total: usize = sink.take_morsels().iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn sink_truncates_at_limit_and_signals_stop() {
        let sink = Sink::new(Some(3));
        assert!(!sink.push(batch(&[1, 2])));
        assert!(sink.push(batch(&[3, 4, 5])));
        assert!(sink.push(batch(&[6])));
        let batches = sink.take_morsels();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn sink_ignores_empty_morsels() {
        let sink = Sink::new(Some(1));
        assert!(!sink.push(batch(&[])));
        assert!(sink.take_morsels().is_empty());
    }
}
