//! IO 侧：把一个 ScanSplit 的 Morsel 流泵入有界队列，并落实 Morsel 的字节目标。

use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::RecordBatch;
use async_channel::Sender;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use pylon_table::{MorselStream, TableError};

#[derive(Debug, Clone, Copy)]
pub(crate) struct MorselConfig {
    pub rows: usize,
    pub bytes: usize,
}

/// 打开 split 并把每个 Morsel 送入 `tx`。
///
/// 队列满时 `send().await` 让出，背压由此从计算侧传导到 IO 侧而不占住 IO 线程；
/// 接收端全部退出（LIMIT 达成或出错）时 send 失败，任务结束。
pub(crate) async fn pump(
    open: BoxFuture<'static, Result<MorselStream, TableError>>,
    cfg: MorselConfig,
    tx: &Sender<RecordBatch>,
    stop: &AtomicBool,
) -> Result<(), TableError> {
    let mut stream = open.await?;
    while let Some(next) = stream.next().await {
        let morsel = next?;
        for piece in split_by_bytes(morsel, cfg) {
            if stop.load(Ordering::Acquire) || tx.send(piece).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// 行数上限已由流保证；这里按估算字节（总字节 / 行数）把过大的 Morsel 切成多片。
fn split_by_bytes(morsel: RecordBatch, cfg: MorselConfig) -> Vec<RecordBatch> {
    let rows = morsel.num_rows();
    if rows == 0 {
        return Vec::new();
    }
    let bytes = morsel.get_array_memory_size();
    if bytes <= cfg.bytes.max(1) {
        return vec![morsel];
    }
    let per_row = bytes.div_ceil(rows);
    let rows_per_piece = (cfg.bytes / per_row).clamp(1, cfg.rows.max(1));
    (0..rows)
        .step_by(rows_per_piece)
        .map(|start| morsel.slice(start, rows_per_piece.min(rows - start)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let values: Vec<i64> = (0..rows as i64).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn small_morsel_passes_through_untouched() {
        let pieces = split_by_bytes(
            batch(10),
            MorselConfig {
                rows: 100,
                bytes: 1 << 20,
            },
        );
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].num_rows(), 10);
    }

    #[test]
    fn oversized_morsel_is_sliced_by_estimated_bytes() {
        let big = batch(1000);
        let total_bytes = big.get_array_memory_size();
        let pieces = split_by_bytes(
            big,
            MorselConfig {
                rows: 1000,
                bytes: total_bytes / 4,
            },
        );
        assert!(pieces.len() >= 4, "{}", pieces.len());
        assert_eq!(pieces.iter().map(|p| p.num_rows()).sum::<usize>(), 1000);
        assert!(pieces.iter().all(|p| p.num_rows() <= 250));
    }

    #[test]
    fn empty_morsel_is_dropped() {
        assert!(
            split_by_bytes(
                batch(0),
                MorselConfig {
                    rows: 10,
                    bytes: 10
                }
            )
            .is_empty()
        );
    }
}
