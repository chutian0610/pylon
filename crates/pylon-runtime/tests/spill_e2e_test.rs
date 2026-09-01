//! End-to-end spill test for `HashAggregateOp`.
//!
//! Demonstrates RFC 0007 §5 S2: a single op that overflows its per-task
//! memory budget triggers a spill-to-disk, then reloads at EOS, and
//! emits the correct final result. Verifies:
//!  - `pool.try_grow` failure on a batch triggers `Spillable::spill`
//!    (auto-spill-and-retry in `add_input`)
//!  - the spill file is read back at `no_more_input`
//!  - the final result equals the result of an unconstrained run
//!
//! Run: `cargo test -p pylon-runtime --test spill_e2e_test`

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use pylon_runtime::PipelineOp;
use pylon_runtime::ops::{AggSpec, HashAggregateOp};
use pylon_runtime::{PerTaskPool, SpillManager};
use pylon_types::MemoryPool;

fn make_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]))
}

fn make_batch(start: i64, count: usize) -> RecordBatch {
    let schema = make_schema();
    let n = count;
    let mut ids = Vec::with_capacity(n);
    let mut names = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    for i in 0..n {
        let idx = start + i as i64;
        ids.push(idx);
        names.push(format!("name_{:02}", idx / 50));
        amounts.push((idx as f64) * 1.5 + 0.01);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .unwrap()
}

/// The reference (no-spill) computation: 4 groups of 50 each.
fn expected_groups() -> Vec<(String, i64)> {
    vec![
        ("name_00".to_string(), 50),
        ("name_01".to_string(), 50),
        ("name_02".to_string(), 50),
        ("name_03".to_string(), 50),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_aggregate_triggers_spill_and_reloads_correctly() {
    // 200 rows, 4 distinct groups of 50 each.  We split into two
    // batches so the second one's try_grow can fail and trigger
    // spill.
    let b1 = make_batch(0, 100);
    let b2 = make_batch(100, 100);

    // Pool budget: 5000 bytes.  Each batch's estimate is
    // `100 rows * 32 = 3200` bytes.
    //  - b1.try_grow(3200)  -> Ok    (5000 >= 3200)
    //  - after b1: in_use = 3200
    //  - b2.try_grow(3200)  -> Err   (5000 - 3200 = 1800 < 3200)
    //  - b2 auto-spill: state of b1 -> spill file, pool_allocated=0
    //  - b2 retry try_grow(3200) -> Ok
    //  - process b2, in_use = 3200 again
    //  - no_more_input: reload spill file, merge, emit 4 groups.
    let pool = PerTaskPool::new(5000);

    // Use a deterministic spill root so this test doesn't leak
    // spill files across runs.
    let spill_root = std::env::temp_dir().join(format!(
        "pylon-spill-e2e-pid{}-t{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&spill_root);

    let mut agg = HashAggregateOp::with_pool(
        vec!["name".to_string()],
        vec![AggSpec {
            func: "count".to_string(),
            arg_col: None,
            out_name: "count".to_string(),
        }],
        Arc::new(Schema::empty()),
        pool.clone(),
    )
    .with_spill_root(spill_root.clone());

    agg.add_input(b1).await.expect("b1 fits budget");
    // After b1, pool has 3200 bytes claimed.
    assert_eq!(pool.in_use(), 3200, "b1 should consume ~3200 bytes");

    // b2 will fail try_grow and trigger auto-spill. With a 5000-byte
    // budget and 3200 already in use, b2.try_grow(3200) is rejected
    // -> spill -> retry succeeds.
    agg.add_input(b2).await.expect("b2 spills then processes");

    // After b2 + auto-spill, in_use should be back near b2's
    // estimate (3200); the b1 state was spilled to disk.
    assert_eq!(pool.in_use(), 3200, "spill should release b1's budget");

    // Verify the spill file actually exists on disk.
    let entries: Vec<_> = std::fs::read_dir(&spill_root)
        .expect("spill root should exist")
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "exactly 1 spill file expected (got {} entries: {:#?})",
        entries.len(),
        entries
    );

    // EOS: reload spill, merge, emit.
    agg.no_more_input().await.expect("no_more_input");

    // The spill file should have been deleted on resume (RFC §3.3).
    let entries_after: Vec<_> = std::fs::read_dir(&spill_root)
        .expect("spill root still exists")
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(
        entries_after.len(),
        0,
        "spill file should be unlinked after resume (got {:?})",
        entries_after
    );

    // Now pull the final batch and verify.
    let out_batch = agg.get_output().await.expect("get_output");
    let final_batch = out_batch.expect("expected a final batch");
    assert_eq!(final_batch.num_rows(), 4, "expected 4 groups");

    // Columns: name (Utf8), count (Int64)
    let names = final_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = final_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Materialize to a map and compare (order is sorted by the op).
    let mut actual: Vec<(String, i64)> = (0..final_batch.num_rows())
        .map(|i| (names.value(i).to_string(), counts.value(i)))
        .collect();
    actual.sort();

    assert_eq!(actual, expected_groups());

    // is_finished should be true.
    assert!(agg.is_finished().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_aggregate_no_pool_constraint_does_not_spill() {
    // Sanity check: when budget is much larger than total bytes,
    // no spill is triggered.
    let pool = PerTaskPool::new(1 << 20); // 1 MiB
    let _ = SpillManager::new(std::env::temp_dir().join("pylon-no-spill-test")).expect("tempdir");

    let mut agg = HashAggregateOp::with_pool(
        vec!["name".to_string()],
        vec![AggSpec {
            func: "count".to_string(),
            arg_col: None,
            out_name: "count".to_string(),
        }],
        Arc::new(Schema::empty()),
        pool.clone(),
    );

    let b1 = make_batch(0, 200);
    agg.add_input(b1).await.expect("single batch fits");
    agg.no_more_input().await.expect("no_more_input");

    let out = agg.get_output().await.expect("get_output").expect("batch");
    assert_eq!(out.num_rows(), 4);
}

/// RFC 0007 §3.5: a coord-retried task resumes from the stalled
/// attempt's spill file via `with_pending_resume`. Op A spills and
/// dies (simulating the stall); Op B — the retry — folds the spilled
/// state plus its own input and emits the merged groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_resume_folds_retry_spill() {
    let spill_root = std::env::temp_dir().join(format!(
        "pylon-spill-retry-pid{}-t{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&spill_root);
    let mgr = SpillManager::new(&spill_root).expect("tempdir");

    // Op A (stalled attempt): fold b1, spill its state, drop.
    let mut op_a = HashAggregateOp::new(
        vec!["name".to_string()],
        vec![AggSpec {
            func: "count".to_string(),
            arg_col: None,
            out_name: "count".to_string(),
        }],
        Arc::new(Schema::empty()),
    );
    op_a.add_input(make_batch(0, 100)).await.expect("b1 in");
    let handle = {
        use pylon_runtime::Spillable;
        op_a.spill(&mgr).await.expect("op A spills state")
    };
    // Op A dies here without emitting (the stall). The spill file
    // stays on disk for the retry.
    drop(op_a);
    assert!(handle.path.exists(), "spill file exists for retry");

    // Op B (coord retry): carries the handle; folds the spilled
    // state plus its own input at no_more_input.
    let mut op_b = HashAggregateOp::new(
        vec!["name".to_string()],
        vec![AggSpec {
            func: "count".to_string(),
            arg_col: None,
            out_name: "count".to_string(),
        }],
        Arc::new(Schema::empty()),
    )
    .with_pending_resume(handle);
    op_b.add_input(make_batch(100, 100)).await.expect("b2 in");
    op_b.no_more_input().await.expect("no_more_input");

    let final_batch = op_b
        .get_output()
        .await
        .expect("get_output")
        .expect("final batch");
    assert_eq!(final_batch.num_rows(), 4, "4 groups");
    let names = final_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = final_batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut actual: Vec<(String, i64)> = (0..final_batch.num_rows())
        .map(|i| (names.value(i).to_string(), counts.value(i)))
        .collect();
    actual.sort();
    // Merged result = the same groups an uninterrupted 200-row run
    // would produce: b1 (spilled state) contributed name_00/01,
    // the retry's own input b2 contributed name_02/03.
    assert_eq!(actual, expected_groups(), "retry folded spilled state");
}
