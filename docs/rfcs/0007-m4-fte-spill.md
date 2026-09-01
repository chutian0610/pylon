# RFC 0007 — M4 Fault-Tolerant Execution + Spill

- **Status**: Draft (2026-08-20)
- **Date**: 2026-08-20
- **Author**: Pylon Working Group
- **Discussion**: Builds on RFC 0004 (M3 Arrow Flight shuffle), RFC 0005
  (pipeline trait surface), and the R7 `QueryStateMachine` (carry-over
  verified by `d610c8e` + PR #6 audit). Extends the connector SPI
  established in R6 (PR #1-#5 integrated via PR #8).

## 摘要

把 M3 留下的两类"in-memory only"短板补齐：

- **Spill**：per-task 工作集超预算 → HashAggregateOp / SortOp 把中间
  状态写到 S3-compatible ObjectStorage，超内存不重启 query。
- **FTE (fault-tolerant execution)**：worker 在任何时刻都可能挂
  (节点宕机、OOM、network blip)。`QueryStateMachine` 协调 retry，每
  task 在 retry 时从 spill + persisted exchange output 重建，不从头
  执行。

不动的事（按 milestones.md + RFC 0005 §8）：

- **Coordinator HA** → M5。Coord 还是 single point of failure，M4 只
  假定 worker 可以挂。
- **TLS / OIDC** → M5。
- **Adaptive batch size / 列存 shuffle** → 留作 perf-only follow-up。

## 0. Why this RFC now

[`docs/notes/m3-status.md`](../../notes/m3-status.md) 的 "Out-of-scope
(deferred)" 段第一条就是 FTE + spill。修不掉它，三件事卡住：

1. **生产可用性**：worker crash = 整 query 丢。中等规模集群一台机器
   月度故障率不容忽视，M3 同等 mesh 下每 query 末段期望 worker 存活
   概率 < 99%。
2. **TPC-H SF100**：原始 M3 scope 已推迟到 M3.5+。SF100 的 Q1/Q3 需
   要 spill + retry 才跑得动。
3. **Spill 缺失**：`crates/pylon-runtime/src/ops/aggregate.rs` 的
   docstring 明文 "M3 first cut — non-streaming. … streaming emit +
   spilling arrives in M4+." —— 写该 op 时已经预告了 M4 的形状，本
   RFC 是兑现。

Spec 先行的原因（与 RFC 0001-0005 同款）：M4 是 8 个 workstream /
~43 dev days 的多 PR 工程，没有 RFC 作锚，PR1-8 各自的 trait 边界会
撞车。本 RFC 锁住 trait 边界，PR 序列照表推进。

## 1. Two problems, one shared infrastructure

| 问题 | 表现 | 共享的救生资源 |
|---|---|---|
| Memory pressure | op 的 working set 超 budget → abort or 死锁 | per-task `MemoryPool` + `SpillManager` (把溢出字节搬到 ObjectStorage) |
| Worker fault | 任务跑到一半进程死 → 该 task 输出全丢 | `QueryStateMachine` 重新派发 + FTE-aware connector 从 spill / persisted exchange output 重建 |

两边都需要 ObjectStorage 落盘，所以"spill file format"和"FTE
persistence format"是同一个东西。本 RFC 不引入新 file format——复用
Arrow IPC streaming（已在 M3 Exchange 用过，RFC 0004 §6）。

## 2. Module layout

| 组件 | crate | 状态 |
|---|---|---|
| `MemoryPool` trait | `pylon-types`（已有槽位；RFC 0005 §4 `DataSourceContext.memory_pool`） | 待补成 trait，本 RFC |
| `PerTaskPool` impl + accounting | `pylon-runtime` | 待实现 |
| `SpillManager`（per-fragment budget） | `pylon-runtime` | 待新增 |
| Arrow IPC spill codec | `pylon-runtime`（已在 Exchange 复用） | 待 export |
| `Connector::supports_fte()` | `pylon-connector-spi` | 待新增 |
| FTE-aware `DataSource`/`DataSink` 扩展 | `pylon-connector-spi` | 待新增 `append_handles` / `next_handles` 形态 |
| ObjectStorage impl（S3-compatible） | `pylon-storage` | 待新增（加 `object_store` 依赖） |
| Retry orchestration + `TaskAck::Stalled` | `pylon-coord` | 待扩展 R7 的 `QueryStateMachine` |
| Spillable `HashAggregateOp` | `pylon-runtime/ops/aggregate.rs` | 重写 |
| Spillable `SortOp` | `pylon-runtime/ops/sort.rs` | 新增 |
| Chaos testbed | `tools/chaos/` | 新增 |

规则（继承 RFC 0005 §3）：

- **a)** `MemoryPool` trait 只能 `pub` 在 `pylon-types`；具体 impl 在
  `pylon-runtime`；connectors 不能 `use pylon_runtime::PerTaskPool`，
  只通过 `DataSourceContext.memory_pool` 拿到。这条由
  `tools/check-spi-boundaries.sh` 强制（已存在）。
- **b)** Spill 文件写入只通过 `DataSink::append`；spill 文件读取只
  通过 `DataSource::next`；spill manager **不直接**拿 `object_store`
  handle——它只看到 `DataSource`/`DataSink` trait object。
- **c)** Engine 不在 ObjectStorage 上 block。`DataSink::append` 返
  backpressure 信号（"pool exhausted"），engine 进入 wait tick 而不
  block tokio task。connector 拥有 pushback 所有权。
- **d)** Retry 不违反 RFC 0005 §3 rule #6：driver 仍每条线程独占
  `&mut PipelineOp`，retry 只是换线程 + 重新构造 op，op 的
  `&mut self` 永远不跨线程。

## 3. The contract (load-bearing types)

### 3.1 `MemoryPool` trait（占位 → 实化）

RFC 0005 §4 已经把 `Arc<dyn MemoryPool>` 作为 `DataSourceContext` 的
字段保留，但 trait 本身没定义。这一 PR 把它补出来：

```rust
/// Per-task byte budget. The pool is the SOLE gatekeeper of
/// `Vec<u8>` / Arrow allocation in pipelined ops.
pub trait MemoryPool: Send + Sync {
    /// Try to claim N bytes. `Ok(())` if claimed; `Err` if the pool
    /// would exceed its budget. The op MUST call `release(bytes)`
    /// when it is done with the buffer.
    fn try_grow(&self, bytes: usize) -> Result<(), PylonError>;

    /// Releases N bytes previously claimed via `try_grow`. The op
    /// MUST call this explicitly; release is not automatic on Drop.
    fn release(&self, bytes: usize);

    /// Returns the bytes currently claimed.
    fn in_use(&self) -> usize;

    /// Returns the configured budget.
    fn budget(&self) -> usize;

    /// Asks the pool "how many of `target` bytes may I claim right
    /// now?". Used by ops to size batch intake when the input side
    /// is `needs_input() == true` but the pool is partially full.
    /// Default impl: `min(target, budget - in_use)`.
    fn try_reserve(&self, target: usize) -> usize {
        let headroom = self.budget().saturating_sub(self.in_use());
        target.min(headroom)
    }
}
```

conformance rule: 每个 op 中规模随输入增长的 `Vec<u8>`/`RecordBatch`
必须在分配前 `try_grow`，用完后显式调用 `release(bytes)`。op 必须
保证在其生命周期结束前账平所有 `try_grow` 的字节（这是 RFC 0005
§3 rule #6 的具体兑现）。

### 3.2 `Spillable` operator contract

```rust
/// Marker: an op knows how to spill its working set and resume.
pub trait Spillable: PipelineOp {
    /// Persist the current working set to `manager`. Returns a
    /// `SpillHandle` that can be passed to `resume()` later.
    async fn spill(&mut self, manager: &SpillManager) -> Result<SpillHandle>;

    /// Resume from a previously-spilled state. Idempotent w.r.t.
    /// the spilled bytes; the op itself accumulates state on top.
    async fn resume(
        &mut self,
        manager: &SpillManager,
        handle: SpillHandle,
    ) -> Result<()>;
}
```

M4 first cut：只有 `HashAggregateOp` + `SortOp` 实现 `Spillable`。

### 3.3 Spill file layout

文件本身就是 Arrow IPC streaming，跟 RFC 0004 §6 的
`ExchangeSink`/`ExchangeSource` 用的同款。一个 spill file = `schema`
IPC message + N `RecordBatch` IPC messages + EOS marker。命名规则：

```
s3://<bucket>/pylon-spill/<query_id>/<stage_id>/<task_id>/<attempt>/spill-<seq>.arrow
```

- `attempt` = coord 端的 retry counter；同一 task 重派时 attempt++
- `seq` = 该 attempt 内 spill 序号（spill 可能多次触发）
- spill 文件由 `SpillManager` 创建、读完即删（读成功的 spill 文件
  在 resume 完成时同步 unlink；M4 不做 N-version 保留）

### 3.4 `Connector::supports_fte` + spill-capable flag

不引入新 connector 类型——给现 `Connector` 加一个能力位：

```rust
pub trait Connector: Send + Sync {
    fn connector_id(&self) -> &ConnectorId;
    fn capabilities(&self) -> ConnectorCapabilities;
    ...
}

/// Bitflag.
pub struct ConnectorCapabilities {
    /// True iff this connector can be the destination of spill
    /// files and FTE-persisted exchange output.
    pub fault_tolerant: bool,
}
```

`pylon-storage` impl 在配置了 `s3://` URL 时 `fault_tolerant = true`；
否则（local FS）也 `true`（local-FS spill 是合法降级路径，不影响本
协议）。M4 first cut 不实现 Lakekeeper / Hadoop connector，所以这个
flag 暂时只有 `pylon-storage` 一个数据点。

### 3.5 `QueryStateMachine` extension (R7 follow-up)

R7（`d610c8e` + PR #6 标记）已经给出 per-(query, stage) ack 的 trait
面。本 RFC 在 `TaskAck` 上加第三变体：

```rust
pub enum TaskAck {
    /// Task completed successfully. Existing R7 variant.
    Done,
    /// Task errored terminally. Existing R7 variant.
    Failed,
    /// NEW (M4). Task reached a recoverable spill boundary; coord
    /// may re-dispatch later passing the spill handle. R7 code path
    /// untouched; this is additive.
    Stalled { spill_handle: SpillHandle },
}
```

Stalled → coord 端 `QueryStateMachine.attempt + 1`，spill_handle 落
到 coord-side metadata（in-memory + on-disk log，跟随
milestones.md M4 "FTE source" 那一栏的实现走）。

> **Status (2026-09, M4.S5 + C5.5)**: Stalled ack + coord-side
> bookkeeping 已落地（`TaskAck::Stalled` / attempt counter /
> stalled-handle registry）。重派链条也已闭合：dispatcher 为每个
> stage 启动 retry watcher，消费 `stalled_handles()` 后按原
> `TaskSpec` 重派并注入 `spill_handle` OpSpec 键；
> `HashAggregateOp::with_pending_resume` 在 `no_more_input` 时折叠
> spilled state，重试从断点续算而非重启。on-disk log 与跨 S3
> spill root 的重派仍在 M4.S7/S8 之前补（见 candidates C5.6）。

## 4. Operator-level changes (where to look during implementation)

### 4.1 `HashAggregateOp` 重写（aggregate.rs）

当前（M3 first cut）：

```rust
//! M3 first cut — non-streaming. All input is buffered, then on
//! `no_more_input()` a single output batch with one row per group
//! is emitted.
```

替换为：

- 输入按 `pool.try_reserve(input_batch_size)` 来定 batch intake；
- 内部 `GroupKey → AggState` map 也走 pool accounting；
- 当 `pool.try_grow(next_chunk) == Err` → 触发 spill：
  1. 把 overflow buckets（按 bytes 由大到小）序列化为 Arrow IPC
     写到 `SpillManager.sink()`
  2. 清掉这些 buckets in-place，回收 bytes 到 pool
  3. 记 `SpillHandle` 到 op 的内部 vec
- `no_more_input()` 阶段：合并所有 spills + in-memory，最后一个
  output batch emit 出去。

`needs_input()` 在 pool 满时返 `false`，让 driver tick 转去 spill。

### 4.2 新增 `SortOp` (sort.rs)

M3 没有 SortOp。M4.S6 引入：top-K spillable sort with k-way merge。
trait contract 同 §3.2。

### 4.3 `PipelineOp` 不动

RFC 0005 §3 rule #6（driver 是唯一的 `&mut PipelineOp` 持有者）保持
不变。Spill 是 op 自己的内部行为，不需要 `PipelineOp` trait 改动。

## 5. Refactor sequence

| Phase | 目标 | 风险 | 依赖 |
|---|---|---|---|
| **M4.S1** | `pylon-types::MemoryPool` trait（沿用 RFC 0005 §4 已保留的 slot）+ `PerTaskPool` impl in `pylon-runtime`；接入 `HashAggregateOp` group_map 的 accounting。 | 低 | RFC 0005 R2 / R6 |
| **M4.S2** | `pylon-runtime::SpillManager` + spill file 生命周期（Arrow IPC streaming）。E2E spillable-but-not-fault-tolerant：单个 `HashAggregateOp` 真实溢写一次，resume 复算结果正确。 | 中 | M4.S1 |
| **M4.S3** | `ConnectorCapabilities.fault_tolerant` + `pylon-storage` impl `== true`。Spill manager 走 connector trait object 写入。 | 中 | M4.S2 |
| **M4.S4** | `pylon-storage` 加 `object_store` 依赖；`s3://` URL config 走通。不接 gRPC、不接 Flight，纯 local-FS-like `put`/`get`。 | 低 | M4.S3 |
| **M4.S5** | `TaskAck::Stalled` + coord-side spill-handle bookkeeping。R7 ack path 复用。 | 低 | M4.S3 + R7 (已 done) |
| **M4.S6** | Spillable `SortOp`（新增 sort.rs）。 | 中 | M4.S2 |
| **M4.S7** | Chaos testbed：`tools/chaos/` 跑随机 kill worker，verify query 终态正确。 | 高 | M4.S5 + M4.S6 |
| **M4.S8** | E2E 标志场景：1B 行 aggregate 在 1 coord + 2 worker 上跑，中途 kill 一 worker，query 走 spill + retry 完整完成，结果与 uninterrupted run 字节级一致。 | 中 | M4.S7 |

Approx 时间合计 6+5+5+6+3+8+5+5 ≈ 43 dev days，对齐
`milestones.md` M4 行（M3 后 M4 跨 1–2 月）。

## 6. Open questions / non-goals

### 6.1 Coordinator HA？ — M5。
coord 仍是 single point of failure。本 RFC 不动 coordinator-side
state replication。

### 6.2 Adaptive batch / dynamic budget？
per-task budget 在 `StageDag` 上一次配置。M4 不做动态调优（RFC
0005 §8.2 留作 perf-only follow-up，不破坏契约面）。

### 6.3 Cross-region spill？
local FS for dev + S3-compatible for prod。Cross-region 复制纯靠
connector config，是 M5+ 工作。

### 6.4 `Stalled` 与 `Failed` 区分吗？
区分。`Failed` 是终态，coord 把 error 抛给 client；`Stalled` 是
transient，coord 重新派发。R7 已有的 `TaskAck::Failed` 保持语义不变；
`Stalled` 是纯加项。

### 6.5 per-task vs per-fragment budget？
双层。`SpillManager` 在 fragment（stage-task）层管 SPILL_BUDGET（控
制总 spill 字节），`MemoryPool` 在 task 层管 TASK_BUDGET（控制 in-
memory 字节）。两者由 coord 在 dispatch 时建好实例。

## 7. Verification (test plan)

每个 phase 都跑这些：

- **单元测试**：在 `crates/pylon-runtime/tests/` 新增
  `spill_manager_test.rs` + `hash_aggregate_spill_test.rs` +
  `sort_op_test.rs`。M4.S5 加 `query_state_stalled_test.rs`。
- **M4.S7 chaos**：每 N 秒随机 SIGKILL 一个 worker，记录最终 query
  completion time + 结果正确性。脚本在
  `tools/chaos/random_kill.sh`，触发后跑 `tools/e2e/aggregate_1b.sh`。
- **M4.S8 E2E**：1B 行 Parquet 上 `SELECT k, COUNT(*) GROUP BY k`，
  在 stage 0 跑 30s 后 kill 一 worker；retry path 走通 + 最终
  `(k, count)` 行集合与 uninterrupted run 字节级一致（双跑对照）。

## 8. References

- [RFC 0003 §5](./0003-m2-control-data-plane.md)
  — M2 control channel reused for retry dispatch.
- [RFC 0004](./0004-m3-flight-shuffle.md)
  — Arrow Flight; R7 ack path 走过这一层。已 `Implemented (2026-08-18)`。
- [RFC 0005 §3](./0005-pipeline-trait-surface.md)
  — domain boundaries；本 RFC 的 rule #a–#d 继承 §3 的 8 条。
- [RFC 0005 §4](./0005-pipeline-trait-surface.md)
  — `DataSourceContext.memory_pool` 已为 `Arc<dyn MemoryPool>` 留
  槽，本 RFC §3.1 实化。
- [M3 sign-off packet](../../notes/m3-status.md) — "Out-of-scope
  (deferred)" 段第一条原文就是 FTE + spill，本 RFC 是签收兑现。
- [milestones.md §M4](../../roadmap/milestones.md) — 任务列表与
  dev-day 估算的源头。
- Presto fault-tolerant scheduler
  (`/Users/didi/myprojects/references/presto`) — `SqlStageExecution`
  retry metadata；本 RFC 的 `TaskAck::Stalled` 对齐 Presto 的
  `TaskState::STALLED`。
- Velox spill (`/Users/didi/myprojects/references/velox`) —
  `HashTable::spillPartition` + `Sort::spill`；本 RFC §4.1 的方
  法命名直接 crib。
- 关系 PR：`#6` (R7 audit), `#7` (R9 audit), `#8` (R6 integration)，
  以及当前 PR（本 RFC 草案，尚未生成）。
