# RFC 0002 — Execution Unit Hierarchy (Trino-aligned)

- **Status**: Accepted
- **Date**: 2026-08-18
- **Author**: Pylon Working Group
- **Discussion**: Builds on RFC-0001 §3 and §4

## 摘要

把 Pylon 的执行层抽象按照 Trino / Velox 模型完整对齐：Query → Stage → Task → Pipeline → Driver → Op。所有抽象都有 Rust 结构体实现。M2 默认 driver 模式保持 per-op-tokio-task（async-friendly 落地），M5+ 引入 single-thread fused driver（Trino-Velox 风格）。

## 1. 动机

RFC-0001 落地时，我们把 driver 抽象命名为 `Driver` 并使用"每 op 一个 tokio task + mpsc"实现。这导致两个问题：

1. **概念错位**：Trino 里的 `Driver` 是单线程循环；我们的"Driver" 实际上是 Trino 里的 Pipeline（一个有状态的 op chain）。后续看代码、读 Trino 论文、benchmark 都会概念混淆。
2. **缺少 Stage / Task / Query 抽象**：M2 跨进程分发时，这些都是必须的——否则 `Fragmenter` 和 `Scheduler` 没有放置处。

## 2. 决策

### ADR-001: 六层抽象（与 Trino 等价）

```
Layer  Name         Lives in       Quantity           Concretely
─────  ────────     ───────────    ───────────────    ──────────────────────
1      Query        coord          1 per submission  pylon_coord::query::Query
2      Stage        coord          ≥1 per query      pylon_coord::stage::Stage
3      Task         coord & wire   N per stage       pylon_coord::task::TaskSpec
4      Pipeline     worker         ≥1 per task       pylon_runtime::pipeline::Pipeline
5      Driver       worker         M per pipeline    pylon_runtime::driver::Driver
6      PipelineOp   worker         N per pipeline    pylon_runtime::op::PipelineOp
```

### ADR-002: Driver 单线程（M5+ 目标，M2 fallback）

M2 默认 `DriverMode::PerOpTokioTask`（行为同 M1）：
- 每 op 一 tokio task
- 跨 op 走 bounded mpsc
- async-friendly，最容易把 cross-process Flight 接入

M5+ 引入 `DriverMode::SingleThreadLoop`：
- 单一 OS thread / `block_in_place`
- op 之间直 method call，零 lock 零 channel
- 走 `spawn_blocking` 处理 CPU kernel

切换机制：env var `PYLON_DRIVER_MODE=per_op|single_thread`。

### ADR-003: PipelineOp::is_blocked 返回 future

`is_blocked` 是 Trino 的核心方法。签名：

```rust
async fn is_blocked(&self) -> Result<Option<BoxFuture<'static, ()>>>;
```

- 返回 `Some(future)` 表示 op 在等外部 IO
- Driver loop 中：拿到 future 后 polling，resolve 后回到正常 poll
- M2 默认实现返回 `Ok(None)`（ops 没显式 await，未来由 `ExchangeSourceOp` 等重写）

### ADR-004: StateBridge trait 提前定义

为 M3+ 的 HashJoinBridge 预留接口。trait：

```rust
pub trait StateBridge: Send + Sync + Debug {
    fn name(&self) -> &str;
    fn on_state_change(&self, change: StateChange) -> Result<()>;
}
```

`StateChange { BuildComplete, ProbeComplete, Restored, MemoryBackpressure }`。

M2 提供的 `DummyBridge` impl 是空操作。M3 实现 `HashJoinBridge` 时换掉。

### ADR-005: Fragmenter 在 coordinator

沿用 Trino 设计：`Fragmenter` 在 `pylon-coord` 进程，把 `PhysicalPlan` 切成 `StageDag`。这样：
- coordinator 进程**不**依赖具体 op 实现，只需 `OpSpec` 描述（name + config map）
- worker 通过 OpSpec 名称构造 PipelineOp，互不耦合
- 未来换 Substrait 只需改 Fragmenter 输出格式

## 3. 数据流概览

```
                  coord 进程                       worker 进程
             ┌─────────────────┐                ┌──────────────────────┐
SQL          │ Query           │                │                      │
  ──────────►│   parse         │                │                      │
             │   LogicalPlan   │                │                      │
             │   PhysicalPlan  │                │                      │
             │ Fragmenter      │                │                      │
             │   ┌──────────┐  │ TaskSpec[]     │  Driver.run()        │
             │   │StageDag  │  │ ────gRPC──────►│    Pipeline::new(ops)│
             │   │          │  │  + ExchangeSpec│    DriverMode::...   │
             │   └──────────┘  │                │      run()           │
             │ Scheduler       │                │         ↓            │
             │   assign()      │                │      Arrow batches   │
             └─────────────────┘                └──────────────────────┘
```

## 4. Rust 结构体清单（按对应层级）

| 层 | 名字 | 关键字段 |
|---|---|---|
| 1 | `Query` (pylon-coord::query) | `id, sql, state, stage_dag, submitted_at` |
| 2 | `Stage` (pylon-coord::stage) | `id, fragment, partition_count, upstream, downstream` |
| 2 | `Fragment` (同 module) | `ops: Vec<OpSpec>, distribution: Distribution` |
| 2 | `OpSpec` (同 module) | `name: String, config: HashMap<String, String>` |
| 2 | `StageDag` (同 module) | `stages: Vec<Stage>` |
| 3 | `TaskSpec` (pylon-coord::task) | `id, query_id, stage_id, partition, fragment, sources, sinks` |
| 3 | `ExchangeSpec` (同 module) | `kind, target_worker, target_partition, source_partition` |
| 4 | `Pipeline` (pylon-runtime::pipeline) | `id, ops: Vec<Arc<Mutex<dyn PipelineOp>>>, state_bridges` |
| 5 | `Driver` (pylon-runtime::driver) | `id, pipeline: Arc<Pipeline>, mode: DriverMode` |
| 5 | `DriverMode` (同 module) | `PerOpTokioTask \| SingleThreadLoop` |
| 6 | `PipelineOp` (pylon-runtime::op) | 7 方法 trait |
| - | `StateBridge` (pylon-runtime::bridge) | `name, on_state_change` trait |

## 5. 与 Trino 的对应表

| Trino 概念 | Pylon 等价 |
|---|---|
| `Query` | `Query` |
| `Stage` | `Stage` |
| `Task` (Stage × Partition) | `TaskSpec` |
| `Pipeline` | `Pipeline` |
| `Driver` | `Driver` (M5+ 时单线程；M2 是 per-op-task fallback) |
| `Operator` | `PipelineOp` |
| `JoinBridge` | `StateBridge` (trait) + 后续具体实现 |
| `Exchange` (3 种) | `ExchangeKind { Partitioned, Broadcast, Gather }` + `Local` (Pylon 加的同节点优化) |
| FTE `ExchangeSink` | M4 加入；M2 仅 transient buffering |
| `NodeScheduler` | `Scheduler` trait + `CapacityScheduler` impl |

## 6. Driver Mode 切换设计

```rust
// pylon-runtime/src/driver.rs
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub enum DriverMode {
    /// M2 default: per-op-as-tokio-task.
    /// Each PipelineOp runs as its own tokio::spawn task;
    /// data flows through bounded mpsc channels between ops.
    /// Async-friendly, easy to add ops, no fusion.
    #[default]
    PerOpTokioTask,
    /// M5+: single-thread fused driver loop.
    /// Currently a stub that delegates to PerOpTokioTask.
    SingleThreadLoop,
}

pub struct Driver {
    pub id: DriverId,
    pub pipeline: Arc<Pipeline>,
    pub mode: DriverMode,
}

impl Driver {
    pub fn new(pipeline: Arc<Pipeline>) -> Self { /* default mode */ }
    pub fn with_mode(mut self, mode: DriverMode) -> Self { ... }
    pub async fn run(self, input: Option<mpsc::Receiver<RecordBatch>>)
        -> Result<mpsc::Receiver<RecordBatch>>;
}
```

调用方代码（M2/M3+ 不变）：

```rust
let pipeline = Arc::new(Pipeline::new(ops));
let mut output = Driver::new(pipeline).run(None).await?;
```

M5+ 不需要修改 worker 端如何 call driver，只需把 `mode` 改成 `SingleThreadLoop` + 实现。

## 7. 与 RFC-0001 §ADR-001 … 008 的关系

| RFC-0001 ADR | 关联到这个 RFC |
|---|---|
| ADR-001 "coordinator 独立进程" | 这里：coordinator = Query/Stage/Task 三个抽象的家 |
| ADR-002 "不用 DataFusion ExecutionPlan" | 没变 |
| ADR-003 "Velox Operator/Driver/Task 1:1" | **本 RFC 是 ADR-003 落地** |
| ADR-004 "Doris 固定线程池" | 没变；`MaxBlockingThreads` 配置 |
| ADR-005 "HashJoinBridge" | `StateBridge` trait 占位，M3+ |
| ADR-006 "Arrow Flight shuffle" | 没变；`ExchangeKind::Local` 是 Pylon 加的优化项 |
| ADR-007 "Iceberg REST Catalog" | 没变 |
| ADR-008 "Substrait 不进 v1" | 没变 |

## 8. 实施 checklist (2026-08-18 done)

- [x] `pylon-runtime/src/pipeline.rs` — 新增 Pipeline struct + `run_pipeline_per_op_task`
- [x] `pylon-runtime/src/driver.rs` — 重写为 Driver + DriverMode
- [x] `pylon-runtime/src/op.rs` — 加 `is_blocked` 方法
- [x] `pylon-runtime/src/bridge.rs` — 新增 StateBridge trait
- [x] `pylon-runtime/src/lib.rs` — 重新导出所有 pub 类型
- [x] `pylon-worker` — 用新 Pipeline + Driver API，行为不变
- [x] `pylon-coord/src/{query,stage,task,scheduler,fragment}.rs` — 新增 5 模块
- [x] `pylon-coord/tests/coord_unit.rs` — 5 个单元测试全过
- [x] M1 smoke test 跑通：33,333 行 WHERE amount > 100000 不变

## 9. 已知 migration 影响

| 项 | 影响 |
|---|---|
| Op 实现 (`FilterOp` 等) | **零变动**。`PipelineOp::is_blocked` 用默认实现 |
| `pylon-worker` main.rs | **一行改动**：从 `Driver::new(ops)` 改为 `Pipeline::new(ops)` 然后 `Driver::new(pipeline)` |
| Benchmarks (TPC-H) | 待 M3+ 跑。结果作为 M5+ single-thread refactor 的 baseline |
| 文档（README / doc strings） | 已更新 |

## 10. Open issues 留给后续 RFC

| ID | 主题 | RFC |
|---|---|---|
| OQ-001 | SingleThreadLoop fused driver 实现细节 | RFC-0005（M5+） |
| OQ-002 | HashJoinBridge 具体实现 | 跟着 M3 join 实现一起出 |
| OQ-003 | ExchangeSink/SourceOp 实现 | RFC-0003（M2 起步） |
| OQ-004 | FTE snapshot + retry 协议 | RFC-0006（M4） |
| OQ-005 | TaskSpec gRPC wire format | M2 起步时按 `prost` 生成 |

## 11. Sign-off

Reference commit: 见 `git log --all --oneline | head`（commit 还没 push；用户 review 后正式 commit）。

Date: 2026-08-18
