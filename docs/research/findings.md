# Research Findings (2026-08-18)

调研日期：2026-08-18
调研目的：在动手写任何代码前，确认每个依赖、参考实现、规范协议的当前状态。
所有结论对应源码 / docs.rs / 官方 spec，可在每个章节末找到引用。

---

## 1. arrow-rs — Compute 模块现状

**关键事实**：
- `arrow` crate 当前版本 **59.2.0** (2026-08-06 发布)
- `arrow::compute::*` 模块是**对外暴露的纯函数集合**，每个函数都是 `fn(RecordBatch, ...) -> Result<RecordBatch>` 形态
- **不需要 DataFusion 也能独立调用**，因为 compute 模块本身是 stateless kernel 包

**可独立复用、无需 DataFusion 的算子清单**（来自 compute module 文档）：

| Kernel | 形态 | 备注 |
|---|---|---|
| `kernels::filter` | `fn(&RecordBatch, &BooleanArray) -> Result<RecordBatch>` | 即 where 子句执行 |
| `kernels::take` | `fn(&RecordBatch, &UInt32Array) -> Result<RecordBatch>` | 用于 hash join probe |
| `kernels::cast` | type coercion | |
| `kernels::substring / length / ...` | 标量函数 | |
| `kernels::sort_to_indices` | 返回 indices，配合 take 用 | |
| `kernels::hash / hash_combine` | 给 hash join / agg | |
| `kernels::topk` | partial sort | |
| `kernels::partitioning` | partitioned shuffle helper | |
| `kernels::zip` | struct 构造 | |
| `kernels::regexp_match` | 正则 | |

**这一节对我们 Pylon 的实际意义**：
- compute kernels 是 **arrow-rs 自带**，不需要 DataFusion 参与
- 我们可以**直接用 `arrow::compute::*`**，绕过 DataFusion 的 ExecutionPlan
- hash-aggregate 这种 stateful 操作要自己写，但**底层 hash / sort / take 都可用**
- quote: "Apache Arrow v25.0.1 spec" — Flight IPC 协议在 Apache Arrow 项目里独立演化

**Source**:
- <https://docs.rs/arrow/latest/arrow/compute/index.html>
- <https://arrow.apache.org/docs/format/Flight.html>

---

## 2. iceberg-rs (Apache Iceberg Rust SDK)

**关键事实**：
- 当前版本 **0.10.1** (2026-08-01 发布)
- 项目名：**"Apache Iceberg Official Native Rust Implementation"**
- 仓库归属 ASF incubator 进程
- 直接依赖 `arrow-array ^58` (与上游 arrow-rs 对齐)

**Surface Area**：
| Trait / Type | 说明 |
|---|---|
| `iceberg::Catalog` | catalog 抽象 |
| `iceberg::CatalogBuilder` | 多种 catalog 实现 (Rest / S3Tables / Glue / Nessie / Hive) 的统一 builder |
| `iceberg::Table` | 表操作 |
| `iceberg::TableScan` | 扫描构建 |
| `iceberg::arrow` (子模块) | **arrow-native reader**，直接吐 `RecordBatch` |

**对我们的意义**：
- ✅ 可以依赖，但 **0.10.x 仍属早期 alpha**，生产化建议绑死某个 minor 版本
- ✅ Iceberg **arrow reader** 允许我们**跳过**自己写 Parquet → RecordBatch 转换
- ✅ catalog 抽象已经对接了 RestCatalog — 配合 Apache Polaris / Lakekeeper 立即可用
- ⚠️ 不支持 Iceberg V3 完整 spec（V3 stable 时间点待确认，需要在 PoC 阶段验证）
- ❌ 没有 table write 完整路径（write-staging 仍需 RustIceberg 自己拼）

**Source**: <https://docs.rs/iceberg/latest/iceberg/>

---

## 3. Apache Polaris (incubating)

**关键事实**：
- 当前 ASF 项目状态：incubating（不是 TLP）
- 核心定位：**"open catalog for data and AI assets"** — Snowflake 捐给 ASF
- 实现的是 Iceberg REST Catalog API + 加 RBAC / Principal / Credential Vending

**实现范围**（相对 Iceberg REST Catalog OpenAPI 193 KB spec）：
- ✅ catalog/namespace/table 全套 REST API
- ✅ Server-side credential vending for S3/GCS/ADLS
- ✅ Principal model (service principal)
- ✅ Role-based access control at catalog/namespace/table level
- ✅ External IdP federation

**对我们的意义**：
- ✅ 完全是我们的"category 之外"的服务，**与其解耦**
- 缺点：要部署一个 JVM 服务 (Spring Boot)；运维成本高
- 优点：治理能力 (RBAC) 完整
- ⚠️ standalone 部署时通常要求 Kubernetes / 较强基础设施

**Source**: <https://polaris.apache.org/>

---

## 4. Lakekeeper (Rust OSS Iceberg Catalog)

**关键事实**：
- 当前版本：迭代活跃中（Apache Iceberg REST Catalog 兼容）
- 描述：**"A secure, fast, and user-friendly Apache Iceberg REST Catalog built with Rust"**
- 商业模式：开源 + **Vakamo 公司提供 "Lakekeeper+" enterprise** 支持

**实现范围**：
- ✅ Apache Iceberg REST Catalog API (Polaris 兼容)
- ✅ Server-side credential vending
- ✅ Apache Arrow integration
- ✅ 单一 Rust binary 部署（不依赖 JVM）

**对我们的意义**：

> **这是我们项目的 "Best fit" 搭档**——开源、纯 Rust、与 Apache Polaris 同套 API、不引入 JVM。单 binary 部署就是最佳 demo 形态。

- ⭐⭐⭐⭐⭐ 推荐作为 Pylon 项目的默认 catalog 后端
- 需要验证：RBAC 完整度（如果不急用，仍可用 GLue / Unity Catalog 兜底）

**Source**: <https://docs.lakekeeper.io/>

---

## 5. Substrait

**关键事实**：
- 定位（来自 substrait.io 主页）：*"Substrait is a format for describing compute operations on structured data. It is designed for interoperability across different languages and systems."*
- 三大组成：
  1. 形式规范 (formal spec)
  2. 人类可读的文本 (textproto)
  3. 紧凑的跨语言二进制 (proto bytes)

**核心 schema 来源**（来自 GitHub protos）：
| 文件 | 含义 | 我们关心什么 |
|---|---|---|
| `proto/substrait/algebra.proto` | 关系代数运算符 (Read/Project/Filter/Join/Aggregate/Sort/...) 108 sections, 69 KB | **是物理算子的 wire format** |
| `proto/substrait/plan.proto` | Plan = 一组 relations + 顶层 advanced extensions 23 sections | plan serialization |
| `proto/substrait/type.proto` | types：和 arrow types 对齐，可 zero-copy | |

**关键澄清**（回答之前用户的问题）：
> **Substrait 不是 logical plan 的抽象；它是 physical plan 的 wire format。**

- Substrait 的 relations 直接表达算子（Project / Filter / HashJoin / Aggregate / Window / Sort / Exchange / Fetch...）
- 没有 Substrait "Logical" 概念，**只有一种 Plan**，对应到 **执行层算子**
- "Logical plan" 仍是各 SQL 方言 / 各 engine 自己的事，Substrait 试图标准化的是从 logical/physical 转换后的**可序列化形式**（即 "执行计划"）

**对我们的意义**：
- ✅ 产出 Substrait proto 的 planner（来自 logical 转 physical）= **engine-agnostic IR**
- ✅ 把 Substrait as protobuf 输出给 distributed worker = scheduler / worker 解耦
- ⚠️ Substrait v0.x 仍变化中，consumer 多于 producer；选择依赖版本时要固定
- 备选/补充：Trino 用 **TTransactionalPlan** 自家 wire format，不依赖 Substrait

**Source**:
- <https://substrait.io/>
- <https://cdn.jsdelivr.net/gh/substrait-io/substrait@main/proto/substrait/algebra.proto>
- <https://cdn.jsdelivr.net/gh/substrait-io/substrait@main/proto/substrait/plan.proto>

---

## 6. Velox (Meta Presto Native Execution Engine)

**关键事实**：
- 当前状态：**VeloxCon 2026 已举办**——项目健康活跃
- 定位（来自官网）："high-performance, open-source execution engine designed for composable data systems"
- 与 Presto coordinator 的关系：保留 planner + Java 调度器，worker 用 Velox (C++) 替换 Java 执行

**Operator 接口契约**（来自 `velox/exec/Operator.h`）：
- 6 个 pure virtual 方法定义完整生命周期：
  - `needsInput() -> bool`
  - `addInput(input)`
  - `getOutput() -> Output`
  - `noMoreInput()`
  - `isFinished() -> bool`
  - `isBlocked() -> ContinueFuture` (返回 future 表示当前 data 还没到)
  - `close()`
- 注释中提到的 Presto 同名概念对应：`HashJoinBridge`、`Exchange`

**Driver 循环契约**（来自 `velox/exec/Driver.h`）：
- Driver 内部枚举方法（用于 profiling / debug）：
  - `kOpMethodIsBlocked` / `kOpMethodNeedsInput` / `kOpMethodGetOutput` / `kOpMethodAddInput` / `kOpMethodNoMoreInput` / `kOpMethodIsFinished`
- Driver 状态字段：
  - `barrier_` — barrier 处理 state（用于 fault-tolerant 协调）
  - `totalDriverBlockedNanos_` — 累计阻塞时间（自适应调度依据）
- Driver 生命周期与 Task 联动:
  - `CancelGuard` class — 每个 Driver 移除时通知 Task thread count 调整
  - `startBarrier()` — 由 Task 触发 Driver 处理 barrier

**HashJoinBridge 契约**（`velox/exec/HashJoinBridge.h`）：
- 注释直接引用 **"Corresponds to the Presto concept of the same name"**
- 三个参与方：HashBuild (在 build pipeline) + HashProbe (在 probe pipeline) + HashJoinBridge (共享 state 对象)
- 关键方法：
  - `setHashTable(unique_ptr, spillPartitionSet, hasNullKeys, spillFunc)`
  - `setHashTable(shared_ptr<wave::HashTableHolder>)` — broadcast join 用
  - `probeFinished(restart)` — 让 build side 在 spill 时重 build
- **支持 mixed grouped execution** (grouped probe + ungrouped build)

**Exchange 模式**（来自 `velox/exec/Exchange.h`）：
- 4.5 KB 单一 header，简洁、focused
- Exchange 是 source operator 的一种：通过远程 shuffle source 反序列化

**对我们的意义**：
> **Operator / Driver / Task 三件套 = 我们 Pylon runtime 的直接参考实现**
> **API 表层 1:1 翻译成 Rust + arrow 即可**

**Source**:
- <https://cdn.jsdelivr.net/gh/facebookincubator/velox@main/velox/exec/Operator.h>
- <https://cdn.jsdelivr.net/gh/facebookincubator/velox@main/velox/exec/Driver.h>
- <https://cdn.jsdelivr.net/gh/facebookincubator/velox@main/velox/exec/HashJoinBridge.h>
- <https://cdn.jsdelivr.net/gh/facebookincubator/velox@main/velox/exec/Exchange.h>
- <https://cdn.jsdelivr.net/gh/facebookincubator/velox@main/velox/exec/Driver.cpp>
- <https://velox-lib.io/>

---

## 7. Apache DataFusion (再确认)

**关键事实**：
- 当前版本 **54.1.0**
- 定位：*"DataFusion is an in-memory query engine that uses Apache Arrow as the memory model"*
- **划清界限**：
  - `LogicalPlan` / 表达式求值 / 优化器规则：很多可独立于 ExecutionPlan 用
  - `ExecutionPlan` 整体（`physical_plan` 模块）：pull-stream 抽象，**不应复用**

**结论**（重复之前的论点）：
- ❌ `physical_plan::execute()` — 不能复制
- ⚠️ `physical_plan::joins::*` — kernel 部分可剥 stateless 算子代码
- ✅ `expr::*` (PhysicalExpr) / `scalar_*` — 表达式部分可借
- ✅ Type system / Schema / array builders — 直接来自 arrow-rs，不涉及
- ✅ Optimizer rules (predicate/projection pushdown) — 可作独立 crate

**Source**:
- <https://docs.rs/datafusion/latest/datafusion/>
- <https://docs.rs/datafusion/latest/datafusion/physical_plan/index.html>
- <https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/index.html>

---

## 8. Apache Doris Pipeline Execution

**关键事实**（来自 "evolution of the Apache Doris execution engine" 2024-06-18 blog post）：
- 自 **2.0.0**（2022 末）引入 Pipeline Execution Engine 替换 Volcano Iterator
- 自 **2.1.0**（2023）升级为 **PipelineX**，并成为默认

**核心动机**（blockquote）：
> "If too many threads are blocked, the thread pool will be saturated and unable to respond to subsequent queries. Thread scheduling is entirely managed by the operating system, without any user-level control or customization."

**核心设计原则**（直接对应我们的 Pylon 决策）：
1. **固定 thread pool size = CPU core 数**
2. **把易阻塞的算子（IO/RPC）拆为独立 pipeline task**
3. **user-space polling scheduler**（不用 OS thread 调度）

**PipelineX 解决了**：
- Limited execution concurrency
- High execution overhead
- High scheduling overhead
- Poor readability of operator profile

**Local Shuffle**（一个细节值得注意）：
- Doris 的 fragment concurrency 受 tablet 数量限制 → 引入 local shuffle key 重新分桶
- Pylon 可以跳坑：从一开始用**调度器自由决定 partition 数**

**对我们的意义**：
- 这和我们之前讨论的方案在工程细节上完美一致
- 唯一新提示：**"fixed thread pool = N CPU"** 这一条值得作为 Pylon 的硬性约束写进 RFC

**Source**: <https://doris.apache.org/blog/evolution-of-the-apache-doris-execution-engine/>

---

## 9. RisingWave Actor Model

**关键事实**（来自 `risingwave/src/stream/src/executor/actor.rs`）：
- `Actor<C>` 是 streaming framework 的**基本执行单元**
- 内部字段：
  - `consumer: C` — StreamConsumer
  - `subtasks: Vec<SubtaskHandle>` — 并发子任务
  - `actor_context: ActorContextRef`
  - `LocalBarrierManager` — **局部 barrier 管理**
- 注释引用："Drop the stream in a blocking task to avoid interfering with other actors."
- **模式**：用 `tokio::task::spawn_blocking` 处理 CPU-bound drop

**对我们的意义**：
- ✅ **验证了一个正确事实**：Rust streaming actor model 适合我们的 driver loop
- ✅ **`LocalBarrierManager` 概念**可以拿来 → Pylon 的 worker 内 per-driver barrier coordinator
- ⚠️ RisingWave 是 streaming SQL 引擎（非 interactive query），他们的 actor 是 per-stream-actor，Pylon 是 per-task-driver，颗粒度不同
- ⚠️ 他们的 barrier semantics 是 per-epoch (1 次/秒)，Pylon 是 per-query，频率差 10⁶ 倍

**Source**:
- <https://cdn.jsdelivr.net/gh/risingwavelabs/risingwave@main/src/stream/src/executor/actor.rs>

---

## 10. Trino FTE (Fault-Tolerant Execution)

**关键事实**（来自 Trino docs "fault-tolerant-execution.html"）：
- 默认行为：worker 失败 → query 整体失败，需要重跑
- FTE 开启：worker 失败 → 仅 retry 受影响 partition
- FTE 利用 **ExchangeSink operator 把 shuffle output 写到 coordinator 管理的存储**（典型是 S3）
- **retry 时只需从 shuffle storage 重读，不再重新跑整个上游**

**SPI 接口契约**（来自 `trino-spi/src/main/java/io/trino/spi/exchange/ExchangeSource.java`）：
- 提供 `ExchangeSource` 的抽象给 coordinator，server 实现用于调度 retry
- 注释："Used in fault-tolerant execution"

**Exchange SPI**（来自 `ExchangeManagerRegistry.java`）：
- `ExchangeManagerRegistry` 是 SPI registry — 在 coordinator 端注册 ExchangeManager
- 多个实现可插拔：FileSystem (S3) / HDFS / InMemory（testing）

**对我们的意义**：
- 这是 Pylon Exchange 层**可直接借鉴的架构模型**
- 因为我们没有 Trino 的 multi-coordinator HA 历史负担，FTE 可以**更简单**——单 coordinator + S3 shuffle storage 起步

**Source**:
- <https://trino.io/docs/current/admin/fault-tolerant-execution.html>
- <https://cdn.jsdelivr.net/gh/trinodb/trino@master/core/trino-spi/src/main/java/io/trino/spi/exchange/ExchangeSource.java>
- <https://cdn.jsdelivr.net/gh/trinodb/trino@master/core/trino-main/src/main/java/io/trino/exchange/ExchangeManagerRegistry.java>

---

## 11. Apache Arrow Flight Protocol

**关键事实**（来自 Apache Arrow spec "Flight.html"）：
- Flight RPC 是 gRPC + Arrow IPC 的 binding
- 核心抽象：
  - `FlightDescriptor` — 一段数据的标识（命令 / path / 其它）
  - `Put / Get / DoExchange` — 双向 streaming operations
  - `DoExchange` 适合双向流式交互（producer-consumer 长连接）
- 一个 stream 由 message 序列组成，每个 message 是 `RecordBatch` or 头部尾标记

**协议和我们 shuffle 的契合度**：
- 一个 shuffle partition = 一个 Flight stream
- Producer push, consumer pull；天然支持 batching
- metadata 通过 `FlightDescriptor` 携带 plan/operator/partition 标识

**对我们的意义**：
- ✅ Flight 是**正确选择**（更优于手动用 protobuf）
- ✅ 可以在 `DoExchange` 之上实现**两边互相发起**的 interactive exchange 模式（用于混合 broadcast/partitioned）
- ⚠️ HTTP/2 流量调度是 gRPC 默认行为，但 shuffle 是 fire-and-forget 流；需要服务端长连接保活机制

**Source**: <https://arrow.apache.org/docs/format/Flight.html>

---

## 12. Nessie / Unity Catalog / Gravitino（轻比较）

**Nessie**:
- *"Project Nessie is a Transactional Catalog for Data Lakes with Git-like semantics"*
- 有 Iceberg / Delta / Hudi 集成
- 特色：Git-like 分支/标签/历史，跨 catalog 操作可事务
- **对我们的适合度**：⭐⭐ (引入 git 语义对我们 overkill；catalan 只读 metastore 不需要)

**Unity Catalog** (Databricks):
- Iceberg / Delta 通用，多元治理
- 与 Databricks/Fabric 生态深度绑定
- **对我们的适合度**：⭐⭐ (厂商控制)

**Apache Gravitino**:
- ASF 顶级项目孵化中
- 定位：跨所有 catalog source 的"元数据联邦层" (single point)
- 支持 Iceberg / Hive / Paimon / MySQL / PG 等
- **对我们的适合度**：⭐⭐⭐ (如果你需要 Paimon、Hive、JDBC 一起查，gravitino 是唯一选择；否则 overkill)

**catalog 选型最终决策（与之前一致）**：

```
Default:         Lakekeeper (Rust OSS)
Alt:             Apache Polaris (RBAC + 跨引擎时)
如果生产已经用:   任何 Iceberg REST Catalog 实现都可以
如果你用 Paimon / Hive 多套: Gravitino 联邦
如果你要 branch semantics: Nessie (允许但付出复杂度)
```

---

## 总结: 三层依赖

```
┌─────────────────────────────────────────────────────────────┐
│  Pylon Application Crate (我们写)                            │
│  • SQL parser / Logical planner / Fragmenter / Scheduler     │
│  • Runtime (Driver / Task / Op)                              │
│  • Exchange (Arrow Flight)                                   │
│  • Catalog Client (REST → Iceberg REST)                       │
└─────────────────────────────────────────────────────────────┘
                            │
       ┌────────────────────┼────────────────────┐
       ▼                    ▼                    ▼
   库（直接 import）     协议（理解 schema）   服务（HTTP）
   • arrow-rs           • Arrow Flight        • Lakekeeper (catalog)
   • parquet            • Substrait (option)  • Apache Polaris (alt)
   • iceberg-rs (alpha) • Iceberg REST        • Arrow Flight Server (Shuffle)
   • sqlparser-rs       • OpenAPI 自带的 S3
   • object_store
   • tokio / tower
```

**结论**：
1. 核心计算 = arrow-rs（pure kernels）
2. 物理 plan serialization = 自定 IR + 可选 Substrait
3. Catalog = Iceberg REST Catalog 协议（任意实现后端）
4. Shuffle = Arrow Flight + 自定义 ExchangeSink
5. 流执行模型 = Velox (Operator/Driver/Task) 1:1 翻译 + Doris "fixed threadpool" + RisingWave LocalBarrierManager
