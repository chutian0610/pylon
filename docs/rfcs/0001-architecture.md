# RFC 0001 — Pylon Query Architecture

- **Status**: Draft
- **Date**: 2026-08-18
- **Author**: Pylon Working Group
- **Discussion**: <private — pending review>

## 摘要

提议构建 **Pylon**: 一个全 Rust 实现的 Arrow-native MPP SQL 查询引擎，定位为 Trino/Presto 的低开销替代品。

本文档确立:
1. 项目目标 / 非目标
2. 关键架构决策
3. Crate 结构
4. 实现路线图

调研依据见 [docs/research/findings.md](../research/findings.md)。

## 1. 目标

### 1.1 In-Scope（必须能做的事）

1. 解析 ANSI SQL 子集（含 CTE / window / subquery）→ LogicalPlan → PhysicalPlan
2. 将 PhysicalPlan Fragment 化为 Stage DAG，每个 Stage 切成 N 个 Task
3. 调度 Task 在 worker pool 上运行（capacity-based）
4. 每个 worker 跑 pipeline driver loop（参考 Velox Driver 的 semantics）
5. Stage 间 exchange（partitioned / broadcast / gather / local 四种）
6. **FTE-equivalent 容错**：shuffle output 写到对象存储（S3/GCS/ADLS），task retry 仅 replay partition
7. 单 worker per-query 内存池 + spill-to-disk
8. Connector = 通过 Iceberg REST Catalog API 接入任意 catalog（Lakekeeper / Polaris / ...）
9. Parquet 表扫描（基于 `iceberg-rs + arrow-rs + parquet`）
10. SQL 客户端：JDBC（用 `caliburn-rs`）、HTTP REST、Prometheus 指标

### 1.2 Out-of-Scope（不在 v1 内）

- 多 coordinator HA（Raft 状态机等不写）
- CBO 优化器（v1 用 rule-based + runtime stats，未来再加）
- 分布式事务 / 跨表 snapshot
- 非 Iceberg 数据源（Delta/Paimon 留给 Iceberg REST 后端去做）
- 完整 ANSI SQL / PG 兼容（v1 子集，见 §5）

### 1.3 Non-Goals

- 不与 Spark / Ray / Dask 竞争批处理市场
- 不替代 OLTP 系统
- 不集成 Hive Metastore 直连（永远走 Iceberg REST）

## 2. 关键架构决策

### ADR-001: 三层架构，业务代码只在外两层

```
┌─────────────────────────────────────────────────────┐
│  coord (binary)                                     │
│  • 接受 client SQL                                  │
│  • 解析 → 优化 → Fragment                           │
│  • 调度 → 监控 query state                          │
│  • 不接触 RecordBatch（不参与数据流）                │
└─────────────────┬───────────────────────────────────┘
                  │ gRPC (control plane only)
                  ▼
┌─────────────────────────────────────────────────────┐
│  worker (binary)                                    │
│  • 接收 Task → 在 N 个 thread (= CPU 核数)          │
│  • 跑 M 个 pipeline driver                          │
│  • 各 Driver 持有一个 operator chain                │
│  • 通过 Arrow Flight 做 exchange                    │
└─────────────────────────────────────────────────────┘
```

**两个独立 binary** vs 单一 binary：是独立 binary。Coordinator 不持有数据路径，更易 HA 演化。

**理由**：和 Velox / Presto Native 同样的"coord + native worker"切开；不要重蹈覆辙做"smooshed together"。

### ADR-002: 用 arrow-rs 直接，不依赖 DataFusion

理由见 findings §7。**详细决定**：
- ❌ **不依赖** `apache/datafusion::physical_plan`（ExecutionPlan trait 及其全部实现）
- ❌ **不依赖** `apache/datafusion::optimizer` 整体（依赖太多）— 仅看或抄规则
- ✅ **可选**：`apache/datafusion::expr::*`（PhysicalExpr / ScalarUDF / 表达式求值）
- ✅ **直接依赖**：`arrow::compute::*`（stateless kernels）

> **复用总预算：30% 代码、80% 业务能力**

### ADR-003: 用 Velox Operator / Driver / Task 三件套作运行时参考，但重写

下面这些设计点直接照抄 Velox Contract：

```rust
#[async_trait]
trait PipelineOp: Send + Sync {
    fn name(&self) -> &'static str;
    /// Driver 询问上游是否需要送一个 batch 到本算子
    async fn needs_input(&self) -> bool;
    /// Driver 推进上游的一个 batch 进入本算子
    async fn add_input(&mut self, batch: RecordBatch) -> Result<()>;
    /// Driver 询问本算子是否产生了一个 batch 给下游
    async fn get_output(&mut self) -> Result<Option<RecordBatch>>;
    /// Driver 通知本算子上游已无更多输入（build side END）
    async fn no_more_input(&mut self) -> Result<()>;
    /// Driver 询问本算子是否已结束（可释放）
    async fn is_finished(&self) -> bool;
    /// Driver 询问本算子是否在阻塞 IO（用于 backpressure）
    async fn is_blocked(&self) -> Result<bool>;
    /// Driver 释放算子全部资源（最后调用一次）
    async fn close(&mut self) -> Result<()>;
}
```

> 与 Velox 6 个 pure-virtual 对应：needsInput / addInput / getOutput / noMoreInput / isFinished / isBlocked。

### ADR-004: Doris 风格"固定线程池"

- worker 启动时确定 thread 数 = min(CPU_count, **16**)
- 每个 thread 持有一个 driver loop
- driver loop 在 ready queue 拉 driver 跑
- 阻塞 IO 算子必须显式告诉 driver 本帧不抢 thread（用 `is_blocked` + tokio mpsc 待有数据再唤起）

**禁止**为每个 query 创建 OS thread。

### ADR-005: HashJoinBridge 借鉴 Velox 的"build / probe 拆成两个算子，bridge 共享"

```rust
struct HashJoinBridge {
    table: ArcSwap<HashTable>,
    spill_partitions: Mutex<Vec<SpillPartition>>,
    state: AtomicState, // Building | Probing | Restoring
}

trait HashBuild: PipelineOp {
    fn bridge(&self) -> Arc<HashJoinBridge>;
}
trait HashProbe: PipelineOp {
    fn bridge(&self) -> Arc<HashJoinBridge>;  // 与 HashBuild 共享同一 Arc
}
```

### ADR-006: 用 Arrow Flight 做 shuffle

- 每个 partition = 一个 Flight stream
- Producer side 是 task 的最后一个 driver
- Consumer side 是 task 的第一个 driver
- 服务端长连接保持 (gRPC keep-alive)
- 写入即 commit（gRPC 状态完成 = 已落到对端 kernel buffer）
- FTE snapshot 落 S3：每个 producer 把输出额外 snapshot 到 `/pylon/query/<qid>/stage/<sid>/partition/<pid>.arrow`

> 注意：我们**不依赖**某个 third-party Flight server，自己实现一个窄端就行，作为 Pylon worker 进程内的一个 Rust module。

### ADR-007: Catalog = Iceberg REST Catalog 抽象 + 推荐 Lakekeeper

```rust
#[async_trait]
trait Catalog: Send + Sync {
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>>;
    async fn list_tables(&self, ns: &NamespaceIdent) -> Result<Vec<TableIdent>>;
    async fn load_table(&self, ident: &TableIdent, credentials_vended: bool) -> Result<Table>;
    async fn commit(&self, ident: &TableIdent, requirement: &[Requirement], update: Update) -> Result<Table>;
}

struct LakekeeperClient {
    base_url: Url,
    http: reqwest::Client,
    access_token_provider: Arc<dyn AccessTokenProvider>,
    vended_creds: bool,
}
```

把 `LakekeeperClient` 作为 reference impl；其次 Polaris Client 同形。

### ADR-008: Substrait 作为可选 Plan serial format（不进 v1）

- v1 自有 IR：`pylon-plan` crate 定义 PhysicalPlan trait 与变体
- v1 不与 Substrait 关联
- 理由：v1 阶段输出 Substrait 没有必要（自满足）— 等生态到了再考虑 exporter/importer

## 3. Crate 结构

```
pylon/
├── Cargo.toml                  # workspace root
├── crates/
│   ├── pylon-plan/             # SQL → LogicalPlan → PhysicalPlan
│   │    ├── src/lib.rs
│   │    ├── parser/            # sqlparser-rs + dialect
│   │    ├── logical/           # LogicalPlan enum, expr
│   │    ├── optimizer/         # Rule-based optimizer
│   │    └── physical/          # PhysicalPlan enum + Fragmenter
│   │
│   ├── pylon-runtime/          # Pipeline execution (核心)
│   │    ├── src/lib.rs
│   │    ├── driver.rs          # Driver loop
│   │    ├── task.rs            # Task lifecycle
│   │    ├── ops/               # 算子实现
│   │    │    ├── filter.rs
│   │    │    ├── project.rs
│   │    │    ├── aggregate.rs  # hash agg
│   │    │    ├── join.rs       # build + probe + bridge
│   │    │    ├── sort.rs
│   │    │    ├── source.rs     # 通用 SourceOp
│   │    │    ├── sink.rs
│   │    │    └── exchange.rs   # partitioned / broadcast source
│   │    ├── memory.rs          # per-task memory pool
│   │    ├── spill.rs           # disk spill manager
│   │    └── barrier.rs         # LocalBarrierManager (RisingWave-style)
│   │
│   ├── pylon-exchange/         # Arrow Flight server/client
│   │    ├── src/
│   │    │    ├── flight_server.rs
│   │    │    ├── flight_client.rs
│   │    │    └── fence.rs      # FTE snapshot to S3
│   │
│   ├── pylon-catalog/          # Iceberg REST Catalog abstraction
│   │    ├── src/
│   │    │    ├── mod.rs
│   │    │    ├── rest.rs       # REST 协议
│   │    │    ├── lakekeeper.rs # Lakekeeper 实现
│   │    │    ├── polaris.rs    # Polaris 实现 (auth 略有不同)
│   │    │    └── memory.rs     # 测试用 in-memory
│   │
│   ├── pylon-iceberg/          # Iceberg 表读 / 写 (基于 iceberg-rs)
│   │    ├── src/
│   │    │    ├── reader.rs     # Iceberg → RecordBatch
│   │    │    ├── writer.rs     # RecordBatch → Iceberg (v1 read-only)
│   │    │    └── stats.rs      # column statistics extraction
│   │
│   ├── pylon-storage/          # 对象存储抽象 (基于 object_store)
│   │    ├── src/
│   │    │    ├── s3.rs
│   │    │    ├── gcs.rs
│   │    │    └── adls.rs
│   │
│   ├── pylon-coord/            # Coordinator binary
│   │    ├── src/
│   │    │    ├── main.rs       # tokio main
│   │    │    ├── planner.rs    # 接收 SQL → pylon-plan
│   │    │    ├── scheduler.rs  # Stage scheduling
│   │    │    ├── rest_api.rs   # HTTP API (HTTP/2)
│   │    │    └── catalog_proxy.rs  # 多租户 catalog credentials
│   │
│   ├── pylon-worker/           # Worker binary
│   │    ├── src/
│   │    │    ├── main.rs
│   │    │    ├── task_runner.rs
│   │    │    ├── driver_pool.rs
│   │    │    └── metric_emit.rs
│   │
│   └── pylon-types/            # 共享基础类型
│        ├── src/
│        │    ├── arrow_ext.rs  # Schema / RecordBatch 工具方法
│        │    ├── error.rs
│        │    └── arena.rs      # per-task memory arena
│
├── crates/external/  # 严格区分我们的代码与第三方
├── tests/             # 集成测试
├── benches/           # 性能基准 (TPC-H)
├── docs/              # 文档与 RFC
└── README.md
```

## 4. 错误模型与可观测性

- 所有错误用 `snafu` crate 自定义 `PylonError { variant, source }`
- 所有可恢复错误驱动 retry；不可恢复错误 logging + 终止 query
- Metrics 用 `metrics-rs`，原生 emit 到 Prometheus
- Tracing 用 `tracing` + `tracing-subscriber`；OpenTelemetry 可选

## 5. v1 SQL 子集

支持（必须可工作）：

```sql
SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
JOIN: INNER / LEFT / RIGHT / FULL OUTER, ON <expr>
Aggregate functions: COUNT, SUM, AVG, MIN, MAX, + ARRAY_AGG, approx_percentile
Window: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE
CTE (WITH)
Subquery (correlated, non-correlated)
Set ops: UNION / INTERSECT / EXCEPT
Type casts and predicates (BETWEEN, IN, IS NULL, LIKE)
DML: INSERT INTO <iceberg-table> SELECT ...   (仅 basic)
DDL: CREATE TABLE / DROP TABLE / SHOW TABLES (via catalog)
```

不支持（v1+）：

- Recursive CTE
- Lateral joins
- 系统级 UDF（v1 只支持注册式纯函数 UDF）
- 任何非 Arrow 类型（如 JSON 半结构化类型可以放在 v1+）

## 6. 实现路线图

**Phase 0 — Setup (Week 1–2)**
- Workspace skeleton
- CI (GitHub Actions: lint / test / format)
- 把所有 ADR 写成 `docs/rfcs/0001-...000N`

**Phase 1 — Single-worker pipeline runtime (Week 3–8)**
- `pylon-plan` 中 fragmenter + 简单 SQL parser
- `pylon-runtime` 中 driver loop + PipelineOp trait
- 实现 `FilterOp` / `ProjectOp` / `SeqScanOp`
- 单 fragment + 单 task，跑通 `SELECT * FROM t` 在示例 Parquet 表上
- 验证：相比 Trino 9960 + Java worker 的同等 query，处理时间在一个数量级以内（起步允许 2x gap）

**Phase 2 — Multi-worker + Basic exchange (Month 3–4)**
- `pylon-exchange` 中 Arrow Flight server/client
- `pylon-coord` 中 scheduler 雏形
- hash partitioned exchange：两 stage join
- 单 coordinator（无 HA），用 in-memory 拓扑（暂时不接 storage）

**Phase 3 — Iceberg + REST Catalog (Month 5–6)**
- `pylon-catalog` Lakekeeper client
- `pylon-iceberg` reader
- End-to-end: SELECT ... FROM iceberg-table 在 Lakekeeper 后端
- 跑 TPC-H SF100

**Phase 4 — FTE + Spill (Month 7–9)**
- `pylon-exchange` 中 fence / snapshot 到 S3
- `pylon-runtime` 中 memory pool + spill manager
- 故障注入测试：kill worker → query 不中断继续

**Phase 5 — Hardening + 第二个 connector (Month 10–12)**
- JDBC connector（用 caliburn-rs）
- 安全/认证层（OAuth/OIDC 集成）
- Coordinator HA（Raft）

**Phase 6 — Open-source 准备 (Month 12+)**
- Apache 2.0 license
- 项目命名、CI / 测试矩阵 / docs site（typedoc / mdbook）
- 至 ASF incubation 或 CNCF sandbox

## 7. 测试

- **单元**：每个 op / 每个 fragmenter rule / scheduler rule
- **集成**：完整 SQL 端到端，多 worker 部署
- **Conformance**：SQL Logic Test (SLT) 子集（`sqllogictest-rs`）做 ANSI 风格合规
- **性能**：TPC-H SF100 / SF1000，对比 Trino release，当前数字与 Dollar / CPU
- **故障注入**：随机 kill worker / 杀 coordinator proxy，验证 query 可恢复

## 8. 跨切关注

- **observability**: structured logging, metrics, traces 都必须从 day-one
- **build & deps**: Cargo workspace + 本地 editions = 2024 edition
- **MSRV**: 与 Rust 1.85+ 对齐
- **license**: Apache-2.0 (与 arrow-rs / DataFusion / Iceberg 一致)
- **CI**: Linux (Ubuntu 22.04, latest GCC + musl) + macOS (aarch64)

## 9. 决策记录更新规则

任何对 ADR 的修改：
1. 提交 RFC PR 由工作组批准
2. RFC 被合并 / 拒绝进 git 仓库
3. 编号永不重用，被拒绝的进 `docs/rfcs/rejected/` 备查

## 10. 开放问题（待 M1 阶段回答）

| ID | 问题 | 负责人 | 解决时间 |
|---|---|---|---|
| OQ-01 | 用 `tokio` 还是 `monoio`/`glommio`？tokio 更普及但 glommio 更精确控制 | TBD | Phase 1 |
| OQ-02 | sql parser：`sqlparser-rs` vs 自写 4k 行 LALR | TBD | Phase 1 |
| OQ-03 | Spillable hash agg 还是 streaming hash agg（hash table rolling + LSD radix）？ | TBD | Phase 4 |
| OQ-04 | Substrait 出口做不做（让别的 engine 能消费 Pylon plan）？ | TBD | Phase 6 |
| OQ-05 | 是否要支持 Iceberg V3 (v3 row lineage / variant) | TBD | Phase 5+ |
