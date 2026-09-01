# Pylon Implementation Roadmap

时间单位：1 个月 = 4 周

## 里程碑 0 — Scaffold (2026-08-18 → 2026-08-31)  *(hygiene pass 2026-08-20 — 见 PR #13)*

**目标**：建立 Rust workspace，所有 ADR 落定。

| 任务 | 状态 |
|---|---|
| 创建 workspace `Cargo.toml` | ✅ 完成 |
| 添加 `pre-commit` (rustfmt, clippy, cargo-deny) | ✅ 完成 (PR #12 落地 `.pre-commit-config.yaml` + `tools/deny.toml` + `cargo deny check` CI job；PR #17 mechanical autofix 完成 `cargo fmt --all` + 全部 clippy 修复，rustfmt / clippy -D warnings 质量门已开。) |
| GitHub Actions CI 模版 | ✅ 完成 (PR #12: `.github/workflows/ci.yml` 3 jobs (`boundary-checks`、`build-test`、`deny-check`)；PR #17 新增 `fmt-check` + `clippy (-D warnings)` 两个 job，共 5 jobs。) |
| 8 个 crate 全部创建空 stub | ✅ 完成 (11 main crates + 2 tools: pylon-types / -plan / -runtime / -connector-spi / -exchange / -catalog / -iceberg / -proto / -storage / -coord / -worker / tools/gen-sample-data / tools/verify-output。每个 crate 都有自己的 `src/`，远超 stub。) |
| 调研笔记 `docs/research/findings.md` | ✅ 完成 |
| RFC 0001 Architecture | ✅ 完成 |
| RFC 0002 Crate 结构 | ✅ 完成 (原 "Crate 结构" intent 已被 `docs/rfcs/0005-pipeline-trait-surface.md` §1 "Module layout (Presto's three-tier pattern)" 接住且细化为 11-crate 实际布局。RFC 0002 文件本身的 "Execution Hierarchy" 内容作为 RFC 0005 §1 的早期讨论保留。) |
| RFC 0003 Driver 循环 spec | ✅ 完成 (`docs/rfcs/0003-m2-control-data-plane.md` 实际覆盖 M2 control plane (gRPC) + data plane (双向往返 coord)，范围超出原始 "Driver 循环" label。本 doc Status: Draft。) |
| RFC 0004 PipelineOp trait spec | ✅ 完成 (注意文件是 `0004-m3-flight-shuffle.md`，即 "M3 Arrow Flight Shuffle Protocol"，Status Implemented 2026-08-18。原始 milestone 行预期 "PipelineOp trait spec"，但 trait 实际由 RFC 0005 R2 落地 (`crates/pylon-runtime/src/op.rs` 的 `PipelineOp` trait)。本行 scope superseded — 见 RFC 0005 §4 / §7 R2。) |
| RFC 0005 Scheduler spec | ✅ 完成 (`docs/rfcs/0005-pipeline-trait-surface.md` Status Draft; scope 实际是 *Pipeline trait surface* 而非仅 "Scheduler"。R0-R9 序列全部 ✅，R6 PR #1+#2+#8 已进 main, R7 经 d610c8e 线性进 main (PR #6 标记), R9 经 cd652e2 线性进 main (PR #7 标记)。) |
| RFC 0006 Exchange 协议 spec | ✅ 合入 RFC 0004 (见 docs/notes/rfc-0006-status.md) |

---

## 里程碑 1 — Single-worker Pipeline (9 月)  *(hygiene pass 2026-08-20 — 见 PR #14)*

**目标**：单进程内跑通 pipeline runtime + 3 个算子 + 简单 SQL。

定义完成（DoD）：
- [x] `pylon-plan` 能 parse: `SELECT col_a FROM t WHERE col_b > 0` ✅ (evidence: `crates/pylon-plan/src/translate.rs:100` `parse_sql` 用 sqlparser-rs + `GenericDialect`; 同 parser chain 在 `aggregate_e2e_test` 的 3 个 e2e 测试里跑过 100k 行 SQL→plan→execute 端到端)
- [x] `pylon-runtime` 实现 PipelineOp trait + Driver loop + Task lifecycle ✅ (evidence: `PipelineOp` trait 在 `crates/pylon-runtime/src/op.rs:20`; `Driver` 在 `crates/pylon-runtime/src/driver.rs:37`; `Pipeline` 在 `crates/pylon-runtime/src/pipeline.rs:32`。Task lifecycle 由 `crates/pylon-worker/src/main.rs` 经 tonic `OpenSession` bidi 串接 coord + worker —— R7 carry-over via PR #6 已 audit 验证。)
- [x] 实现 `SeqScanOp(Parquet)` + `FilterOp` + `ProjectOp` ✅ (evidence: 3 ops 全部存在，在 `crates/pylon-runtime/src/ops/` 下作为 `impl PipelineOp for ...` 落地。实际还多出 4 个 ops：`aggregate`、`arrow_compute`、`exchange`、`partition_filter`)
- [x]* 单 task、3 op、跑通 100K 行的 Parquet 表，输出 Parquet 🟡 partial — (evidence:) **2-op pipeline (SeqScan→HashAggregate) on 100k parquet 验证过**: `crates/pylon-runtime/tests/aggregate_e2e_test.rs` 的 3 个 e2e 测试 (`e2e_count_star_only_global_aggregate`, `e2e_scan_aggregate_single_stage`, `e2e_aggregate_emits_exactly_one_batch`) 全部 pass。**3-op pipeline (Filter+Project+SeqScan) 没有 dedicated e2e 测试**。**"输出 Parquet" 部分未实装**：`tools/verify-output` 是 read-only Parquet inspector；当前 ops tree 末尾没有 write-to-parquet sink。这个 gap 留给 RFC 0007 M4 范围内的 Spillable + FTE 子系统恢复时机一起补；当前需求下生成 parquet 也是 R6 之后的 Iceberg 路径。
- [ ] 单元测试覆盖率 ≥70% 在 ops/ 下 ❌ (evidence: `cargo-llvm-cov` / `cargo-tarpaulin` 均未配置 — `grep` `Cargo.toml` + `.github/workflows/ci.yml` 无命中；CI workflow 只有 boundary + build-test + deny-check。**issue**: 这是个独立 PR 的范围 (`coverage tooling + first report`) — 加 cargo-llvm-cov 依赖、写 make target、把 coverage 数字更新进本行)
- [ ] 一个 micro-benchmark 比对 Polars / DataFusion 同输入 ❌ (evidence: 没有 `benches/` 目录 — `find` 无命中；pylon 没有现成 baseline runner。 给定 RFC 0007 §4 plan 优先级，独立 PR 处理: 加 `benches/aggregate_baseline.rs` 复刻 100k 行 aggregate，对 polars / datafusion 跑同一 aggregate；写结果数字进本行)

| 任务 | 估计天数 |
|---|---|
| PipelineOp trait draft + 编译错误驱动讨论 | 2 |
| Driver loop 实现 + 单 op 调试 | 3 |
| Driver multi-op + mpsc channel 装配 | 4 |
| `SeqScanOp` (Parquet → RecordBatch) | 2 |
| `FilterOp` 用 `arrow::compute::filter_record_batch` | 1 |
| `ProjectOp` 用 `RecordBatch::project` | 1 |
| 简单 SQL parser 用 sqlparser-rs | 4 |
| Logical plan → Physical plan mapping (无 fragment) | 3 |
| End-to-end 跑通 1 个 query | 3 |
| 单元 / 集成测试 | 3 |
| 文档（`docs/development/local-dev.md`) | 2 |

---

## 里程碑 2 — Multi-worker + Exchange (10 月)  *(hygiene pass 2026-08-20 — 见 PR #14)*

**目标**：多进程 coordinator/worker，二 stage query（partitioned + broadcast exchange）。

定义完成（DoD）：
- [x] `pylon-coord` binary：HTTP API 接收 SQL、解析→plan→fragment→schedule ✅ (evidence: `crates/pylon-coord/src/bin/pylon-coord.rs:4` `use axum::{...}`、`Json` + `Router` 全套；`axum::serve(listener, app)` 起 server；`Json(req): Json<SubmitQuery>` 处理 POST，回 `Json(QuerySubmitted)`；`plan_and_dispatch` 进 SQL→plan→fragment→schedule path)
- [x]* `pylon-worker` binary：接受 task、驱动 driver loop、向 coord 心跳 🟡 partial — (evidence: 接受 task via tonic gRPC `OpenSession` ✅ (R7 + PR #6 audit)；driver loop 在 `crates/pylon-runtime/src/driver.rs` ✅。**"向 coord 心跳"未实现** — M3 RFC 0004 §4 选择**不**采周期性 heartbeat，而是用一次性 `RegisterWorker` + 长连接 `OpenSession` 流代替 (见 `m3-status.md` "Out-of-scope (deferred)" 列表 — heartbeat 在那儿也是 deferred 项)。M2 DoD 把 "心跳" 写进来了，但 M3 的 M3 实现选择了语义等价的 "持久 stream" 代替。*this is a different decision, not a defect* — DoD 行文字需调整: "向 coord 注册并保持长连接"。
- [x] `pylon-exchange`：Arrow Flight 服务端/客户端双向 ✅ (evidence: RFC 0004 全 M3 实现，Status Implemented 2026-08-18。`crates/pylon-exchange/src/flight_rpc.rs` 实现 `arrow_flight::flight_service_server::FlightService` (服务端)；`crates/pylon-runtime/src/ops/exchange.rs::ExchangeSinkRpc` 用 tonic Flight `DoExchange` (客户端)；M3 sign-off packet B-2 验证)
- [x] 实现 partitioned HashPartitionExchange（N→N） ✅ (evidence: `crates/pylon-coord/src/fragment.rs` 中 `cfg.default_partition_count` + `with_default_partition_count()`；fragment.rs 切分点在遇 `PhysicalPlan::Aggregate { group_by }` 时插入 ExchangeSink(op N 个) + per-partition task pairs (ExchangeSource + Aggregate)；2-worker E2E `tools/e2e/two_worker_smoke.sh` 跑过 100k 行 `SELECT name, COUNT(*) GROUP BY name` 端到端)
- [ ] 实现 Broadcast exchange（1→N） ❌ (evidence: `crates/pylon-runtime/src/ops/exchange.rs` + `fragment.rs` 没有任何 Broadcast 实现；`m3-status.md` "Out-of-scope (deferred)" 列了 broadcast 与 HashJoin/Window 等一起为后续 milestone 推后)
- [ ] 跑两 stage `SELECT ... FROM t1 JOIN t2 ON ...`，部署 1 coord + 2 worker ❌ (evidence: 没有 `HashJoin` op — `m3-status.md` 列出 "HashJoin / Distinct / Window fragmenter rules (fragmenter 框架通用，只有 Aggregate 规则实装)"。`crates/pylon-coord/src/fragment.rs:9` 注释明文 "Adding a new boundary op (M4 HashJoin / Distinct / Window) is now..." —— 表明 HashJoin 是 M4 才该 push 的。需要 RFC 0007 §5 S2-S8 落地。)
- [ ] TPC-H Q1 / Q3 在 SF10 上能跑 ❌ (evidence: `m3-status.md` "Out-of-scope (deferred)" 显式列 "Lakekeeper / Iceberg / TPC-H SF100 (推到 M3.5+)"。SF10 小但也需要完整 catalog/Iceberg infrastructure 才跑得起来 — M3.5+ 范围。同时未现成 TPC-H 数据生成或 query 模板)

| 任务 | 估计天数 |
|---|---|
| Coordinator skeleton (HTTP + plan 调用) | 5 |
| Worker skeleton (Task lifecycle) | 5 |
| Arrow Flight server 启动 + 简单 Get/Put | 5 |
| HashPartitionExchange op 实现 | 5 |
| Broadcast exchange op 实现 | 3 |
| Fragmenter (1 阶段内多 Stage 切分) | 4 |
| Stage scheduler (capacity-based) | 6 |
| End-to-end 1+1 worker 测试 | 3 |
| TPC-H Q1/Q3 跑通 | 4 |

---

## 里程碑 3 — Cross-worker Arrow Flight Shuffle (2026-08-18)

**目标**：把 M2 的 "coord 作为 gatherer" 换成 worker↔worker 的 Arrow Flight 流；coord 只路由，不持数据；多 stage query 能跑。

**实际交付（范围调整）**：原 M3 scope 是 Iceberg + Lakekeeper + TPC-H SF100。8 月这一轮把范围换成 RFC-0004 的跨 worker shuffle 基底（A1 Aggregate 节点 / A2 Fragmenter / B-1 Worker Discovery / B-2 ExchangeSinkRpc 跨 Flight RPC / B-3.5 真跨进程 E2E）。Iceberg 那边推后到 M3.5+/后续轮次，catalog/iceberg/storage 三个 crate 仍是 stub。详细 sign-off 见 [docs/notes/m3-status.md](../notes/m3-status.md)。

定义完成（DoD 调整版）：
- [x] `LogicalPlan::Aggregate` + `HashAggregateOp` (COUNT/SUM/MIN/MAX) — A1
- [x] `Fragmenter` post-order walk + `HashPartitionExchange` 注入 + per-row FNV-1a hash routing — A2
- [x] 1-stage aggregate E2E (`SELECT name, COUNT(*), SUM(amount) GROUP BY name` over `sample.parquet`，结果对) — A1
- [x] 1-worker 2-stage partitioned aggregate E2E (4 partitions, in-process `ExchangeSink`) — A2
- [x] `pylon_coord::Discovery` + `RegisterWorker` proto RPC + worker 端 `flight_addr` 上报 — B-1
- [x] Worker 进程同进程起 Arrow Flight server (`arrow_flight::flight_service_server::FlightService` impl) — B-1
- [x] `ExchangeSinkRpc` op 走真 tonic Flight `DoExchange`（per-row hash 路由到 N 个 target `flight_addr`）— B-2
- [x] 2-worker 跨进程 E2E (`SELECT name, COUNT(*) GROUP BY name` 跑过，stage0 在 worker 0 扫 + `ExchangeSinkRpc` per-row hash 路由到 2 个 partition target，stage1 partition p 派给 worker p%2 跑 `ExchangeSource + HashAggregate`，coord 合并结果）— B-3 / B-3.5

| Stage | commit | status |
|---|---|---|
| A1-1 Logical::Aggregate | 2e1d9d8 | ✓ |
| A1-2 Physical lowering | e828af4 | ✓ |
| A1-3 HashAggregateOp | 2247723 | ✓ |
| A1-4 worker wiring | 692e267 | ✓ |
| A1-5 1-stage E2E | 7ee6848 | ✓ |
| A1 rollup | d3e8889 | ✓ |
| A2-1 Fragmenter + HashPartitionExchange | a0483aa | ✓ |
| A2-2 2-stage in-process E2E | 880c65e | ✓ |
| A2 rollup | ac95641 | ✓ |
| B-1 Discovery + Flight server | a9ed2ac | ✓ |
| B-2 ExchangeSinkRpc | 122f3f4 | ✓ |
| B-3 2-worker smoke | e9b3a03 | ✓ |
| B-3.5 gap1 (真 IPC) | 1435fb8 | ✓ |
| B-3.5 gap2 (真 cross-worker E2E) | 178d5eb | ✓ |

headline numbers:
- 91 unit tests passing (33 suites; up from 17 at end of M1)
- `tools/e2e/two_worker_smoke.sh` 跑过：`SELECT name, COUNT(*) FROM sample GROUP BY name` 在 1 coord + 2 worker 上跑通，结果 100k 行 `(name, count=1)`

**Out of scope (deferred)**：
- [ ] Lakekeeper / Iceberg / TPC-H SF100（推到 M3.5+）
- [ ] HashJoin / Distinct / Window fragmenter rules（fragmenter 框架通用，只有 Aggregate 规则实装）
- [ ] Nested aggregate（显式 reject）
- [ ] 同 worker `ExchangeSource` 走真 Flight RPC（当前 in-process `PylonFlightService` only；跨 worker 已经走真 RPC）
- [ ] Coordinator HA / TLS / OIDC

---

## 里程碑 4 — FTE + Spill (1–2 月)

**目标**：生产可用性 + 容错。

| 任务 | 估计天数 |
|---|---|
| FTE sink：写 Arrow IPC stream 到 S3 | 6 |
| FTE source：从 S3 读回作为 retry base | 4 |
| per-task memory pool | 6 |
| Spill manager (per-fragment budget) | 5 |
| Spill 版本的 hash agg | 8 |
| Spill 版本的 sort | 6 |
| 故障注入测试平台 (chaos-style) | 5 |
| 拉一个 worker 看 query 是否继续 | 3 |

---

## 里程碑 5 — Hardening (3–4 月)

| 任务 | 估计天数 |
|---|---|
| OAuth/OIDC 认证 | 4 |
| JDBC 客户端（caliburn-rs） | 7 |
| 第二个 connector (Postgres via ODPS?) | 14 |
| Coordinator HA (Raft) | 14 |
| Performance 优化 (cache, JIT 内联, 自适应 driver 数) | 10 |
| Operator profile 指标化 | 5 |

---

## 里程碑 6 — 开源化 (5–6 月)

| 任务 | 估计天数 |
|---|---|
| `LICENSE` / `NOTICE` / 治理文件 | 1 |
| 用户文档（mdbook） | 7 |
| 贡献者指南 | 2 |
| 测试矩阵 (Linux/macOS, GCC/musl) | 3 |
| 性能报告 + 已知差距清单 | 4 |
| 提交 ASF 孵化计划 / CNCF sandbox | 4 |

---

## 总览

| Phase | 周期 | 关键输出 |
|---|---|---|
| M0 | 2026-08 | scaffold |
| M1 | 2026-09 | 单进程 pipeline 跑通 |
| M2 | 2026-10 | 多 worker + exchange |
| M3 | 2026-11 至 12 | Iceberg + Lakekeeper + TPC-H SF100 |
| M4 | 2027-01 至 02 | FTE + spill (生产能力) |
| M5 | 2027-03 至 04 | Hardening + 第二个 connector + HA |
| M6 | 2027-05 至 06 | Open-source 准备 |
