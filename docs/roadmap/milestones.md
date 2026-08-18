# Pylon Implementation Roadmap

时间单位：1 个月 = 4 周

## 里程碑 0 — Scaffold (2026-08-18 → 2026-08-31)

**目标**：建立 Rust workspace，所有 ADR 落定。

| 任务 | 状态 |
|---|---|
| 创建 workspace `Cargo.toml` | ⏳ |
| 添加 `pre-commit` (rustfmt, clippy, cargo-deny) | ⏳ |
| GitHub Actions CI 模版 | ⏳ |
| 8 个 crate 全部创建空 stub | ⏳ |
| 调研笔记 `docs/research/findings.md` | ✅ 完成 |
| RFC 0001 Architecture | ✅ 完成 |
| RFC 0002 Crate 结构 | ⏳ |
| RFC 0003 Driver 循环 spec | ⏳ |
| RFC 0004 PipelineOp trait spec | ⏳ |
| RFC 0005 Scheduler spec | ⏳ |
| RFC 0006 Exchange 协议 spec | ⏳ |

---

## 里程碑 1 — Single-worker Pipeline (9 月)

**目标**：单进程内跑通 pipeline runtime + 3 个算子 + 简单 SQL。

定义完成（DoD）：
- [ ] `pylon-plan` 能 parse: `SELECT col_a FROM t WHERE col_b > 0`
- [ ] `pylon-runtime` 实现 PipelineOp trait + Driver loop + Task lifecycle
- [ ] 实现 `SeqScanOp(Parquet)` + `FilterOp` + `ProjectOp`
- [ ] 单 task、3 op、跑通 100K 行的 Parquet 表，输出 Parquet
- [ ] 单元测试覆盖率 ≥70% 在 ops/ 下
- [ ] 一个 micro-benchmark 比对 Polars / DataFusion 同输入

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

## 里程碑 2 — Multi-worker + Exchange (10 月)

**目标**：多进程 coordinator/worker，二 stage query（partitioned + broadcast exchange）。

定义完成（DoD）：
- [ ] `pylon-coord` binary：HTTP API 接收 SQL、解析→plan→fragment→schedule
- [ ] `pylon-worker` binary：接受 task、驱动 driver loop、向 coord 心跳
- [ ] `pylon-exchange`：Arrow Flight 服务端/客户端双向
- [ ] 实现 partitioned HashPartitionExchange（N→N）
- [ ] 实现 Broadcast exchange（1→N）
- [ ] 跑两 stage `SELECT ... FROM t1 JOIN t2 ON ...`，部署 1 coord + 2 worker
- [ ] TPC-H Q1 / Q3 在 SF10 上能跑

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

## 里程碑 3 — Iceberg + Lakekeeper (11–12 月)

**目标**：真实数据源接入，与 Iceberg 生态打通。

定义完成（DoD）：
- [ ] Lakekeeper 实例部署，跑元数据
- [ ] `pylon-catalog::LakekeeperClient`：list/load/commit
- [ ] `pylon-iceberg::reader`：snapshot → manifest → Parquet → RecordBatch
- [ ] SQL `SELECT * FROM iceberg-table` 端到端
- [ ] TPC-H SF100 全套跑过（除 TPC-H Q15 包含 view creation）
- [ ] benchmark 与 Trino 457 release 对比：

| Metric | Pylon | Trino | Delta |
|---|---|---|---|
| TPC-H total runtime | ? | ? | ? |
| Peak memory | ? | ? | ? |
| Cold start time | ? | ? | ? |

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
