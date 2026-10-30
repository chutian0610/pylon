# Pylon

Pylon 是一个 Rust 编写、Arrow-native 的分布式 SQL 查询引擎：读 Iceberg 湖仓表、
面向交互式分析负载（TPC-H 族），执行模型采用 morsel-driven push pipeline，
架构目标是存算分离与无状态计算节点。对标 Trino 的 workload 家族，但更云原生、
scale 更强。

## 当前状态

**M1 实现中。** 首条端到端弹道（M1/T1，#2）已落地，SQL 文本进、Arrow 结果出的
规格级接缝自第一天贯通：

```
pylon "SELECT a, b FROM t LIMIT 10"   # 表 t = <data-dir>/t/*.parquet
```

链路 = 自研 parser（手写 lexer + nom 语法层）→ planner（列绑定 + 投影裁剪下推）
→ morsel push 执行器（tokio IO 预取 + 有界队列 + 专用计算线程，算子内无 await）
→ stdout 表格。TableFormat 接缝已定形（schema / scan 契约，产出可并行打开的
Morsel 流），首个实现为本地 Parquet 目录 fixture，T5 由 Iceberg adapter 取代。

Parser 已覆盖 M1 全部语法面（#3）：算术/比较/逻辑运算、IS NULL、BETWEEN、
date 与 interval 字面量及运算、WHERE、GROUP BY、聚合（参数含表达式、COUNT(*)）、
SELECT DISTINCT、ORDER BY、LIMIT、SHOW TABLES、DESCRIBE；每个构造有 AST golden
快照（`crates/pylon-sql/tests/golden/`），非法输入返回带行列与脱字符的错误。
执行面暂仍为投影 + LIMIT（WHERE / 聚合 / 排序的规划与执行随后续 ticket 展开，
试跑会得到带位置的 "not supported yet"）；`pylon --parse "<sql>"` 可打印完整 AST：

```bash
cargo build --release
cargo test --workspace          # 集成测试含 DuckDB 对拍（PATH 上有 duckdb 即跑；
                                # 设 PYLON_REQUIRE_DUCKDB=1 可强制要求）
./target/release/pylon --data-dir <目录> "SELECT a, b FROM t LIMIT 10"
./target/release/pylon --parse "SELECT l_returnflag, SUM(l_quantity) FROM lineitem \
  WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY GROUP BY l_returnflag"
```

术语表见 [CONTEXT.md](CONTEXT.md)，决策记录见 [docs/adr/](docs/adr/)。

## 已定架构决策

| 决策 | 出处 |
|---|---|
| 全自研查询栈（含自研 SQL parser：手写 lexer + nom，Trino 语法族），不使用 DataFusion 运行时 | [ADR-0001](docs/adr/0001-self-built-query-stack.md) |
| 执行模型：morsel-driven push pipeline，聚合是 pipeline 的墙 | [ADR-0002](docs/adr/0002-morsel-driven-push-pipelines.md) |
| Worker 无状态原则；明确不做 serverless | [ADR-0003](docs/adr/0003-stateless-workers.md) |
| 能力面仅 SELECT；写路径整体后置，解禁条件 = iceberg-rust DML 原语成熟 | [ADR-0004](docs/adr/0004-write-scope-select-and-append-insert.md)、[ADR-0009](docs/adr/0009-iceberg-only-select-only.md) |
| 类型系统：Arrow 即逻辑即物理类型 | [ADR-0005](docs/adr/0005-arrow-unified-type-system.md) |
| 线程模型：专用计算线程（无 await）+ tokio 异步 IO 供给 | [ADR-0006](docs/adr/0006-dedicated-compute-threads-async-io.md) |
| 数据源仅 Iceberg，经自有 TableFormat 接缝；直接依赖 iceberg-rust 读路径，全仓 arrow/parquet 锁 ^58 | [ADR-0007](docs/adr/0007-tableformat-seam-hive-iceberg.md)、[ADR-0009](docs/adr/0009-iceberg-only-select-only.md) |
| Shuffle：Arrow Flight DoExchange P2P 直连为默认，对象存储为演进模式，Redis 只做协调元数据 | [ADR-0008](docs/adr/0008-shuffle-p2p-by-default.md) |

## 路线图

- **M1 单机 Iceberg SELECT**：自研 parser → 逻辑/物理计划 → morsel push 执行器；
  算子面 = Scan（谓词/列裁剪下推）/ Filter / Project / HashAggregate / Sort（聚合后
  小结果）/ TopN。验收：TPC-H SF1（PyIceberg 造数）中无 JOIN 且无子查询的 Q1、Q6
  对拍 DuckDB，加自建套件覆盖其余 SQL 面。
- **M2 分布式骨架**：coordinator + worker，hash 分区聚合经 Arrow Flight P2P
  shuffle。
- **M3 JOIN**：hash join build/probe 两段式 pipeline。验收：TPC-H 22 条全通。
- **M4 容错与弹性**：查询级任务重派、扩缩容。
- 条件触发：写路径（append INSERT 及后续 DML），随 iceberg-rust 上游成熟度解禁。

## 明确不做 / 已知边界

- 不自建存储；不做 serverless（Worker 常驻、可弹性伸缩，不随查询起灭）。
- 数据文件只读 Parquet（无 ORC）；数据源仅 Iceberg（Hive 已评估并排除，
  见 ADR-0009）。
- 不使用 DataFusion 运行时；UPDATE / DELETE / MERGE INTO / CTAS 不在当前范围。

## 开发环境（规划）

MinIO（S3 兼容）+ Lakekeeper（Iceberg REST Catalog）承担开发与集成测试；
单元测试走本地文件系统与轻量 catalog。
