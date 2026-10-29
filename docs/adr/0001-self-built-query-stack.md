# 自研查询栈，不使用 DataFusion 运行时

> 修订（2026-10）：SQL 解析由 sqlparser-rs 改为**自研**（lexer 手写 + 语法层 nom
> 组合子，Trino 语法族子集）。理由：sqlparser-rs 为多方言兼容背负大量泛化 AST 与
> 特例处理，全新引擎只需 Trino 族；自有 AST 与自研 planner 对接更干净，这也是
> DuckDB / CockroachDB / Trino 等有生产野心的引擎的常态。parser 自此从"周边
> 复用"划入"核心自研"。护栏：parser 范围严格跟随里程碑 SQL 面，不预建通用 parser。

Pylon 以生产级 push 执行引擎为目标，执行路径（逻辑计划、优化器、物理计划、执行器）
全部自研；SQL 解析用 sqlparser-rs，周边能力（Iceberg catalog、文件 IO 等）复用生态，
但一律经过自有 trait 接缝隔离、可替换。不使用 DataFusion 的任何运行时组件：其物理
计划 API 是为 pull/Volcano 迭代器执行器设计的，复用前端会把这个抽象拖进 push 架构的
核心。

## Considered Options

- 复用 DataFusion 前端（parser + logical plan + optimizer），只自研执行器 —— 被否：
  物理计划 API 与 pull 执行耦合深，接缝处会持续摩擦。
- 整栈 DataFusion —— 被否：DataFusion 是 pull 系，这样做项目失去存在意义。

## Consequences

- SQL 语义、优化器规则、类型转换语义的工程量全部自担。
- 全仓 arrow/parquet 版本跟随 iceberg crate 的要求（^58），随其升级节奏。
