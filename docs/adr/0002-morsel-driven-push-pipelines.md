# 执行模型：morsel-driven push pipeline

单机执行采用 morsel-driven push pipeline（Velox / DuckDB / Polars 流式引擎同一谱系）：
计算线程从 source 认领一个 Morsel，沿 pipeline 主动推到终点（聚合算子或输出），
聚合是 pipeline 的"墙"。并行度来自 Morsel 级认领与 work stealing，而非算子级调度。
跨节点 shuffle 采用异步流，其协议在分布式阶段另行设计。

## Considered Options

- Pull / Volcano 迭代器（DataFusion 系）—— 被否：逐批 next() 的调用开销与流水线
  阻断对交互式延迟不利，且与市场上已有引擎同质化。
- Actor / channel 模型（RisingWave 流式风格）—— 被否：更贴流处理，批式分析下
  调度开销大，延迟聚合的表达不自然。

## Consequences

- 聚合、join 等有状态算子需要 build/probe 两段式设计与 pipeline 切分。
- 参考实现：Polars 流式引擎（push + morsel 最贴近）、Arroyo（push DAG）。
