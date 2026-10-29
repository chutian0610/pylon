# 线程模型：专用计算线程 + 异步 IO 供给

Push 算子运行在固定大小、算子代码内无 await 的专用计算线程池上，以 Morsel 认领制
加 work stealing 取得并行；S3 / REST / Parquet 预取等网络 IO 由 tokio 异步任务
承担，产出经有界队列喂给计算侧，IO 与计算重叠（Velox / DuckDB / Polars 流式引擎
同构）。

## Considered Options

- 全异步算子（async fn 跑在 tokio runtime 上）—— 被否：per-morsel 热路径上的
  async 状态机开销与 runtime 争抢，对交互式延迟不利。

## Consequences

- 计算侧与 IO 侧的队列边界（有界、背压）成为执行器核心契约之一。
