# Shuffle 交换：P2P 直连为默认，外置存储为演进

背景：ADR-0003 确立 Worker 无状态、shuffle 不依赖本地盘持久，其初版把"落外部
介质（S3/Redis 类）"当作 shuffle 默认路径（存储版，RSS 的对象存储变体）。修正：
在已接受查询级重试（容错由重跑承担）与交互式优先的负载定位下，exchange 延迟主导
查询体验，P2P 直连（Trino / Flink batch / Spark 原生模式）才是正确的默认。

决策：shuffle 默认经 **Arrow Flight DoExchange** 在 Worker 间直接流式传输 hash
分区批次，内存有界队列做背压，本地盘仅作慢消费者的可选溢出缓冲（缓冲，非事实
来源）。Worker 丢失的容错在 v1 由查询级重试承担，不要求 shuffle 数据持久化。

外置对象存储 shuffle 保留为后续演进模式，服务于超大 shuffle（消费者跟不上、
生产者需提前退出）与长查询的 stage 级容错（M7 里程碑）。Redis 只用于协调元数据
（心跳 / 发现 / 任务状态），不做 shuffle 数据介质。

## Considered Options

- 对象存储直写直读为默认（本 ADR 修正的对象）——基础设施最简，但写+读两次 IO
  与 S3 延迟对交互式负载不利；降级为演进选项。
- RSS 式专用 shuffle 服务集群——引入常驻专用组件，与组件最少化冲突，否决。

## Consequences

- 修订 ADR-0003 中"shuffle 落外部介质"的默认表述；其无状态原则本身不变。
- M2 分布式骨架的工程重心是 exchange 运行时（连接网格、流控、慢消费者处理）。
