# Pylon

Pylon 是一个 Rust 编写的 Arrow-native 分布式 SQL 查询引擎，读写 Iceberg 湖仓表、
面向交互式分析负载，架构目标是存算分离与无状态计算节点。不自建存储。

## Language

### 执行模型

**Morsel**:
Pipeline 中数据流动的最小单位：一批固定行数的列式数据切片。
_Avoid_: chunk、vector、裸用的 batch

**Push Pipeline**:
由驱动线程主动把 Morsel 沿算子序列推送到 Pipeline 终点（聚合或输出）的执行模型。
_Avoid_: pull 执行、Volcano、iterator 式执行

**Worker**:
执行被调度任务的计算进程，按无状态原则设计：可丢失状态一律外置到外部存储，
进程随时可重建。常驻并可弹性伸缩，不与单个查询的生命周期绑定。
_Avoid_: executor、node

### 存储接入

**TableFormat**:
Pylon 接入外部表格式的自有接缝抽象，承载 schema 解析、文件规划、scan 能力
（写能力预留）；当前唯一实现是 Iceberg。
_Avoid_: connector、provider
