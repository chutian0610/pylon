# 类型系统：Arrow 即逻辑类型即物理类型

Pylon 的逻辑类型与物理类型统一为 Arrow DataType，不设独立的 LogicalType 层；SQL
隐式转换、timestamp 时区语义、decimal 精度提升等规则作为 planner 中的显式规则表
实现。

## Considered Options

- Trino 式 LogicalType / PhysicalType 双层 —— 被否：Arrow-native 定位下双层映射
  是持续的维护成本；Iceberg 与 Hive 表的 schema 到 Arrow 均有自然映射。

## Consequences

- SQL 类型语义（隐式转换、比较规则）需自行定义并用对拍测试钉死。
- 外部表格式的 schema 差异在 TableFormat 接缝层归一为 Arrow schema。
