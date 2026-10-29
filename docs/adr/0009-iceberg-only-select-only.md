# 数据源与能力面收缩：仅 Iceberg、仅 SELECT

背景：HMS 调研显示 Rust 侧 HMS 客户端是个人维护单点（无 Kerberos），Hive 集成
风险高于收益；且 Rust 生态的 Iceberg 写路径只有 append（无 delete / merge），
单独的 INSERT 价值有限。

决策：Pylon 数据源只支持 Iceberg，排除 Hive（含 HMS 与 Hive 格式目录表）；
能力面当前只承诺 SELECT，写路径（含 append INSERT）整体后置，解禁条件不变——
上游 OverwriteAction / 冲突验证 / DML 原语成熟。元数据从 Iceberg Catalog
（REST，如 Lakekeeper）获取。TableFormat 接缝原则保留（ADR-0007 的遗产），
当前唯一实现为 Iceberg；未来若重引其他格式，经同一接缝扩展。

本 ADR 取代 ADR-0007 的格式路线图部分，并收紧 ADR-0004 的写边界。

## Consequences

- M1（单机读）从第一天依赖 Iceberg 读取能力，"早期不依赖 iceberg-rust"的前提
  失效，读取实现路线（iceberg-rust vs 自研薄读层）需重新决策。
- 本地开发环境回到 MinIO + Lakekeeper（REST Catalog）；TPC-H 造数回到
  PyIceberg 写表。
