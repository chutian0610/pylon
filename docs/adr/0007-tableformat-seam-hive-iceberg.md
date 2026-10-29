# 存储格式策略：TableFormat 接缝，Hive 与 Iceberg 双一等公民

> 已部分取代：数据源收缩为仅 Iceberg，见 ADR-0009。本 ADR 中存活的决策是
> TableFormat 接缝原则与"否决完整自研 Iceberg SDK"；Hive 双一等公民、目录表
> 先行、HMS 路线图均已作废。

Pylon 不自建存储，经自有 TableFormat trait 接缝接入外部表格式（schema 解析、
文件规划、scan、append 写四项能力）。Hive 与 Iceberg 都是长期一等支持方向：
Hive 代表当前最大的存量使用场景，Iceberg 代表湖仓方向，不因后者是热点而放弃前者。
实现顺序：Hive 目录表（无 Metastore，Parquet + hive_partitioning 目录约定）→
Hive HMS → Iceberg。数据文件只支持 Parquet，不支持 ORC。

早期里程碑不依赖 iceberg-rust：全仓 arrow/parquet 用最新版，arrow 版本自由由此
获得。到 Iceberg 里程碑时再决定用 iceberg-rust 还是自研薄读层，届时按
"API 趋稳 / DML 原语就绪 / arrow 版本不冲突"评估解禁条件。完整自研 Iceberg SDK
（manifest / snapshot / deletes 全部自担）被否决——工程量与 spec 细节风险远超收益。

## Consequences

- 本 ADR 修订最初的"官方 iceberg crate + REST Catalog 直接依赖"决策（Q5 原案）。
- 依赖面：SQL 解析用 sqlparser-rs，对象存储用 object_store 系；到 Iceberg 里程碑
  前不引入 iceberg-rust。
- HMS 层的已知约束（2026-10 调研）：Rust 侧唯一可用客户端是 `hive_metastore`
  crate（个人维护、IDL 生成自 Hive 2.3、无 Kerberos/SASL 路径；iceberg-catalog-hms
  内部也是依赖它）。因此 HMS 里程碑按"独立评估"对待，不进 MVP 关键路径；读写都以
  目录表形态先行，HMS 只替换元数据来源，文件扫描层完全复用。
