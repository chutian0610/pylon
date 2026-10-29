# 数据能力边界：SELECT + append-only INSERT，DML 后置

> 修订（2026-10，见 ADR-0009）：能力面进一步收紧为**仅 SELECT**——append INSERT
> 亦后置，理由：上游无 delete / merge，单独的 append 写价值有限。解禁条件不变。

Pylon 当前阶段的表操作只覆盖 SELECT（读）与 INSERT INTO（append-only 写，
经 iceberg-rust SDK writer 路径）。UPDATE / DELETE / MERGE INTO / CTAS /
overwrite 明确不在当前范围：截至 2026-10，iceberg-rust 写路径仅 append 成熟
（overwrite / row-delta / delete files 均未就绪），DataFusion 侧 CoW UPDATE 与
MERGE 亦在推进中，自研 DML 意味着自担 Iceberg commit 冲突验证等 spec 级细节。

解禁条件：上游事务原语（OverwriteAction、冲突验证）成熟后重评。流式入湖
（append）是当前写路径的目标场景。
