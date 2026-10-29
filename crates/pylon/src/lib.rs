//! Pylon CLI 前门：SQL 文本进，Arrow 结果（或带位置的用户可见错误）出。
//!
//! 这是规格级接缝——M1 全部 e2e 验收都钉在这里的外部行为上（结果数据、错误消息与
//! 位置），不断言内部计划或结构（见 M1 spec「Testing Decisions」）。

pub mod output;

use std::path::Path;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use pylon_exec::ExecError;
use pylon_exec::PipelineDriver;
use pylon_plan::PlanError;
use pylon_table::ParquetDirFormat;

pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl QueryResult {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PylonError {
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Exec(#[from] ExecError),
}

impl PylonError {
    /// 渲染为用户可见错误文本；带位置的错误附源码行与脱字符。
    pub fn render(&self, sql: &str) -> String {
        match self {
            PylonError::Plan(e) => e.render(sql),
            PylonError::Exec(e) => format!("error: {e}"),
        }
    }
}

/// 执行一条查询。`data_dir` 下的每个子目录是一张 Parquet 目录表（M1/T1 fixture 形态）。
pub fn run_query(sql: &str, data_dir: &Path) -> Result<QueryResult, PylonError> {
    run_query_with(sql, data_dir, &PipelineDriver::default())
}

pub fn run_query_with(
    sql: &str,
    data_dir: &Path,
    driver: &PipelineDriver,
) -> Result<QueryResult, PylonError> {
    let format = ParquetDirFormat::new(data_dir);
    let pipeline = pylon_plan::plan(sql, &format)?;
    let batches = driver.run(&pipeline)?;
    Ok(QueryResult {
        schema: pipeline.output_schema,
        batches,
    })
}
