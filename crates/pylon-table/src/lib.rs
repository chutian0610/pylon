//! TableFormat 接缝：Pylon 接入外部表格式的自有抽象（ADR-0007 / ADR-0009）。
//!
//! 职责：表 schema 解析（归一为 Arrow）与扫描规划（产出可并行打开为 Morsel 流的
//! [`ScanSplit`]，承接列裁剪与谓词下推）。数据格式的读取细节（Parquet 解码、对象
//! 存储 IO、delete 文件处理）全部封在 TableFormat 实现内，执行器只消费 Morsel 流
//! ——这是该接缝"定形"的关键：T5 换 Iceberg adapter 时执行器零改动。
//!
//! 下推契约（M1/T3 定形）：
//! - 列裁剪（`projection`）是**精确**的：返回的 [`ScanPlan::projection`] 之外的列
//!   不会出现在 Morsel 中；
//! - 谓词（`filters`）是**非精确提示**：格式实现可用它跳过文件/行组以少读数据，
//!   但不承诺行级过滤——正确性由执行器在 scan 之上的残差 Filter 保证。
//!
//! 当前唯一实现：[`ParquetDirFormat`]，读取本地 Parquet 目录（M1/T1 临时 fixture，
//! T5 由 Iceberg adapter 取代）。

pub mod parquet_dir;
#[doc(hidden)]
pub mod testing;

use std::fmt;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use pylon_expr::Scalar;

pub use parquet_dir::ParquetDirFormat;

/// 可下推的简单比较谓词（`列 op 标量`）。复合谓词由 planner 拆为多个的合取。
#[derive(Debug, Clone, PartialEq)]
pub struct ScanPredicate {
    /// 表 schema 列索引
    pub column: usize,
    pub op: CmpOp,
    pub value: Scalar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl fmt::Display for CmpOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CmpOp::Eq => "=",
            CmpOp::NotEq => "<>",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
        };
        f.write_str(s)
    }
}

/// Morsel 流：产出 ≤ 目标行数的 RecordBatch，出错带 [`TableError`]。
pub type MorselStream = BoxStream<'static, Result<RecordBatch, TableError>>;

/// 一个可独立打开的扫描单元（通常是一个数据文件）。
///
/// `open` 返回的 future 与流都是 `'static`（自带全部所需状态），执行器可把它们搬进
/// 任意任务；调用发生在 tokio 运行时内。
pub trait ScanSplit: Send + Sync + fmt::Debug {
    /// 诊断标识（如文件路径），用于错误信息。
    fn describe(&self) -> String;

    /// 打开为 Morsel 流，每个 Morsel 至多 `morsel_rows` 行。
    fn open(&self, morsel_rows: usize) -> BoxFuture<'static, Result<MorselStream, TableError>>;
}

pub trait TableFormat: Send + Sync {
    /// 表格式名称，仅用于诊断输出。
    fn name(&self) -> &'static str;

    /// 解析表 schema，归一为 Arrow schema。表不存在时返回 [`TableError::TableNotFound`]。
    fn table_schema(&self, table: &str) -> Result<SchemaRef, TableError>;

    /// 规划一次扫描。
    ///
    /// `projection` 为表 schema 的列索引，可任意顺序、可重复；`None` 表示全列。返回的
    /// [`ScanPlan::projection`] 已去重并按表 schema 顺序排列；各 split 解码出的 Morsel
    /// schema 恰为表 schema 在该投影上的子集（表序）。
    ///
    /// `filters` 为非精确谓词提示（见模块文档）：实现可以忽略，也可以用它裁掉
    /// 文件/行组；绝不允许因此漏掉满足谓词的行。
    fn scan(
        &self,
        table: &str,
        projection: Option<&[usize]>,
        filters: &[ScanPredicate],
    ) -> Result<ScanPlan, TableError>;
}

#[derive(Debug)]
pub struct ScanPlan {
    /// 表 schema 列索引，升序去重。
    pub projection: Vec<usize>,
    /// 可并行的扫描单元。
    pub splits: Vec<Box<dyn ScanSplit>>,
}

#[derive(Debug, thiserror::Error)]
pub enum TableError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table '{0}' has no Parquet files")]
    EmptyTable(String),
    #[error("column index {index} out of range for table '{table}' with {width} columns")]
    ColumnOutOfRange {
        table: String,
        index: usize,
        width: usize,
    },
    #[error("io error at {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parquet error at {path}: {source}")]
    Parquet {
        path: std::path::PathBuf,
        #[source]
        source: parquet::errors::ParquetError,
    },
    #[error("{path}: {message}")]
    Scan {
        path: std::path::PathBuf,
        message: String,
    },
}

/// 把调用方给出的投影归一为升序去重的列索引；`None` 展开为全列。越界报错。
pub fn normalize_projection(
    table: &str,
    projection: Option<&[usize]>,
    width: usize,
) -> Result<Vec<usize>, TableError> {
    let Some(projection) = projection else {
        return Ok((0..width).collect());
    };
    if let Some(&index) = projection.iter().find(|&&i| i >= width) {
        return Err(TableError::ColumnOutOfRange {
            table: table.to_string(),
            index,
            width,
        });
    }
    let mut columns = projection.to_vec();
    columns.sort_unstable();
    columns.dedup();
    Ok(columns)
}
