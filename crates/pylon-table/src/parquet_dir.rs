//! 本地 Parquet 目录 fixture：`<root>/<table>/*.parquet` 即一张表。
//!
//! M1/T1 的临时 TableFormat 实现，用于在 Iceberg adapter（T5）就位前让全链路跑通。
//! 约定：目录内所有 Parquet 文件 schema 一致，以字典序首个文件的 schema 为表 schema。

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::{MorselStream, ScanPlan, ScanSplit, TableError, TableFormat, normalize_projection};

#[derive(Debug, Clone)]
pub struct ParquetDirFormat {
    root: PathBuf,
}

impl ParquetDirFormat {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// 表名只能是单层目录名：含路径分隔符或 `..` 的名字一律视为不存在，避免越出 root。
    fn table_dir(&self, table: &str) -> Result<PathBuf, TableError> {
        let is_plain =
            !table.is_empty() && table != "." && table != ".." && !table.contains(['/', '\\']);
        if !is_plain {
            return Err(TableError::TableNotFound(table.to_string()));
        }
        Ok(self.root.join(table))
    }

    /// 字典序列出表目录下的 Parquet 文件；目录不存在报 TableNotFound，无文件报 EmptyTable。
    fn list_files(&self, table: &str) -> Result<Vec<PathBuf>, TableError> {
        let dir = self.table_dir(table)?;
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(TableError::TableNotFound(table.to_string()));
            }
            Err(source) => return Err(TableError::Io { path: dir, source }),
        };

        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| TableError::Io {
                path: dir.clone(),
                source,
            })?;
            let path = entry.path();
            let is_parquet = path
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"));
            if is_parquet && path.is_file() {
                files.push(path);
            }
        }
        if files.is_empty() {
            return Err(TableError::EmptyTable(table.to_string()));
        }
        files.sort();
        Ok(files)
    }
}

fn read_schema(path: &Path) -> Result<SchemaRef, TableError> {
    let file = File::open(path).map_err(|source| TableError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new()).map_err(|source| {
            TableError::Parquet {
                path: path.to_path_buf(),
                source,
            }
        })?;
    Ok(metadata.schema().clone())
}

/// 单个 Parquet 文件的扫描单元：按根列投影解码，Morsel 大小由执行器给定。
#[derive(Debug, Clone)]
pub struct ParquetSplit {
    path: PathBuf,
    /// 文件 schema 内要读取的根列索引，升序。
    columns: Vec<usize>,
}

impl ScanSplit for ParquetSplit {
    fn describe(&self) -> String {
        self.path.display().to_string()
    }

    fn open(&self, morsel_rows: usize) -> BoxFuture<'static, Result<MorselStream, TableError>> {
        let path = self.path.clone();
        let columns = self.columns.clone();
        Box::pin(async move {
            let open_err = |message: String| TableError::Scan {
                path: path.clone(),
                message,
            };
            let handle = tokio::fs::File::open(&path)
                .await
                .map_err(|e| open_err(e.to_string()))?;
            let builder = ParquetRecordBatchStreamBuilder::new(handle)
                .await
                .map_err(|e| open_err(e.to_string()))?;

            let width = builder.schema().fields().len();
            if let Some(&bad) = columns.iter().find(|&&c| c >= width) {
                return Err(open_err(format!(
                    "column index {bad} requested but file has {width} columns"
                )));
            }
            let mask = ProjectionMask::roots(builder.parquet_schema(), columns.iter().copied());
            let stream = builder
                .with_projection(mask)
                .with_batch_size(morsel_rows.max(1))
                .build()
                .map_err(|e| open_err(e.to_string()))?;

            let stream = stream.map_err(move |e| TableError::Scan {
                path: path.clone(),
                message: e.to_string(),
            });
            Ok(Box::pin(stream) as MorselStream)
        })
    }
}

impl TableFormat for ParquetDirFormat {
    fn name(&self) -> &'static str {
        "parquet-dir"
    }

    fn table_schema(&self, table: &str) -> Result<SchemaRef, TableError> {
        let files = self.list_files(table)?;
        read_schema(&files[0])
    }

    fn scan(&self, table: &str, projection: Option<&[usize]>) -> Result<ScanPlan, TableError> {
        let files = self.list_files(table)?;
        let schema = read_schema(&files[0])?;
        let columns = normalize_projection(table, projection, schema.fields().len())?;
        Ok(ScanPlan {
            projection: columns.clone(),
            splits: files
                .into_iter()
                .map(|path| {
                    Box::new(ParquetSplit {
                        path,
                        columns: columns.clone(),
                    }) as Box<dyn ScanSplit>
                })
                .collect(),
        })
    }
}
