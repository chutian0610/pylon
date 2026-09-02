//! Arrow IPC streaming codec shared by the exchange transport and
//! the spill / input-log persistence paths (RFC 0007).
//!
//! A *batch stream* is one complete Arrow IPC message sequence:
//! schema + batch + EOS. Concatenating batch streams yields a valid
//! file that [`read_concatenated_ipc`] decodes — this is the
//! multipart sink's on-disk format and is backward compatible with
//! plain single-stream files.

use crate::PylonError;
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::SchemaRef;

/// Encodes one Arrow batch as a complete IPC stream (schema + batch
/// + EOS).
pub fn encode_batch_stream(schema: &SchemaRef, batch: &RecordBatch) -> Result<Vec<u8>, PylonError> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema).map_err(ipc_err)?;
    writer.write(batch).map_err(ipc_err)?;
    writer.finish().map_err(ipc_err)?;
    Ok(buf)
}

/// Decodes bytes holding one Arrow IPC stream — or several complete
/// streams concatenated back-to-back. Also accepts plain
/// single-stream files, so objects written by earlier versions
/// remain readable.
pub fn read_concatenated_ipc(bytes: Vec<u8>) -> Result<Vec<RecordBatch>, PylonError> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut out = Vec::new();
    while (cursor.position() as usize) < cursor.get_ref().len() {
        let mut reader = StreamReader::try_new(&mut cursor, None).map_err(ipc_err)?;
        for batch in reader.by_ref() {
            out.push(batch.map_err(ipc_err)?);
        }
    }
    Ok(out)
}

fn ipc_err(e: arrow_schema::ArrowError) -> PylonError {
    PylonError::Arrow(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    #[test]
    fn single_stream_roundtrip() {
        let stream = encode_batch_stream(&schema(), &batch(&[1, 2, 3])).unwrap();
        let out = read_concatenated_ipc(stream).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[test]
    fn concatenated_streams_roundtrip() {
        let s1 = encode_batch_stream(&schema(), &batch(&[1, 2])).unwrap();
        let s2 = encode_batch_stream(&schema(), &batch(&[3])).unwrap();
        let out = read_concatenated_ipc([s1, s2].concat()).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].num_rows(), 2);
        assert_eq!(out[1].num_rows(), 1);
    }
}
