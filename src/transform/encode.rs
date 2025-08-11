//! Arrow IPC encoding (IDs → Arrow file bytes)
//! Note: Arrow 56 doesn't support setting IPC compression via
//! `IpcWriteOptions` fields. We emit uncompressed IPC here.

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use bytes::Bytes;

pub fn encode_many_ids_arrow_bytes(ids: &[Bytes], _use_zstd: bool) -> anyhow::Result<Vec<u8>> {
    // Schema: one Binary column "id"
    let field = Field::new("id", DataType::Binary, false);
    let schema = Arc::new(Schema::new(vec![field]));

    // Build the Binary column
    let mut builder = BinaryBuilder::new();
    for b in ids {
        builder.append_value(b);
    }
    let array = Arc::new(builder.finish()) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;

    // Serialize to IPC file
    let mut buf = Vec::with_capacity(64 * 1024);
    let mut cursor = std::io::Cursor::new(&mut buf);

    let opts = IpcWriteOptions::default();
    let mut writer = FileWriter::try_new_with_options(&mut cursor, &schema, opts)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(buf)
}

// Placeholder for future pooling if needed.
pub fn return_encoded_buf_to_pool(_buf: Vec<u8>) {}
