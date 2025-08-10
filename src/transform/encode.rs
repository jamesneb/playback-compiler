//! Replay delta encoding (Arrow IPC)
//!
//! Overview
//! --------
//! Encodes replay delta records into Arrow IPC format using a stable schema.
//! The first column is a non-null Utf8 `id`. New fields should be appended as
//! nullable columns to preserve forward/backward compatibility.
//!
//! Compatibility
//! -------------
//! - Use FileWriter for self-contained files (replay deltas stored in object storage).
//! - StreamWriter can be introduced for continuous pipelines if needed.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;

use crate::proto::Job;

// Static schema to avoid re-allocating per message.
static SCHEMA: Lazy<Arc<Schema>> =
    Lazy::new(|| Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])));

pub fn encode_replay_delta_arrow(job: &Job) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let id_array = Arc::new(StringArray::from(vec![job.id.clone()])) as ArrayRef;
    let batch = RecordBatch::try_new(SCHEMA.clone(), vec![id_array])?;

    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let mut writer = FileWriter::try_new(cursor, &SCHEMA)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(buffer)
}
