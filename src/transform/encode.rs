use crate::{errors::CompilerError, proto::Job};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use std::{io::Cursor, sync::Arc};

pub fn encode_replay_delta_arrow(job: &Job) -> Result<Bytes, CompilerError> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let id_col = Arc::new(StringArray::from(vec![job.id.clone()])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![id_col])
        .map_err(|e| CompilerError::JobProcessingError(e.to_string()))?;
    let mut buf = Vec::new();
    let cur = Cursor::new(&mut buf);
    let mut w = FileWriter::try_new(cur, &schema)
        .map_err(|e| CompilerError::JobProcessingError(e.to_string()))?;
    w.write(&batch)
        .map_err(|e| CompilerError::JobProcessingError(e.to_string()))?;
    w.finish()
        .map_err(|e| CompilerError::JobProcessingError(e.to_string()))?;
    Ok(Bytes::from(buf))
}
