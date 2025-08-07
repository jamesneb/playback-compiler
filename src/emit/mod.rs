use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;

pub mod uploader;

pub fn write_to_arrow(id: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let id_array = Arc::new(StringArray::from(vec![id])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array])?;

    // Write to in-memory buffer instead of file
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let mut writer = FileWriter::try_new(cursor, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(buffer)
}
