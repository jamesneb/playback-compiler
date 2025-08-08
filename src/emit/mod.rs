use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::ipc::reader::FileReader;
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

pub fn append_to_arrow(new_id: &str, existing_data: Option<&[u8]>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    
    let mut all_ids = Vec::new();
    
    // If we have existing data, read the existing IDs first
    if let Some(data) = existing_data {
        let cursor = Cursor::new(data);
        match FileReader::try_new(cursor, None) {
            Ok(reader) => {
                for batch_result in reader {
                    if let Ok(batch) = batch_result {
                        if let Some(id_column) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                            for i in 0..id_column.len() {
                                let existing_id = id_column.value(i);
                                all_ids.push(existing_id.to_string());
                            }
                        }
                    }
                }
            },
            Err(_) => {
                // If we can't read existing data, just start fresh
            }
        }
    }
    
    // Add the new ID
    all_ids.push(new_id.to_string());
    
    // Create new Arrow file with all IDs
    let id_array = Arc::new(StringArray::from(all_ids)) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array])?;

    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let mut writer = FileWriter::try_new(cursor, &schema)?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(buffer)
}
