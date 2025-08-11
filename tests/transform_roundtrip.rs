//! Unit: Arrow encode/round-trip (BinaryArray)
use arrow::array::{Array, BinaryArray};
use arrow::ipc::reader::FileReader;
use bytes::Bytes;
use playback_compiler::proto::Job;
use playback_compiler::transform::{decode::decode_job, encode::encode_many_ids_arrow_bytes};
use std::io::Cursor;

#[test]
fn decode_rejects_garbage() {
    let payload = Bytes::from_static(b"\x00\x01\x02");
    let err = decode_job(&payload).unwrap_err();
    let s = format!("{err:?}");
    assert!(s.contains("Decode"), "expected Decode error, got: {s}");
}

#[test]
fn arrow_contains_id_roundtrip() {
    let id = "abc-123".to_string();
    let _job = Job { id: id.clone() };

    let bytes = encode_many_ids_arrow_bytes(&[Bytes::from(id.clone())], false).expect("encode");
    assert!(!bytes.is_empty(), "arrow bytes should not be empty");

    let mut reader = FileReader::try_new(Cursor::new(bytes), None).expect("reader");
    let mut found = false;
    for batch in &mut reader {
        let b = batch.expect("batch");
        let ba = b
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary column 0");
        for i in 0..ba.len() {
            if ba.value(i) == id.as_bytes() {
                found = true;
                break;
            }
        }
        if found {
            break;
        }
    }
    assert!(found, "expected id abc-123 in Arrow data");
}
