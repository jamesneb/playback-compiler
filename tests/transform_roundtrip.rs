//! Unit: Arrow encode/round-trip
//! Verifies that encoded deltas can be read back and contain the expected ids.

use arrow::array::Array;
use arrow::array::StringArray;
use arrow::ipc::reader::FileReader;
use bytes::Bytes;
use playback_compiler::proto::Job;
use playback_compiler::transform::{decode::decode_job, encode::encode_replay_delta_arrow};
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
    let job = Job {
        id: "abc-123".into(),
    };
    let bytes = encode_replay_delta_arrow(&job).expect("encode");
    assert!(!bytes.is_empty(), "arrow bytes should not be empty");

    let mut reader = FileReader::try_new(Cursor::new(bytes), None).expect("reader");
    let mut found = false;
    for batch in &mut reader {
        let b = batch.expect("batch");
        let sa = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column 0");
        for i in 0..sa.len() {
            if sa.value(i) == "abc-123" {
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
