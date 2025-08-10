use arrow::array::{Array, StringArray};
use arrow::ipc::reader::FileReader;
use bytes::Bytes;
use playback_compiler::proto::Job;
use playback_compiler::transform::{decode::decode_job, encode::encode_replay_delta_arrow};
use proptest::prelude::*;
use proptest::string::string_regex;
use std::io::Cursor;

// IDs: mix ASCII (no newlines) and general Unicode scalar values.
// - The Unicode generator uses `char` (scalar values only; no surrogates).
fn any_id() -> impl Strategy<Value = String> {
    let ascii = string_regex(r"[^\n]{0,1024}").unwrap();
    let unicode = proptest::collection::vec(any::<char>(), 0..512)
        .prop_map(|v| v.into_iter().collect::<String>());
    prop_oneof![ascii, unicode]
}

proptest! {
  // encode → arrow → read back → contains id
  #[test]
  fn arrow_roundtrips_id(id in any_id()) {
      let job = Job { id: id.clone() };
      let bytes = encode_replay_delta_arrow(&job).expect("encode");
      prop_assume!(!bytes.is_empty());

      let mut reader = FileReader::try_new(Cursor::new(bytes), None).expect("reader");
      let mut found = false;
      for rb in &mut reader {
          let b = rb.expect("batch");
          let sa = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
          for i in 0..sa.len() {
              if sa.value(i) == id { found = true; break; }
          }
          if found { break; }
      }
      prop_assert!(found);
  }

  // Random bytes should fail to decode as a valid Job
  #[test]
  fn decode_rejects_random_bytes(buf in proptest::collection::vec(any::<u8>(), 0..4096)) {
      let payload = Bytes::from(buf);
      let _ = decode_job(&payload).unwrap_err();
  }
}
