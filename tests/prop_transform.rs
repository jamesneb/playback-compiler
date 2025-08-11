use arrow::array::{Array, BinaryArray};
use arrow::ipc::reader::FileReader;
use bytes::Bytes;
use playback_compiler::proto::Job;
use playback_compiler::transform::decode::decode_job;
use playback_compiler::transform::encode::encode_many_ids_arrow_bytes;
use proptest::prelude::*;
use proptest::string::string_regex;
use std::io::Cursor;

// Generate identifiers with ASCII or general Unicode characters.
fn any_id() -> impl Strategy<Value = String> {
    let ascii = string_regex(r"[^\n]{0,1024}").unwrap();
    let unicode = proptest::collection::vec(any::<char>(), 0..512)
        .prop_map(|v| v.into_iter().collect::<String>());
    prop_oneof![ascii, unicode]
}

proptest! {
  // Encode identifiers to Arrow, read back, and verify the ID is present.
  #[test]
  fn arrow_roundtrips_id(id in any_id()) {
      // Job is constructed to mirror realistic usage even if unused.
      let _job = Job { id: id.clone() };

      let bytes = encode_many_ids_arrow_bytes(&[Bytes::from(id.clone())], false).expect("encode");
      prop_assume!(!bytes.is_empty());

      let mut reader = FileReader::try_new(Cursor::new(bytes), None).expect("reader");
      let mut found = false;
      for rb in &mut reader {
          let b = rb.expect("batch");
          let ba = b.column(0).as_any().downcast_ref::<BinaryArray>().expect("binary col 0");
          for i in 0..ba.len() {
              if ba.value(i) == id.as_bytes() { found = true; break; }
          }
          if found { break; }
      }
      prop_assert!(found);
  }

  // Random bytes should fail to decode as a valid Job.
  #[test]
  fn decode_rejects_random_bytes(buf in proptest::collection::vec(any::<u8>(), 0..4096)) {
      let payload = Bytes::from(buf);
      let _ = decode_job(&payload).unwrap_err();
  }
}
