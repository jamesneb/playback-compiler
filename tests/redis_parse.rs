//! Unit tests for parsing `XREAD` responses into `QueueMessage` values.

use bytes::Bytes;
use deadpool_redis::redis::Value;
use playback_compiler::ingest::QueueMessage;
use playback_compiler::redis::parse_xread_value;

#[test]
fn parse_minimal_xread_reply() {
    let reply = Value::Bulk(vec![Value::Bulk(vec![
        Value::Data(b"jobs".to_vec()),
        Value::Bulk(vec![Value::Bulk(vec![
            Value::Data(b"1-0".to_vec()),
            Value::Bulk(vec![
                Value::Data(b"payload".to_vec()),
                Value::Data(b"\x08\x01".to_vec()),
            ]),
        ])]),
    ])]);

    let msgs: Vec<QueueMessage> = parse_xread_value(reply);
    assert_eq!(msgs.len(), 1);
    let msg = &msgs[0];
    assert_eq!(msg.id, "1-0");
    assert_eq!(msg.payload, Bytes::from_static(b"\x08\x01"));
}

#[test]
fn parse_ignores_missing_payload() {
    let reply = Value::Bulk(vec![Value::Bulk(vec![
        Value::Data(b"jobs".to_vec()),
        Value::Bulk(vec![Value::Bulk(vec![
            Value::Data(b"2-0".to_vec()),
            Value::Bulk(vec![Value::Data(b"k".to_vec()), Value::Data(b"v".to_vec())]),
        ])]),
    ])]);

    let msgs = parse_xread_value(reply);
    assert!(msgs.is_empty());
}

#[test]
fn parse_handles_empty_reply() {
    assert!(parse_xread_value(Value::Nil).is_empty());
    assert!(parse_xread_value(Value::Bulk(vec![])).is_empty());
}
