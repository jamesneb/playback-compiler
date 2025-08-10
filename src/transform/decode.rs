//! Job decoding (Protobuf)
//!
//! Overview
//! --------
//! Decodes inbound job payloads (protobuf) into a typed `Job`. Invalid payloads
//! return a descriptive error for caller-controlled routing (e.g., DLQ).

use bytes::Bytes;

use crate::{errors::CompilerError, proto::Job};
use prost::Message;

pub fn decode_job(payload: &Bytes) -> Result<Job, CompilerError> {
    Job::decode(&payload[..]).map_err(|e| CompilerError::Decode(e.to_string()))
}
