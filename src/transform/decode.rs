//! Decode Protobuf job payloads into typed `Job` instances.
//!
//! Invalid bytes yield a structured error so callers can route failures
//! appropriately.

use bytes::Bytes;

use crate::{errors::CompilerError, proto::Job};
use prost::Message;

pub fn decode_job(payload: &Bytes) -> Result<Job, CompilerError> {
    Job::decode(&payload[..]).map_err(|e| CompilerError::Decode(e.to_string()))
}
