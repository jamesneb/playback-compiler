use crate::{errors::CompilerError, proto::Job};
use bytes::Bytes;
use prost::Message;

pub fn decode_job(payload: &Bytes) -> Result<Job, CompilerError> {
    Job::decode(&payload[..]).map_err(|e| CompilerError::Decode(e.to_string()))
}
