//! Common type aliases and helper modules shared across the crate.
//!
//! This module remains dependency-light to avoid cyclic build edges.
pub type Bytes<'a> = &'a [u8];
pub mod fp;
