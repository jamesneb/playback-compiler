//! Common types and small helpers
//!
//! Overview
//! --------
//! Centralizes lightweight type aliases and utility helpers shared across
//! modules. Keep this module dependency-free (beyond core/std/bytes) to avoid
//! cyclic build edges.
//!
//! Guidelines
//! ----------
//! - Prefer **type aliases** over newtypes unless invariants are enforced.
//! - Keep helpers side-effect free; IO belongs in feature modules.
pub type Bytes<'a> = &'a [u8];
pub mod fp;
