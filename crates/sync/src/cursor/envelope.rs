//! Cursor storage entry points, using exhaustive types-owned wire codecs.

use crate::error::Error;
use bifrost_types::Checkpoint;

use bifrost_types::wire::cursor;
pub use bifrost_types::wire::cursor::{
    CursorEnvelope, ENGINE_VERSION, EnvelopeKind, MIN_MIGRATABLE, encode_envelope,
};
pub(super) use bifrost_types::wire::{write_bytes, write_string};

/// Decode and migrate a stored checkpoint, preserving the engine's schema
/// recovery classification for unsupported versions and historical reserved tags.
pub fn decode_envelope(bytes: &[u8]) -> Result<Checkpoint, Error> {
    cursor::decode_envelope(bytes).map_err(Error::from)
}

pub(super) fn decode_scope(bytes: &[u8]) -> Result<bifrost_types::CursorScope, Error> {
    cursor::decode_scope(bytes).map_err(Error::from)
}
pub(super) use bifrost_types::wire::cursor::encode_scope;
