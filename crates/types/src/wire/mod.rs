//! Stable storage codecs for shared account vocabulary.
//!
//! Matches on types-owned enums live here so adding a variant requires an
//! explicit storage representation at compile time. Existing tags and byte
//! layouts must stay stable; new tags must update the relevant version gate.
//! The sync engine composes these codecs with its own ledger state.

pub mod cursor;
pub mod ledger;
pub mod partition;

/// A malformed or unreadable durable representation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// A well-formed representation from an unsupported schema.
    #[error("storage schema is incompatible")]
    SchemaIncompatible,
    /// Invalid bytes within a supported schema.
    #[error("{0}")]
    Malformed(String),
}

use DecodeError as Error;

/// Append a UTF-8 string with a little-endian u32 byte length.
pub fn write_string(out: &mut Vec<u8>, s: &str) {
    write_bytes(out, s.as_bytes());
}

/// Append bytes with a little-endian u32 length; oversized fields panic.
pub fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("cursor envelope: field exceeds u32 length limit");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Read a length-prefixed UTF-8 string and return the consumed byte count.
pub fn read_string(bytes: &[u8]) -> Result<(String, usize), Error> {
    let (raw, consumed) = read_bytes(bytes)?;
    let s = String::from_utf8(raw)
        .map_err(|e| Error::Malformed(format!("cursor envelope: invalid UTF-8: {e}")))?;
    Ok((s, consumed))
}

/// Read length-prefixed bytes and return the consumed byte count.
pub fn read_bytes(bytes: &[u8]) -> Result<(Vec<u8>, usize), Error> {
    if bytes.len() < 4 {
        return Err(Error::Malformed(
            "cursor envelope: missing length prefix".into(),
        ));
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let end = 4usize
        .checked_add(len)
        .ok_or_else(|| Error::Malformed("cursor envelope: length overflow".into()))?;
    if bytes.len() < end {
        return Err(Error::Malformed(
            "cursor envelope: truncated length-prefixed bytes".into(),
        ));
    }
    Ok((bytes[4..end].to_vec(), end))
}

/// Sequential cursor over the payload.
///
/// The cursor envelope's primitives return `(value, consumed)` and let each
/// call site thread the offset, which is workable for its handful of fields and
/// is not workable for a ledger's nesting depth. Same encodings, one place that
/// tracks the position.
#[derive(Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    /// The row's envelope version, for tags a version introduced: a tag an
    /// older writer could not have produced is corruption in an older row.
    version: u32,
}

impl<'a> Reader<'a> {
    /// Envelope version supplied by the enclosing storage codec.
    #[must_use]
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Whether all bytes in this payload have been consumed.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.at == self.bytes.len()
    }

    /// Construct a reader for a payload and its enclosing ledger version.
    /// The caller enforces the readable version window before decoding fields.
    #[must_use]
    pub fn new(bytes: &'a [u8], version: u32) -> Self {
        Self {
            bytes,
            at: 0,
            version,
        }
    }

    /// Read exactly the requested number of bytes.
    pub fn take(&mut self, count: usize) -> Result<&'a [u8], Error> {
        let end = self
            .at
            .checked_add(count)
            .ok_or_else(|| Error::Malformed("ledger envelope: length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(Error::Malformed(
                "ledger envelope: truncated payload".into(),
            ));
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    /// Read one byte.
    pub fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    /// Read a little-endian u16.
    pub fn u16(&mut self) -> Result<u16, Error> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    /// Read a little-endian u32.
    pub fn u32(&mut self) -> Result<u32, Error> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Read a little-endian u64.
    pub fn u64(&mut self) -> Result<u64, Error> {
        let bytes = self.take(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(u64::from_le_bytes(buf))
    }

    /// Read a fixed-size byte array.
    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let bytes = self.take(N)?;
        let mut buf = [0u8; N];
        buf.copy_from_slice(bytes);
        Ok(buf)
    }

    /// Read a little-endian i64.
    pub fn i64(&mut self) -> Result<i64, Error> {
        let bytes = self.take(8)?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        Ok(i64::from_le_bytes(buf))
    }

    /// Read length-prefixed bytes.
    pub fn bytes(&mut self) -> Result<Vec<u8>, Error> {
        let (value, consumed) = read_bytes(&self.bytes[self.at..])?;
        self.at += consumed;
        Ok(value)
    }

    /// Read a length-prefixed UTF-8 string.
    pub fn string(&mut self) -> Result<String, Error> {
        let (value, consumed) = read_string(&self.bytes[self.at..])?;
        self.at += consumed;
        Ok(value)
    }
}

#[cfg(test)]
mod tests;
