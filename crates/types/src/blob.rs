//! Blob handles and byte-range types.
//!
//! Blob enumeration is separated from hydration. Inventory streams
//! yield `BlobHandle`s; the consumer opens them on demand via
//! `Account::open_blob` or `Account::open_blob_range`. The handle
//! carries enough capability metadata to let the engine decide
//! between single-stream and parallel-range fetching.

use crate::ids::BlobId;

/// Half-open byte window `[start, start + length)` for partial blob fetches.
/// HTTP adapters translate the length to an inclusive wire endpoint.
///
/// `length = None` is open-ended to the end of the blob; servers
/// answer with whatever they have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub length: Option<u64>,
}

/// Algorithm-tagged content hash for blob dedup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    pub algorithm: DigestAlgorithm,
    pub value: Vec<u8>,
}

/// Hash algorithms used by `Digest`. Includes legacy options because
/// some providers still ship them on attachment metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DigestAlgorithm {
    Sha256,
    Sha1,
    Md5,
}

/// On-wire encoding the blob is transferred in. Tells the consumer
/// whether to decode (Gmail attachments are base64url in JSON,
/// everything else is raw bytes on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BlobEncoding {
    Raw7Bit,
    Raw8Bit,
    Base64,
    Base64Url,
    QuotedPrintable,
}

/// Per-handle blob capabilities.
///
/// Gmail attachments are base64url in JSON, so range resume is not
/// generally available there. Graph file attachments support range;
/// item attachments do not. The flags say so honestly per-handle.
#[derive(Debug, Clone)]
pub struct BlobCapabilities {
    pub supports_range: bool,
    pub supports_parallel: bool,
    pub digest_available_pre_download: bool,
    pub encoding: BlobEncoding,
}

/// Engine-facing blob descriptor.
///
/// `size` and `content_type` are `Option` because not every protocol
/// surfaces them at enumeration time. `digest` is `Some` only when
/// `AccountCapabilities::blob_digest_pre_download` is true (otherwise
/// the consumer must post-hash after download).
#[derive(Debug, Clone)]
pub struct BlobHandle {
    pub id: BlobId,
    pub size: Option<u64>,
    pub content_type: Option<String>,
    pub digest: Option<Digest>,
    pub capabilities: BlobCapabilities,
}
