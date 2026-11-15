use crate::core::{
    id::{AccountId, BlobId},
    session::URLParser,
};

pub(crate) mod copy;
pub(crate) mod download;
pub(crate) mod manage;
pub(crate) mod upload;

#[non_exhaustive]
pub(crate) enum URLParameter {
    AccountId,
    BlobId,
    Name,
    Type,
}

impl URLParser for URLParameter {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "accountId" => Some(URLParameter::AccountId),
            "blobId" => Some(URLParameter::BlobId),
            "name" => Some(URLParameter::Name),
            "type" => Some(URLParameter::Type),
            _ => None,
        }
    }
}

// Uploads have no blob or filename yet. Reject download-only parameters
// while parsing the session instead of silently sending an altered URL.
#[non_exhaustive]
pub(crate) enum UploadParameter {
    AccountId,
}

impl URLParser for UploadParameter {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "accountId" => Some(Self::AccountId),
            _ => None,
        }
    }
}

/// A reference to a blob, sufficient to construct a download URL.
///
/// The unit that travels through the API for blob identity. RFC 8620
/// §6 specifies that download URLs are templated with `accountId`
/// and `blobId`, and may also reference `name` and `type` to populate
/// the response's `Content-Disposition` and `Content-Type`. Bundling
/// these into one ref:
///
/// 1. Forces the caller to keep `accountId` paired with `blobId`,
///    which is required when the blob lives in a different account
///    than the client's default (cross-account email, shared
///    calendar attachments).
/// 2. Lets `name` and `type` flow into the URL template instead of
///    being silently dropped.
///
/// Functions returning blob references (`Email/get` for attachments,
/// `Email/import` results, `Account::upload`, etc.) hand back
/// `BlobRef` so consumers do not reconstruct one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) struct BlobRef {
    /// The account that owns this blob.
    pub(crate) account_id: AccountId,
    /// The blob's server-assigned ID.
    pub(crate) blob_id: BlobId,
    /// Suggested filename for `Content-Disposition`. `None` falls back
    /// to a neutral default at download time.
    pub(crate) name: Option<String>,
    /// Suggested MIME type for `Content-Type`. `None` falls back to
    /// `application/octet-stream` at download time.
    pub(crate) content_type: Option<String>,
}

impl BlobRef {
    /// Construct a `BlobRef` from an account + blob ID, with no
    /// name or content type hints.
    pub(crate) fn new(account_id: AccountId, blob_id: BlobId) -> Self {
        Self {
            account_id,
            blob_id,
            name: None,
            content_type: None,
        }
    }

    /// Set the suggested filename hint.
    #[must_use]
    pub(crate) fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the suggested MIME type hint.
    #[must_use]
    pub(crate) fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }
}
