//! Cloud-storage attachment hosting: upload an over-limit attachment to the
//! account's cloud drive and return a shareable link, in one call. bifrost owns
//! the upload/link wire protocol and error classification; the consumer owns
//! the size threshold, warn-vs-host UX, and link insertion.

/// Who a hosted file is shared with. The uniform vocabulary; each provider maps
/// it onto its own wire vocabulary (Drive `type: anyone` vs `type: domain`;
/// OneDrive `scope: anonymous` vs `scope: organization`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShareScope {
    /// Anyone with the link can view. Drive `type: "anyone"`; OneDrive
    /// `scope: "anonymous"`.
    Anyone,
    /// Only members of the account's organization/domain can view. Drive
    /// `type: "domain"` (the account's primary domain); OneDrive
    /// `scope: "organization"`.
    Organization,
}

/// Metadata the consumer supplies for a host request. Upload length is derived
/// from the bytes supplied to `Account::host_attachment`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CloudUploadMeta {
    /// The file name the hosted item is given on the drive.
    pub file_name: String,
    // Preserve the declared wire media type. parse_content_type is a tolerant
    // inbound parser: malformed input becomes text/plain, which would silently
    // change an outbound declaration or hide inbound evidence.
    /// The MIME type declared for the uploaded bytes.
    pub mime: String,
    /// Who the minted share link grants access to.
    pub scope: ShareScope,
}

impl CloudUploadMeta {
    /// Construct upload metadata from a file name, MIME type, and
    /// share scope.
    #[must_use]
    pub fn new(file_name: impl Into<String>, mime: impl Into<String>, scope: ShareScope) -> Self {
        Self {
            file_name: file_name.into(),
            mime: mime.into(),
            scope,
        }
    }
}

/// The result of a successful host: the file is on the drive AND a link exists.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct HostedAttachment {
    /// The shareable URL to insert into the message body.
    pub share_url: String,
    /// Provider-minted file/item id (Drive file id, OneDrive drive-item id).
    pub provider_file_id: String,
}

impl HostedAttachment {
    /// Construct a hosted-attachment result from a share URL and the
    /// provider-minted file/item id.
    #[must_use]
    pub fn new(share_url: impl Into<String>, provider_file_id: impl Into<String>) -> Self {
        Self {
            share_url: share_url.into(),
            provider_file_id: provider_file_id.into(),
        }
    }
}
