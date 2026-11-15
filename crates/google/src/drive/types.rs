use serde::{Deserialize, Serialize};

/// The completed file metadata returned after a successful upload.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GDriveFileResponse {
    pub(crate) id: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) web_view_link: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) size: Option<String>,
}

/// Request body for file metadata when creating an upload session.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FileMetadata<'a> {
    pub(crate) name: &'a str,
    pub(crate) mime_type: &'a str,
}

/// Response from getting file metadata with `webViewLink`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FileWebLinkResponse {
    pub(crate) web_view_link: String,
}
