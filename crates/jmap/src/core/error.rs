use std::fmt::Display;

use serde::Deserialize;

// types: protocol-specific RFC 7807 body kept so JMAP error mapping can inspect type and limit.
#[derive(Debug, Deserialize)]
#[non_exhaustive]
pub(crate) struct ProblemDetails {
    #[serde(rename = "type")]
    #[serde(default = "default_problem_type")]
    p_type: ProblemType,
    status: Option<u32>,
    title: Option<String>,
    detail: Option<String>,
    limit: Option<String>,
    // HTTP-side RFC 7807 bodies never populate this (snake_case with no
    // rename, and JMAP problem bodies do not carry it anyway); only the
    // WebSocket RequestError constructor fills it in.
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[non_exhaustive]
pub(crate) enum JMAPError {
    #[serde(rename = "urn:ietf:params:jmap:error:unknownCapability")]
    UnknownCapability,
    #[serde(rename = "urn:ietf:params:jmap:error:notJSON")]
    NotJSON,
    #[serde(rename = "urn:ietf:params:jmap:error:notRequest")]
    NotRequest,
    #[serde(rename = "urn:ietf:params:jmap:error:limit")]
    Limit,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum ProblemType {
    JMAP(JMAPError),
    Other(String),
}

fn default_problem_type() -> ProblemType {
    // RFC 7807 §3.1 assigns `about:blank` when the member is absent.
    ProblemType::Other("about:blank".to_string())
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) struct MethodError {
    #[serde(rename = "type")]
    p_type: MethodErrorType,
    /// RFC 8620 s3.6.2: the server's own explanation of the failure.
    /// Preserved for support diagnostics rather than dropped at the wire.
    #[serde(default)]
    description: Option<String>,
    /// The per-error `limit` name some errors carry (`requestTooLarge`,
    /// `tooManyChanges` name the limit that was hit).
    #[serde(default)]
    limit: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum MethodErrorType {
    ServerUnavailable,
    ServerFail,
    ServerPartialFail,
    UnknownMethod,
    InvalidArguments,
    InvalidResultReference,
    Forbidden,
    AccountNotFound,
    AccountNotSupportedByMethod,
    AccountReadOnly,
    RequestTooLarge,
    CannotCalculateChanges,
    StateMismatch,
    AlreadyExists,
    FromAccountNotFound,
    FromAccountNotSupportedByMethod,
    AnchorNotFound,
    UnsupportedSort,
    UnsupportedFilter,
    TooManyChanges,
    /// Catch-all for unrecognized method-error type strings. Carries
    /// the wire-supplied identifier so the JMAP conversion can map it
    /// to `WireCause::Jmap(JmapMethod::Unknown { code })` without
    /// inventing placeholder values. Serde's `#[serde(other)]` would
    /// discard the string, so this variant uses a hand-rolled
    /// `Deserialize` impl.
    Other(String),
}

impl<'de> serde::Deserialize<'de> for MethodErrorType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "serverUnavailable" => Self::ServerUnavailable,
            "serverFail" => Self::ServerFail,
            "serverPartialFail" => Self::ServerPartialFail,
            "unknownMethod" => Self::UnknownMethod,
            "invalidArguments" => Self::InvalidArguments,
            "invalidResultReference" => Self::InvalidResultReference,
            "forbidden" => Self::Forbidden,
            "accountNotFound" => Self::AccountNotFound,
            "accountNotSupportedByMethod" => Self::AccountNotSupportedByMethod,
            "accountReadOnly" => Self::AccountReadOnly,
            "requestTooLarge" => Self::RequestTooLarge,
            "cannotCalculateChanges" => Self::CannotCalculateChanges,
            "stateMismatch" => Self::StateMismatch,
            "alreadyExists" => Self::AlreadyExists,
            "fromAccountNotFound" => Self::FromAccountNotFound,
            "fromAccountNotSupportedByMethod" => Self::FromAccountNotSupportedByMethod,
            "anchorNotFound" => Self::AnchorNotFound,
            "unsupportedSort" => Self::UnsupportedSort,
            "unsupportedFilter" => Self::UnsupportedFilter,
            "tooManyChanges" => Self::TooManyChanges,
            _ => Self::Other(value),
        })
    }
}

impl ProblemDetails {
    pub(crate) fn new(
        p_type: ProblemType,
        status: Option<u32>,
        title: Option<String>,
        detail: Option<String>,
        limit: Option<String>,
        request_id: Option<String>,
    ) -> Self {
        ProblemDetails {
            p_type,
            status,
            title,
            detail,
            limit,
            request_id,
        }
    }

    pub(crate) fn error(&self) -> &ProblemType {
        &self.p_type
    }

    pub(crate) fn status(&self) -> Option<u32> {
        self.status
    }

    pub(crate) fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    pub(crate) fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub(crate) fn limit(&self) -> Option<&str> {
        self.limit.as_deref()
    }

    pub(crate) fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
}

impl MethodError {
    pub(crate) fn error_type(&self) -> &MethodErrorType {
        &self.p_type
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn limit(&self) -> Option<&str> {
        self.limit.as_deref()
    }
}

impl Display for MethodError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.p_type {
            MethodErrorType::ServerUnavailable => write!(f, "Server unavailable"),
            MethodErrorType::ServerFail => write!(f, "Server fail"),
            MethodErrorType::ServerPartialFail => write!(f, "Server partial fail"),
            MethodErrorType::UnknownMethod => write!(f, "Unknown method"),
            MethodErrorType::InvalidArguments => write!(f, "Invalid arguments"),
            MethodErrorType::InvalidResultReference => write!(f, "Invalid result reference"),
            MethodErrorType::Forbidden => write!(f, "Forbidden"),
            MethodErrorType::AccountNotFound => write!(f, "Account not found"),
            MethodErrorType::AccountNotSupportedByMethod => {
                write!(f, "Account not supported by method")
            }
            MethodErrorType::AccountReadOnly => write!(f, "Account read only"),
            MethodErrorType::RequestTooLarge => write!(f, "Request too large"),
            MethodErrorType::CannotCalculateChanges => write!(f, "Cannot calculate changes"),
            MethodErrorType::StateMismatch => write!(f, "State mismatch"),
            MethodErrorType::AlreadyExists => write!(f, "Already exists"),
            MethodErrorType::FromAccountNotFound => write!(f, "From account not found"),
            MethodErrorType::FromAccountNotSupportedByMethod => {
                write!(f, "From account not supported by method")
            }
            MethodErrorType::AnchorNotFound => write!(f, "Anchor not found"),
            MethodErrorType::UnsupportedSort => write!(f, "Unsupported sort"),
            MethodErrorType::UnsupportedFilter => write!(f, "Unsupported filter"),
            MethodErrorType::TooManyChanges => write!(f, "Too many changes"),
            MethodErrorType::Other(code) => write!(f, "Other ({code})"),
        }
    }
}

impl Display for ProblemDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.p_type {
            ProblemType::JMAP(err) => match err {
                JMAPError::UnknownCapability => write!(f, "Unknown capability")?,
                JMAPError::NotJSON => write!(f, "Not JSON")?,
                JMAPError::NotRequest => write!(f, "Not request")?,
                JMAPError::Limit => write!(f, "Limit")?,
            },
            ProblemType::Other(err) => f.write_str(err.as_str())?,
        }

        if let Some(status) = self.status {
            write!(f, " (status {status})")?;
        }

        if let Some(title) = &self.title {
            write!(f, ": {title}")?;
        }

        if let Some(detail) = &self.detail {
            write!(f, ". Details: {detail}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ProblemDetails, ProblemType};

    #[test]
    fn a_problem_without_type_defaults_to_about_blank_and_keeps_its_text() {
        let problem: ProblemDetails = serde_json::from_value(serde_json::json!({
            "status": 429,
            "title": "Slow down",
            "detail": "The account is over its request limit"
        }))
        .expect("RFC 7807 permits an omitted type");

        assert!(matches!(
            problem.error(),
            ProblemType::Other(value) if value == "about:blank"
        ));
        assert_eq!(problem.status(), Some(429));
        assert_eq!(problem.title(), Some("Slow down"));
        assert_eq!(
            problem.detail(),
            Some("The account is over its request limit")
        );
    }
}
