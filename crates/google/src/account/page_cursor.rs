//! Caller-owned Google page walks, distinct from durable sync cursors.

use bifrost_types::{AccountError, AccountOperation, PageCursor, Protocol};

fn protocol(operation: AccountOperation) -> Protocol {
    match operation {
        AccountOperation::Search | AccountOperation::SearchMessages => Protocol::Gmail,
        AccountOperation::EventsInRange | AccountOperation::EventSearch => Protocol::GoogleCalendar,
        _ => Protocol::GooglePeople,
    }
}

pub(crate) fn encode(operation: AccountOperation, token: &str) -> PageCursor {
    format!("google-page-1:{}:{token}", operation.ledger_tag())
        .into_bytes()
        .into()
}

pub(crate) fn token(cursor: &[u8], operation: AccountOperation) -> Result<&str, AccountError> {
    let invalid = || {
        AccountError::invalid_page_cursor(
            operation,
            protocol(operation),
            "malformed, retired or foreign Google page cursor",
        )
    };
    let text = std::str::from_utf8(cursor).map_err(|_| invalid())?;
    // An Outlook user expects stale saved results to reload, without clearing
    // durable account state or sending another surface's token to Google.
    text.strip_prefix(&format!("google-page-1:{}:", operation.ledger_tag()))
        .ok_or_else(invalid)
}

pub(crate) fn decode(
    cursor: Option<PageCursor>,
    operation: AccountOperation,
) -> Result<Option<String>, AccountError> {
    cursor
        .map(|cursor| token(&cursor, operation).map(str::to_owned))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, Cause, RequestCause, RequestErrorKind};

    #[test]
    fn page_cursors_are_surface_bound_and_share_the_restart_signal() {
        let operations = [
            AccountOperation::Search,
            AccountOperation::SearchMessages,
            AccountOperation::ContactsList,
            AccountOperation::ContactSearch,
            AccountOperation::DirectorySearch,
            AccountOperation::EventsInRange,
            AccountOperation::EventSearch,
        ];
        for minted in operations {
            let cursor = encode(minted, "opaque:token");
            assert_eq!(token(&cursor, minted).unwrap(), "opaque:token");
            for used in operations.into_iter().filter(|used| *used != minted) {
                let error = token(&cursor, used).expect_err("wrong surface");
                assert_eq!(error.operation(), Some(used));
                assert_eq!(error.protocol(), Some(protocol(used)));
                assert!(matches!(
                    error.kind(),
                    AccountErrorKind::Request(RequestErrorKind::Malformed)
                ));
                assert!(error.chain().iter().any(|cause| matches!(
                    cause,
                    Cause::Request(RequestCause::InvalidArgument {
                        field: Some("page_cursor"),
                        ..
                    })
                )));
            }
        }
        assert!(token(&[0xff], AccountOperation::SearchMessages).is_err());
        assert!(token(b"bare old token", AccountOperation::ContactsList).is_err());
    }
}
