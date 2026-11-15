//! Correlate a single-object read before projecting its contents.

use bifrost_types::{AccountError, AccountOperation, ErrorScope};

use super::error::{JmapErrorContext, get_id_unanswered, into_account_error};

pub(super) fn require_one<T>(
    items: Vec<T>,
    matches_id: impl Fn(&T) -> bool,
    not_found: bool,
    id: &str,
    operation: AccountOperation,
    scope: ErrorScope,
) -> Result<T, AccountError> {
    let context = JmapErrorContext::new(operation).with_scope(scope);
    // An unrelated object is not evidence about the requested identity.
    // Only an explicit notFound answer establishes absence; an omitted
    // answer remains retryable and must not masquerade as a deletion.
    if let Some(item) = items.into_iter().find(matches_id) {
        Ok(item)
    } else if not_found {
        Err(into_account_error(
            crate::Error::IdNotFound(id.to_string()),
            context,
        ))
    } else {
        Err(get_id_unanswered(id, context))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountErrorKind, ContactId, EventId, ProtocolErrorKind, ResourceKind};

    #[test]
    fn explicit_absence_and_unanswered_reads_have_distinct_scoped_errors() {
        for (operation, scope, resource) in [
            (
                AccountOperation::ContactGet,
                ErrorScope::Contact {
                    id: ContactId("wanted".into()),
                },
                ResourceKind::Contact,
            ),
            (
                AccountOperation::EventGet,
                ErrorScope::Event {
                    id: EventId("wanted".into()),
                },
                ResourceKind::Event,
            ),
            (
                AccountOperation::EventUpdate,
                ErrorScope::Event {
                    id: EventId("wanted".into()),
                },
                ResourceKind::Event,
            ),
            (
                AccountOperation::EventRsvp,
                ErrorScope::Event {
                    id: EventId("wanted".into()),
                },
                ResourceKind::Event,
            ),
        ] {
            for items in [vec![], vec!["unrelated"]] {
                for not_found in [false, true] {
                    let error = require_one(
                        items.clone(),
                        |item| *item == "wanted",
                        not_found,
                        "wanted",
                        operation,
                        scope.clone(),
                    )
                    .expect_err("no matching object");
                    let expected = if not_found {
                        AccountErrorKind::NotFound(resource)
                    } else {
                        AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse)
                    };
                    assert_eq!(error.kind(), &expected);
                    assert_eq!(error.scope(), Some(&scope));
                    assert_eq!(error.operation(), Some(operation));
                }
            }
        }
    }

    #[test]
    fn a_single_read_selects_the_requested_identity_instead_of_the_head() {
        let item = require_one(
            vec!["unrelated", "wanted"],
            |item| *item == "wanted",
            false,
            "wanted",
            AccountOperation::ContactGet,
            ErrorScope::Contact {
                id: ContactId("wanted".into()),
            },
        )
        .expect("requested object");
        assert_eq!(item, "wanted");
    }
}
