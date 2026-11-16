use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{ResponseCode, StatusKind, TaggedResponse, UntaggedResponse};

use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for NOTIFY SET (RFC 5465 Section3).
///
/// NOTIFY SET has no solicited untagged data of its own, but the
/// implicit NOOP effect means the server may flush STATUS/LIST/METADATA
/// before the tagged OK. All untagged responses are reclassified as
/// events. The consumer detects NOTIFICATIONOVERFLOW in both untagged
/// responses and the tagged response code.
#[derive(Default)]
pub(crate) struct NotifySetConsumer {
    /// Whether NOTIFICATIONOVERFLOW was seen in untagged responses.
    saw_overflow: bool,
}

impl Consumer for NotifySetConsumer {
    /// `true` when NOTIFICATIONOVERFLOW was detected (RFC 5465 Section5.8).
    /// Implicit NOOP observations are declined before completion, including NO/BAD.
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) -> Option<UntaggedResponse> {
        // RFC 5465 Section5.8: detect NOTIFICATIONOVERFLOW in untagged
        // responses (status with the overflow response code).
        if matches!(
            &resp,
            UntaggedResponse::Status {
                code: Some(ResponseCode::NotificationOverflow(_)),
                ..
            }
        ) {
            self.saw_overflow = true;
        }
        // All responses from the implicit NOOP are unsolicited.
        Some(resp)
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<bool> {
        match tagged.status {
            StatusKind::Ok => {
                // RFC 5465 Section5.8: NOTIFICATIONOVERFLOW can also appear in
                // the tagged response code.
                let overflow = self.saw_overflow
                    || matches!(tagged.code, Some(ResponseCode::NotificationOverflow(_)));
                Finalized::success(overflow, Vec::new())
            }
            StatusKind::No => {
                Finalized::failure(Error::no_with_code(tagged.text, tagged.code), Vec::new())
            }
            StatusKind::Bad => {
                Finalized::failure(Error::bad_with_code(tagged.text, tagged.code), Vec::new())
            }
        }
    }
}
