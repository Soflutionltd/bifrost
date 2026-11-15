//! Membership projection through remembered cursor topology.

use super::CursorRegistry;
use bifrost_types::{CursorScope, MembershipScope};

/// Map a `MembershipScope` from `ScopeLifecycle` to newly-required
/// cursor scopes using the account's cursor model: the shapes its
/// discovery advertised and its registry has held since the last
/// topology replacement, not only the cursors live at this instant.
///
/// Account-wide and type-wide cursor models already cover a newly
/// created membership, so they produce no scope. Per-folder models
/// preserve their existing shape, including every `FolderType` object
/// type the model uses. An empty or unrecognized topology also
/// produces no scope rather than inventing one the protocol never
/// advertised.
#[must_use]
pub fn membership_to_cursor_scopes(
    cursors: &CursorRegistry,
    membership: &MembershipScope,
) -> Vec<CursorScope> {
    let model = cursors.model_shapes();
    if model.is_wide() {
        return Vec::new();
    }
    model.scopes_for(membership)
}

/// Map a deleted `MembershipScope` to every per-membership cursor scope a
/// lifecycle creation could have raised for it during this attachment,
/// under the current model or an earlier one a reattach replaced. Unlike
/// the creation mapping, a wide shape does not suppress the per-folder
/// ones: an over-reaching deletion retires nothing live and purges rows of
/// a folder the provider deleted, while a missed one lets a queued or
/// parked establishment create a cursor nothing retires. Account-wide and
/// type-wide cursors are never produced; they cover other memberships.
pub(crate) fn membership_to_deleted_scopes(
    cursors: &CursorRegistry,
    membership: &MembershipScope,
) -> Vec<CursorScope> {
    cursors.reach_shapes().scopes_for(membership)
}
