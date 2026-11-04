//! Mail PIM surface: message writes, send, drafts, search, containers,
//! identities, thread doors and typed hydration.
//!
//! Split by topic; `common` holds only the helpers two or more of these
//! modules share.

mod common;
mod containers;
mod drafts;
mod hydrate;
mod identities;
mod messages;
mod search;
mod send;
mod threads;

#[cfg(test)]
mod tests;

pub(crate) use containers::{
    container_create, container_delete, container_move, container_rename, containers_list,
    well_known_folder_roles,
};
pub(crate) use drafts::{draft_create, draft_discard, draft_send, draft_update};
pub(crate) use hydrate::{ews_read_folder, message_hydrate};
pub(crate) use identities::{identities_list, vacation_get, vacation_set};
pub(crate) use messages::{
    add_to_container, message_batch_url, set_category, set_extended_property, set_importance,
    set_is_read,
};
pub(crate) use search::{search, search_messages};
pub(crate) use send::{cancel_scheduled_send, reschedule_send, send_message, send_raw_message};
pub(crate) use threads::{delete_thread, move_thread, thread_hydrate};
