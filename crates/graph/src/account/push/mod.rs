//! Push subscription surface: the mode-dispatching `push_subscribe` /
//! `push_unsubscribe` doors, the Graph `/subscriptions` webhook arm with its
//! renewal sessions, and the EWS streaming arm's id translation and
//! registration.
//!
//! Split by arm; `common` holds only what both arms and the dispatcher share.

mod common;
mod dispatch;
mod ews;
pub(crate) mod renewal;
mod webhook;

#[cfg(test)]
mod tests;

pub(crate) use common::PushEndpoint;
pub(crate) use dispatch::{push_subscribe, push_unsubscribe};
pub(crate) use ews::{EwsSubscriptionScope, EwsSubscriptionState};
/// Only `account`'s own tests build a subscription row by hand; production
/// code outside `push` reaches every one of them through the group.
#[cfg(test)]
pub(crate) use webhook::GraphSubscriptionState;
pub(crate) use webhook::{
    GraphSubscriptionGroup, RecreatedSubscriptionIds, retire_all_graph_subscriptions,
};
