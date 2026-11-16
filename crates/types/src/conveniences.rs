//! Shared product conveniences over provider-owned Account primitives.
//!
//! A blanket extension keeps these helpers callable on concrete accounts and
//! dyn Account alike without putting product dispatch on the provider contract.
//! Values and primitive selection come from the account's declaration.

use crate::{
    Account, AccountError, AccountFuture, AccountOperation, CalendarEvent, ContactCard,
    ContactSearchRequest, ContainerId, ContainerKind, EventSearchRequest, ForwardedFlagShape,
    Label, LabelShape, MutationTarget, ObjectId, RepliedFlagShape, StarredFlagShape, ThreadId,
};

/// Shared consumer helpers. Import this trait alongside Account for method syntax.
pub trait AccountConveniences: Account {
    /// Apply a label using the provider-declared shape for its wire kind.
    fn apply_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        dispatch_label(self, target, label, true)
    }

    /// Remove a label from `target`. Same dispatch shape as
    /// `apply_label`.
    fn remove_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        dispatch_label(self, target, label, false)
    }

    /// Alias for `set_is_read`. Exists for naming symmetry with the
    /// other convenience setters.
    fn set_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.set_is_read(target, is_read)
    }

    /// Toggle starred state using the provider-declared primitive and value.
    fn set_starred(
        &self,
        target: MutationTarget,
        starred: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        match self.capabilities().conveniences.starred {
            StarredFlagShape::Keyword(keyword) => self.set_keyword(target, keyword.into(), starred),
            StarredFlagShape::LabelMembership(label) => {
                self.set_label_membership(target, ContainerId(label.to_string()), starred)
            }
            StarredFlagShape::Category(category) => {
                self.set_category(target, category.to_string(), starred)
            }
            StarredFlagShape::None => {
                let error =
                    AccountError::unsupported(AccountOperation::SetStarred, self.protocol());
                Box::pin(async move { Err(error) })
            }
        }
    }

    /// Mark a message as replied using the advertised primitive. Gmail derives
    /// replied state during sync and advertises no writable shape.
    fn mark_replied(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let target = MutationTarget::Message(message);
        match self.capabilities().conveniences.replied {
            RepliedFlagShape::Keyword(keyword) => self.set_keyword(target, keyword.into(), true),
            RepliedFlagShape::ExtendedProperty { name, value } => {
                self.set_extended_property(target, name.to_string(), Some(value.to_string()))
            }
            RepliedFlagShape::None => {
                let error =
                    AccountError::unsupported(AccountOperation::MarkReplied, self.protocol());
                Box::pin(async move { Err(error) })
            }
        }
    }

    /// Mark a message as forwarded using the advertised primitive.
    fn mark_forwarded(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let target = MutationTarget::Message(message);
        match self.capabilities().conveniences.forwarded {
            ForwardedFlagShape::Keyword(keyword) => self.set_keyword(target, keyword.into(), true),
            ForwardedFlagShape::ExtendedProperty { name, value } => {
                self.set_extended_property(target, name.to_string(), Some(value.to_string()))
            }
            ForwardedFlagShape::None => {
                let error =
                    AccountError::unsupported(AccountOperation::MarkForwarded, self.protocol());
                Box::pin(async move { Err(error) })
            }
        }
    }

    /// Persist that an MDN was dispatched using the declared writable keyword.
    fn mark_mdn_sent(&self, message: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let target = MutationTarget::Message(message);
        if let Some(keyword) = self.capabilities().conveniences.mdn_sent_keyword {
            return self.set_keyword(target, keyword.into(), true);
        }

        let error = AccountError::unsupported(AccountOperation::MarkMdnSent, self.protocol());
        Box::pin(async move { Err(error) })
    }

    /// Move a thread using the provider's add-before-remove composite.
    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.move_thread_provider(thread, target, source)
    }

    /// Trash a thread, or permanently delete one already in Trash.
    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.delete_thread_provider(thread, current)
    }

    /// Return contact suggestions with the caller's limit.
    fn contact_autocomplete(
        &self,
        query: String,
        limit: u32,
    ) -> AccountFuture<Result<Vec<ContactCard>, AccountError>> {
        self.contact_autocomplete_provider(query, limit)
    }

    /// Return event suggestions with the caller's limit.
    fn event_autocomplete(
        &self,
        query: String,
        limit: u32,
    ) -> AccountFuture<Result<Vec<CalendarEvent>, AccountError>> {
        self.event_autocomplete_provider(query, limit)
    }
}

impl<T: Account + ?Sized> AccountConveniences for T {}

fn dispatch_label<T: Account + ?Sized>(
    receiver: &T,
    target: MutationTarget,
    label: Label,
    value: bool,
) -> AccountFuture<Result<(), AccountError>> {
    let shapes = receiver.capabilities().conveniences;
    let shape = match label.provenance.kind {
        ContainerKind::Label => shapes.label,
        ContainerKind::Folder => shapes.folder,
    };
    match shape {
        LabelShape::Keyword => receiver.set_keyword(target, label.provenance.native.into(), value),
        LabelShape::LabelMembership => receiver.set_label_membership(target, label.id, value),
        LabelShape::Category => receiver.set_category(target, label.provenance.native, value),
        LabelShape::ContainerMembership => {
            if value {
                receiver.add_to_container(target, label.id)
            } else {
                receiver.remove_from_container(target, label.id)
            }
        }
        LabelShape::None => {
            let error = AccountError::unsupported(
                AccountOperation::SetLabelMembership,
                receiver.protocol(),
            );
            Box::pin(async move { Err(error) })
        }
    }
}

pub(crate) fn default_contact_autocomplete<T: Account + ?Sized>(
    receiver: &T,
    query: String,
    limit: u32,
) -> AccountFuture<Result<Vec<ContactCard>, AccountError>> {
    let mut request = ContactSearchRequest::new(query);
    request.limit = Some(limit);
    let future = receiver.contact_search(request);
    Box::pin(async move { future.await.map(|page| page.items) })
}

pub(crate) fn default_event_autocomplete<T: Account + ?Sized>(
    receiver: &T,
    query: String,
    limit: u32,
) -> AccountFuture<Result<Vec<CalendarEvent>, AccountError>> {
    let mut request = EventSearchRequest::new(query);
    request.limit = Some(limit);
    let future = receiver.event_search(request);
    Box::pin(async move { future.await.map(|page| page.items) })
}

pub(crate) fn unsupported<T: Account + ?Sized>(
    receiver: &T,
    operation: AccountOperation,
) -> AccountFuture<Result<(), AccountError>> {
    let error = AccountError::unsupported(operation, receiver.protocol());
    Box::pin(async move { Err(error) })
}
