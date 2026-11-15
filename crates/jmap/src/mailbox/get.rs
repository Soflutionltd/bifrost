use super::ACL;
use super::{Mailbox, MailboxId, MailboxRights, Role};
use std::collections::HashMap;

impl Mailbox {
    pub(crate) fn id(&self) -> Option<&MailboxId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> MailboxId {
        self.id.take().unwrap_or_else(|| MailboxId::new(""))
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn parent_id(&self) -> Option<&MailboxId> {
        self.parent_id.as_ref()
    }

    pub(crate) fn role(&self) -> Option<&Role> {
        self.role.as_ref()
    }

    pub(crate) fn sort_order(&self) -> Option<u32> {
        self.sort_order
    }

    pub(crate) fn total_emails(&self) -> Option<usize> {
        self.total_emails
    }

    pub(crate) fn unread_emails(&self) -> Option<usize> {
        self.unread_emails
    }

    pub(crate) fn total_threads(&self) -> Option<usize> {
        self.total_threads
    }

    pub(crate) fn unread_threads(&self) -> Option<usize> {
        self.unread_threads
    }

    pub(crate) fn is_subscribed(&self) -> Option<bool> {
        self.is_subscribed
    }

    pub(crate) fn my_rights(&self) -> Option<&MailboxRights> {
        self.my_rights.as_ref()
    }

    pub(crate) fn acl(&self) -> Option<&HashMap<String, HashMap<ACL, bool>>> {
        self.share_with.as_ref()
    }

    pub(crate) fn take_acl(&mut self) -> Option<HashMap<String, HashMap<ACL, bool>>> {
        self.share_with.take()
    }
}
