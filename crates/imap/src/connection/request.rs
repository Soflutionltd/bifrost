//! Typed command-consumer pairings shared by handles and pipelines.
//!
//! Admission and deadlines remain in the common submission path. Streaming and
//! SASL requests retain their caller-owned channels and negotiation state.

use super::dispatch;
use crate::types::{Command, Flag, MailboxAttribute, MailboxName, SecretString, SequenceSet};

pub(super) trait Request {
    type Consumer: dispatch::Consumer + 'static;
    fn into_parts(self) -> (Command, Self::Consumer);
}

macro_rules! request {
    ($name:ident { $($field:ident: $ty:ty),* $(,)? } => $command:expr, $consumer:ty, $make:expr) => {
        pub(super) struct $name { $(pub(super) $field: $ty),* }
        impl Request for $name {
            type Consumer = $consumer;
            fn into_parts(self) -> (Command, Self::Consumer) {
                let Self { $($field),* } = self;
                let consumer = $make;
                ($command, consumer)
            }
        }
    };
}

request! {
    Noop {  } =>
    Command::Noop,
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    Check {  } =>
    Command::Check,
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    Delete { mailbox: MailboxName } =>
    Command::Delete { mailbox },
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    Rename { mailbox: MailboxName, new_name: MailboxName } =>
    Command::Rename { mailbox, new_name },
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    Subscribe { mailbox: MailboxName } =>
    Command::Subscribe { mailbox },
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    Unsubscribe { mailbox: MailboxName } =>
    Command::Unsubscribe { mailbox },
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

#[cfg(test)]
request! {
    Close {  } =>
    Command::Close,
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    Unselect {  } =>
    Command::Unselect,
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    SetMetadata { mailbox: MailboxName, entries: Vec<(String, Option<Vec<u8>>)> } =>
    Command::SetMetadata { mailbox, entries },
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    SetAcl { mailbox: MailboxName, identifier: String, rights: String } =>
    Command::SetAcl { mailbox, identifier, rights },
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    DeleteAcl { mailbox: MailboxName, identifier: String } =>
    Command::DeleteAcl { mailbox, identifier },
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    NotifyNone {  } =>
    Command::NotifyNone,
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

#[cfg(test)]
request! {
    Unauthenticate {  } =>
    Command::Unauthenticate,
    dispatch::TaggedOkConsumer,
    dispatch::TaggedOkConsumer::default()
}

request! {
    Capability {  } =>
    Command::Capability,
    dispatch::CapabilityConsumer,
    dispatch::CapabilityConsumer::default()
}

request! {
    Create { mailbox: MailboxName } =>
    Command::Create { mailbox },
    dispatch::CreateConsumer,
    dispatch::CreateConsumer::default()
}

request! {
    Namespace {  } =>
    Command::Namespace,
    dispatch::NamespaceConsumer,
    dispatch::NamespaceConsumer::default()
}

request! {
    Id { params: Vec<(String, Option<String>)> } =>
    Command::Id(params),
    dispatch::IdConsumer,
    dispatch::IdConsumer::default()
}

request! {
    Enable { capabilities: Vec<crate::types::Capability> } =>
    Command::Enable { capabilities },
    dispatch::EnableConsumer,
    dispatch::EnableConsumer::default()
}

request! {
    Logout {  } =>
    Command::Logout,
    dispatch::LogoutConsumer,
    dispatch::LogoutConsumer::default()
}

request! {
    Login { user: String, pass: SecretString } =>
    Command::Login { user, pass },
    dispatch::LoginConsumer,
    dispatch::LoginConsumer::default()
}

request! {
    Lsub { reference: crate::types::MailboxArgument, pattern: crate::types::MailboxArgument } =>
    Command::Lsub { reference, pattern },
    dispatch::LsubConsumer,
    dispatch::LsubConsumer::default()
}

request! {
    NotifySet { params: crate::types::NotifySetParams } =>
    Command::NotifySet(params),
    dispatch::NotifySetConsumer,
    dispatch::NotifySetConsumer::default()
}

request! {
    CreateSpecialUse { mailbox: MailboxName, special_use: Vec<MailboxAttribute> } =>
    Command::CreateSpecialUse { mailbox, special_use },
    dispatch::CreateConsumer,
    dispatch::CreateConsumer::default()
}

request! {
    List { reference: crate::types::MailboxArgument, pattern: crate::types::MailboxArgument } =>
    Command::List { reference, pattern },
    dispatch::ListConsumer,
    dispatch::ListConsumer::new()
}

request! {
    ListStatus { reference: crate::types::MailboxArgument, pattern: crate::types::MailboxArgument, status_items: crate::types::command_options::StatusItems } =>
    Command::ListStatus { reference, pattern, status_items },
    dispatch::ListStatusConsumer,
    dispatch::ListStatusConsumer::new()
}

request! {
    Status { mailbox: MailboxName, items: crate::types::command_options::StatusItems } =>
    Command::Status { mailbox, items },
    dispatch::StatusConsumer,
    dispatch::StatusConsumer::new()
}

request! {
    Fetch { sequence_set: SequenceSet<crate::types::Seq>, items: crate::types::fetch::FetchItems, changed_since: Option<crate::types::ModSeq> } =>
    Command::Fetch { sequence_set, items, changed_since },
    dispatch::FetchConsumer,
    dispatch::FetchConsumer::new()
}

request! {
    Copy { sequence_set: SequenceSet<crate::types::Seq>, mailbox: MailboxName } =>
    Command::Copy { sequence_set, mailbox },
    dispatch::CopyConsumer,
    dispatch::CopyConsumer::new()
}

request! {
    Move { sequence_set: SequenceSet<crate::types::Seq>, mailbox: MailboxName } =>
    Command::Move { sequence_set, mailbox },
    dispatch::MoveConsumer,
    dispatch::MoveConsumer::new()
}

request! {
    Expunge {  } =>
    Command::Expunge,
    dispatch::ExpungeConsumer,
    dispatch::ExpungeConsumer::new()
}

request! {
    SearchSave { criteria: crate::types::SearchCriteria } =>
    Command::SearchSave { criteria },
    dispatch::SearchSaveConsumer,
    dispatch::SearchSaveConsumer::new()
}

request! {
    ListExtended { selection_options: Vec<crate::types::command_options::ListSelect>, reference: crate::types::MailboxArgument, patterns: Vec<crate::types::MailboxArgument>, return_options: Vec<crate::types::command_options::ListReturn> } =>
    Command::ListExtended { selection_options, reference, patterns, return_options },
    dispatch::ListExtendedConsumer,
    dispatch::ListExtendedConsumer::new(selection_options.clone())
}

request! {
    Store { sequence_set: SequenceSet<crate::types::Seq>, operation: crate::types::StoreOperation, flags: Vec<Flag>, unchanged_since: Option<crate::types::ModSeqValzer> } =>
    Command::Store { sequence_set, operation, flags, unchanged_since },
    dispatch::StoreConsumer,
    dispatch::StoreConsumer::new(unchanged_since)
}

request! {
    UidStore { sequence_set: SequenceSet, operation: crate::types::StoreOperation, flags: Vec<Flag>, unchanged_since: Option<crate::types::ModSeqValzer> } =>
    Command::UidStore { sequence_set, operation, flags, unchanged_since },
    dispatch::StoreConsumer,
    dispatch::StoreConsumer::new(unchanged_since)
}

request! {
    Search { criteria: crate::types::SearchCriteria } =>
    Command::Search { criteria },
    dispatch::SearchConsumer<crate::types::Seq>,
    dispatch::SearchConsumer::<crate::types::Seq>::new()
}

request! {
    UidSearch { criteria: crate::types::SearchCriteria } =>
    Command::UidSearch { criteria },
    dispatch::SearchConsumer<crate::types::Uid>,
    dispatch::SearchConsumer::<crate::types::Uid>::new()
}

request! {
    SearchReturn { criteria: crate::types::SearchCriteria, return_opts: Vec<crate::types::command_options::SearchReturn> } =>
    Command::SearchReturn { criteria, return_opts },
    dispatch::EsearchConsumer,
    dispatch::EsearchConsumer::new()
}

request! {
    UidSearchReturn { criteria: crate::types::SearchCriteria, return_opts: Vec<crate::types::command_options::SearchReturn> } =>
    Command::UidSearchReturn { criteria, return_opts },
    dispatch::EsearchConsumer,
    dispatch::EsearchConsumer::new()
}

request! {
    UidSearchSave { criteria: crate::types::SearchCriteria } =>
    Command::UidSearchSave { criteria },
    dispatch::SearchSaveConsumer,
    dispatch::SearchSaveConsumer::new()
}

request! {
    Thread { algorithm: String, charset: String, criteria: crate::types::SearchCriteria } =>
    Command::Thread { algorithm, charset, criteria },
    dispatch::ThreadConsumer<crate::types::Seq>,
    dispatch::ThreadConsumer::<crate::types::Seq>::default()
}

request! {
    UidThread { algorithm: String, charset: String, criteria: crate::types::SearchCriteria } =>
    Command::UidThread { algorithm, charset, criteria },
    dispatch::ThreadConsumer<crate::types::Uid>,
    dispatch::ThreadConsumer::<crate::types::Uid>::default()
}

request! {
    Sort { sort_criteria: String, charset: String, criteria: crate::types::SearchCriteria } =>
    Command::Sort { sort_criteria, charset, criteria },
    dispatch::SortConsumer<crate::types::Seq>,
    dispatch::SortConsumer::<crate::types::Seq>::default()
}

request! {
    UidSort { sort_criteria: String, charset: String, criteria: crate::types::SearchCriteria } =>
    Command::UidSort { sort_criteria, charset, criteria },
    dispatch::SortConsumer<crate::types::Uid>,
    dispatch::SortConsumer::<crate::types::Uid>::default()
}

request! {
    GetMetadata { mailbox: MailboxName, entries: Vec<String>, max_size: Option<u64>, depth: Option<String> } =>
    Command::GetMetadata { mailbox, entries, max_size, depth },
    dispatch::MetadataConsumer,
    dispatch::MetadataConsumer::new(mailbox.clone())
}

request! {
    GetQuota { root: String } =>
    Command::GetQuota { root },
    dispatch::QuotaConsumer,
    dispatch::QuotaConsumer::new(root.clone())
}

request! {
    SetQuota { root: String, resources: Vec<(String, u64)> } =>
    Command::SetQuota { root, resources },
    dispatch::QuotaConsumer,
    dispatch::QuotaConsumer::new(root.clone())
}

request! {
    GetQuotaRoot { mailbox: MailboxName } =>
    Command::GetQuotaRoot { mailbox },
    dispatch::QuotaRootConsumer,
    dispatch::QuotaRootConsumer::new(mailbox.clone())
}

request! {
    GetAcl { mailbox: MailboxName } =>
    Command::GetAcl { mailbox },
    dispatch::AclConsumer,
    dispatch::AclConsumer::new(mailbox.clone())
}

request! {
    MyRights { mailbox: MailboxName } =>
    Command::MyRights { mailbox },
    dispatch::MyRightsConsumer,
    dispatch::MyRightsConsumer::new(mailbox.clone())
}

request! {
    ListRights { mailbox: MailboxName, identifier: String } =>
    Command::ListRights { mailbox, identifier },
    dispatch::ListRightsConsumer,
    dispatch::ListRightsConsumer::new(mailbox.clone(), identifier.clone())
}

request! {
    UidCopy { sequence_set: SequenceSet, mailbox: MailboxName } =>
    Command::UidCopy { sequence_set, mailbox },
    dispatch::CopyConsumer,
    dispatch::CopyConsumer::new()
}

request! {
    UidMove { sequence_set: SequenceSet, mailbox: MailboxName } =>
    Command::UidMove { sequence_set, mailbox },
    dispatch::MoveConsumer,
    dispatch::MoveConsumer::new()
}

request! {
    UidExpunge { sequence_set: SequenceSet } =>
    Command::UidExpunge { sequence_set },
    dispatch::ExpungeConsumer,
    dispatch::ExpungeConsumer::new()
}

request! {
    UidFetch { sequence_set: SequenceSet, items: crate::types::fetch::FetchItems, changed_since: Option<crate::types::ModSeq> } =>
    Command::UidFetch { sequence_set, items, changed_since, vanished: false },
    dispatch::FetchConsumer,
    dispatch::FetchConsumer::new()
}

request! {
    UidFetchWithVanished { sequence_set: SequenceSet, items: crate::types::fetch::FetchItems, changed_since: Option<crate::types::ModSeq>, vanished: bool } =>
    Command::UidFetch { sequence_set, items, changed_since, vanished },
    dispatch::FetchVanishedConsumer,
    dispatch::FetchVanishedConsumer::new(crate::types::set::UidSet::from_expression(&sequence_set))
}

request! {
    AppendOne { mailbox: crate::types::MailboxArgument, messages: Vec<crate::types::AppendMessage> } =>
    Command::Append { mailbox, messages, multi: false },
    dispatch::AppendConsumer,
    dispatch::AppendConsumer::default()
}
#[cfg(test)]
request! {
    AppendMany { mailbox: crate::types::MailboxArgument, messages: Vec<crate::types::AppendMessage> } =>
    Command::Append { mailbox, messages, multi: true },
    dispatch::MultiAppendConsumer,
    dispatch::MultiAppendConsumer::new(messages.len())
}
request! {
    UidExpungeWithCode { sequence_set: SequenceSet } =>
    Command::UidExpunge { sequence_set },
    dispatch::ExpungeWithCodeConsumer,
    dispatch::ExpungeConsumer::with_tagged_code()
}

pub(super) struct Selection {
    pub(super) mailbox: MailboxName,
    pub(super) is_examine: bool,
    pub(super) options: crate::types::SelectOptions,
}

impl Request for Selection {
    type Consumer = dispatch::SelectConsumer;
    fn into_parts(self) -> (Command, Self::Consumer) {
        let Self {
            mailbox,
            is_examine,
            options,
        } = self;
        let command = if is_examine {
            Command::Examine {
                mailbox,
                condstore: options.condstore,
                qresync: options.qresync,
            }
        } else {
            Command::Select {
                mailbox,
                condstore: options.condstore,
                qresync: options.qresync,
            }
        };
        (command, dispatch::SelectConsumer::new(is_examine))
    }
}

request! {
    UidFetchLimited { sequence_set: SequenceSet, items: crate::types::fetch::FetchItems, max_estimated_bytes: usize } =>
    Command::UidFetch { sequence_set, items, changed_since: None, vanished: false },
    dispatch::FetchConsumer,
    dispatch::FetchConsumer::with_limit(max_estimated_bytes)
}
