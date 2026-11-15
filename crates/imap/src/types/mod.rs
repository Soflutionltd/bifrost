//! Public IMAP types.
//!
//! Core types from RFC 3501 (`IMAP4rev1`) and RFC 9051 (`IMAP4rev2`), plus
//! RFC 5322 (Internet Message Format), RFC 2045/2046 (MIME),
//! RFC 2183 (Content-Disposition), and extension RFCs.
//!
//! All types are fully owned  -  no lifetime parameters. Parsers produce these from `&[u8]`
//! and callers can store them freely.

mod acl;
mod address;
mod auth;
pub(crate) mod body;
mod capability;
mod command;
pub(crate) mod envelope;
pub(crate) mod fetch;
pub(crate) mod flag;
mod ids;
pub(crate) mod mailbox;
pub(crate) mod notify;
pub(crate) mod profile;
pub(crate) mod request;
pub(crate) mod response;
pub(crate) mod search;
mod secret;
mod sync;
mod uid_range;
pub(crate) mod validated;

pub(crate) use acl::MailboxRights;
pub(crate) use address::Address;
pub(crate) use auth::{AuthMechanism, AuthOutcome, CredentialsKind};
pub use auth::{AuthPolicy, Credentials};
pub(crate) use body::BodyStructure;
pub(crate) use envelope::{Envelope, EnvelopeAddress};
pub(crate) use fetch::format_fetch_attrs;
pub(crate) use fetch::{AppendMessage, FetchAttr, FetchResponse, StoreOperation, StoreResult};
pub(crate) use flag::Flag;
pub(crate) use ids::{ModSeq, ModSeqValzer, SeqSet, Uid, UidSet, UidValidity};
pub(crate) use mailbox::{
    MailboxAttribute, MailboxInfo, SelectedMailbox, StatusItem, StatusResult,
};
pub(crate) use notify::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};
pub(crate) use profile::ServerProfile;
pub(crate) use response::{
    AclEntry, Capability, CopyResult, EsearchResponse, ExpungeResult, ListRightsResponse,
    ListStatusEntry, MetadataResult, MoveResult, NamespaceResponse, QuotaResource,
    QuotaRootResponse, Response, ResponseCode, SeqBound, ThreadNode, UidRange, UntaggedResponse,
};
#[cfg(test)]
pub(crate) use response::{MetadataEntry, UntaggedStatus};
pub(crate) use search::SearchCriteria;
pub(crate) use secret::SecretString;
pub(crate) use sync::{QresyncOutcome, SyncFetchResult, SyncSelectResult};
pub(crate) use validated::{
    MailboxArgument, MailboxInput, MailboxName, SequenceSet, ValidationError,
};

// Re-export command types for internal use only.
pub(crate) use command::{Command, CommandKind};

pub(crate) use request::{
    FetchChanges, QresyncParams, SelectAccess, SelectOptions, SyncFetchRequest,
};
