//! Public IMAP polling surface: a typed view of IMAP message ids, folder
//! observation (UIDVALIDITY and UIDNEXT), generation-bound UID enumeration
//! and a raw read with typed outcomes.
//!
//! This is the one published surface beside the factory and its config
//! types. It exists for consumers that track arrivals per folder by UID
//! instead of running the sync engine's change streams: a message moved
//! into a folder with an old INTERNALDATE has a fresh UID, so a UID
//! watermark sees it where a date search does not.
//!
//! Identity: `(container, UIDVALIDITY, UID)` names a message only within
//! one account on one server. Persisted state must be keyed by the
//! consumer's account identity as well. A rename is a container identity
//! change; UIDVALIDITY alone cannot establish continuity across one.

use std::num::{NonZeroU32, NonZeroUsize};

use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountOperation, Cause, ContainerId,
    DiagnosticText, ErrorScope, ObjectId, Protocol, RequestCause, SyncEvent,
};
use bytes::Bytes;

use crate::error::Error;
use crate::types::{MailboxName, SelectedMailbox};

use super::blob::{BlobError, RawFetchOutcome};
use super::error::ImapErrorContext;
use super::{ImapAccount, PooledConn, decode_object_id, encode_object_id, folder_scope};

/// Width of one `UID SEARCH UID lo:hi` window. A window can match at most
/// this many UIDs, so no single search approaches the connection layer's
/// 1,000,000-UID expansion cap however dense the folder is.
const SCAN_WINDOW: u32 = 65_536;

/// Windows one `list_uids` call may search before it returns what it has.
/// Bounds the work of one call over a sparse range; the continuation
/// carries the rest.
const MAX_WINDOWS_PER_CALL: usize = 16;

/// Client-side ceiling on one `fetch_raw` read, the same budget the
/// `Account::open_raw_rfc822` lane carries.
const RAW_FETCH_BUDGET: usize = 256 * 1024 * 1024;

/// A string that is not an IMAP message id this crate can decode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct InvalidImapRef {
    /// The rejected value.
    pub value: String,
}

impl std::fmt::Display for InvalidImapRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "not an IMAP message id or folder identity: {:?}",
            self.value
        )
    }
}

impl std::error::Error for InvalidImapRef {}

/// Typed view of an IMAP message `ObjectId`: folder, UIDVALIDITY and UID.
///
/// Parsing normalises every spelling the account accepts (legacy `imap1`
/// ids, leading-zero numerals) to one value, so two ids naming the same
/// message compare equal here even when their strings differ. The container
/// keeps the folder's exact wire identity: two folders that display alike
/// but differ on the wire stay distinct. [`ImapMessageRef::object_id`]
/// mints the canonical spelling the account itself mints today. The
/// `Account` methods still answer under the exact string submitted to them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImapMessageRef {
    container: ContainerId,
    uid_validity: NonZeroU32,
    uid: NonZeroU32,
}

impl ImapMessageRef {
    /// Parse an IMAP message id.
    ///
    /// # Errors
    ///
    /// [`InvalidImapRef`] when `id` is not an IMAP message id: another
    /// provider's id, a thread id, or a zero UIDVALIDITY or UID.
    pub fn parse(id: &ObjectId) -> Result<Self, InvalidImapRef> {
        let decoded =
            decode_object_id(id, AccountOperation::OpenRawRfc822).map_err(|_| InvalidImapRef {
                value: id.0.clone(),
            })?;
        Ok(Self::from_parts(
            &decoded.folder,
            nonzero(decoded.uidvalidity),
            nonzero(decoded.uid),
        ))
    }

    /// Build a ref from a container id (as `containers_list` hands it out)
    /// and a UID within one UIDVALIDITY.
    ///
    /// # Errors
    ///
    /// [`InvalidImapRef`] when `container` is not an IMAP folder identity.
    pub fn new(
        container: &ContainerId,
        uid_validity: NonZeroU32,
        uid: NonZeroU32,
    ) -> Result<Self, InvalidImapRef> {
        let folder = MailboxName::from_account_id(&container.0).map_err(|_| InvalidImapRef {
            value: container.0.clone(),
        })?;
        Ok(Self::from_parts(&folder, uid_validity, uid))
    }

    fn from_parts(folder: &MailboxName, uid_validity: NonZeroU32, uid: NonZeroU32) -> Self {
        Self {
            container: ContainerId(folder.account_id()),
            uid_validity,
            uid,
        }
    }

    /// The folder, in the canonical spelling `containers_list` uses.
    #[must_use]
    pub fn container(&self) -> &ContainerId {
        &self.container
    }

    /// The UIDVALIDITY generation the UID belongs to.
    #[must_use]
    pub fn uid_validity(&self) -> NonZeroU32 {
        self.uid_validity
    }

    /// The UID within that generation.
    #[must_use]
    pub fn uid(&self) -> NonZeroU32 {
        self.uid
    }

    /// The canonical `ObjectId` for this message.
    #[must_use]
    pub fn object_id(&self) -> ObjectId {
        encode_object_id(&self.folder(), self.uid_validity.get(), self.uid.get())
    }

    fn folder(&self) -> MailboxName {
        MailboxName::from_account_id(&self.container.0)
            .expect("a ref's container was validated when the ref was built")
    }
}

/// UIDVALIDITY and UIDNEXT of one folder, as reported by one EXAMINE.
///
/// These are values obtained during that selection, not a snapshot shared
/// with any later command. UIDNEXT is a boundary, not a promise: every
/// message that arrives after the observation has a UID at or above it, but
/// UIDs below it may be gaps, and UIDNEXT may advance past messages that
/// were expunged before anyone saw them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FolderObservation {
    /// The observed folder, canonical spelling.
    pub container: ContainerId,
    /// The folder's UIDVALIDITY generation.
    pub uid_validity: NonZeroU32,
    /// The folder's UIDNEXT. Valid for an empty folder.
    pub uid_next: NonZeroU32,
    /// The EXISTS count the selection reported.
    pub exists: u32,
}

/// One `list_uids` request: enumerate UIDs from `from` up to an optional
/// inclusive `through`, in the generation `uid_validity`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UidScan {
    /// The generation the caller's position belongs to.
    pub uid_validity: NonZeroU32,
    /// First UID to enumerate.
    pub from: NonZeroU32,
    /// Last UID to enumerate, inclusive. `None` means up to the UIDNEXT each
    /// call observes; a caller that needs a finite pass over a busy folder
    /// freezes it instead, since an open bound can keep moving.
    pub through: Option<NonZeroU32>,
    /// Most refs one call returns.
    pub limit: NonZeroUsize,
}

impl UidScan {
    /// A scan from `from` with an open upper bound.
    #[must_use]
    pub fn new(uid_validity: NonZeroU32, from: NonZeroU32, limit: NonZeroUsize) -> Self {
        Self {
            uid_validity,
            from,
            through: None,
            limit,
        }
    }

    /// Bound the scan at `through`, inclusive.
    #[must_use]
    pub fn through(mut self, through: NonZeroU32) -> Self {
        self.through = Some(through);
        self
    }
}

/// One call's enumeration result.
///
/// The invariant: `refs` are exactly the UIDs the server reported in
/// `from..=completed_through` for the observed generation, ascending and
/// without duplicates. `completed_through` covers gaps; it says nothing about
/// whether the caller has processed the refs, so a watermark advances past it
/// only once every ref at or below it is durably handled.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UidPage {
    /// The selection this listing ran under.
    pub observation: FolderObservation,
    /// Messages found, ascending.
    pub refs: Vec<ImapMessageRef>,
    /// Highest UID through which enumeration is complete. `None` when the
    /// interval was empty (`from` above the bound): nothing was scanned, and
    /// this is not a watermark assignment.
    pub completed_through: Option<NonZeroU32>,
    /// Where the next call resumes, with the same bound, when this call
    /// stopped short of it (the limit or the per-call window budget).
    /// `None` when the bound was reached.
    pub next_from: Option<NonZeroU32>,
}

/// Outcome of [`ImapPoller::list_uids`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum UidListing {
    /// The folder is still in the requested generation.
    Listed(UidPage),
    /// The folder's UIDVALIDITY is not the requested one: every UID the
    /// caller holds for it is void. Nothing was listed.
    GenerationChanged {
        /// The selection that observed the new generation.
        observation: FolderObservation,
    },
}

/// Outcome of [`ImapPoller::fetch_raw`]. Transient and other failures are the
/// `Err` arm, classified like every account error.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RawMessage {
    /// The whole message (`BODY[]`), possibly zero octets.
    Message(Bytes),
    /// The folder is in the ref's generation and the server holds no message
    /// at that UID: it was expunged.
    Absent,
    /// The folder's UIDVALIDITY is no longer the ref's; the UID names nothing.
    GenerationChanged {
        /// The UIDVALIDITY the selection reported.
        observed: NonZeroU32,
    },
}

/// Polling handle over one opened IMAP account, from
/// [`ImapAccountFactory::open_with_poller`](super::ImapAccountFactory::open_with_poller).
///
/// It shares that account instance's connection pool and folder registry.
/// Closing the `Account` closes the poller too: every later call fails
/// with the closed-account error, and a call in flight while the account
/// closes may fail. A later factory open builds a new instance which an
/// earlier poller never follows; take a new poller with it. Dropping the
/// `OpenedAccount` alone does not close the account while a poller holds it.
///
/// Every call selects the folder read-only (EXAMINE). Errors carry the
/// operation `SearchMessages` for observation and listing and
/// `OpenRawRfc822` for reads. A folder the server marks `UIDNOTSTICKY`
/// (RFC 4315: a new UIDVALIDITY on every selection) cannot be tracked by
/// UID and is refused as `Unsupported`, scoped to the folder, rather than
/// reported as an endless generation change.
#[derive(Clone)]
pub struct ImapPoller {
    account: ImapAccount,
}

impl std::fmt::Debug for ImapPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImapPoller").finish_non_exhaustive()
    }
}

impl ImapPoller {
    pub(crate) fn new(account: ImapAccount) -> Self {
        Self { account }
    }

    /// Observe a folder's UIDVALIDITY and UIDNEXT with one EXAMINE.
    ///
    /// # Errors
    ///
    /// A classified `AccountError`: `Request(Malformed)` for a container id
    /// that is not an IMAP folder, `Protocol` when the server omits
    /// UIDVALIDITY or UIDNEXT (or reports either as 0), `Unsupported` for a
    /// `UIDNOTSTICKY` folder, and the usual transport and server classes.
    pub async fn observe(
        &self,
        container: &ContainerId,
    ) -> Result<FolderObservation, AccountError> {
        let op = AccountOperation::SearchMessages;
        let folder = folder_of(container, op)?;
        let (_conn, selected) = examine(&self.account, &folder, op).await?;
        observation(&selected, &folder, op)
    }

    /// Enumerate the UIDs of one folder generation in `scan.from..=bound`,
    /// where the bound is `scan.through` clamped to the observed UIDNEXT
    /// minus one. No `*` is ever sent, so the `n:*` rule (a range past the
    /// highest UID still matches the highest UID) cannot surface a UID below
    /// `from`.
    ///
    /// # Errors
    ///
    /// As [`ImapPoller::observe`], plus the classified failure of a SEARCH.
    pub async fn list_uids(
        &self,
        container: &ContainerId,
        scan: UidScan,
    ) -> Result<UidListing, AccountError> {
        let op = AccountOperation::SearchMessages;
        let folder = folder_of(container, op)?;
        let (conn, selected) = examine(&self.account, &folder, op).await?;
        let observation = observation(&selected, &folder, op)?;
        if observation.uid_validity != scan.uid_validity {
            return Ok(UidListing::GenerationChanged { observation });
        }
        let ceiling = observation.uid_next.get() - 1;
        let bound = scan
            .through
            .map_or(ceiling, |through| through.get().min(ceiling));
        let from = scan.from.get();
        if from > bound {
            return Ok(UidListing::Listed(UidPage {
                observation,
                refs: Vec::new(),
                completed_through: None,
                next_from: None,
            }));
        }
        let limit = scan.limit.get();
        let mut uids: Vec<u32> = Vec::new();
        let mut lo = from;
        let mut completed = from;
        for _ in 0..MAX_WINDOWS_PER_CALL {
            let hi = lo.saturating_add(SCAN_WINDOW - 1).min(bound);
            let mut found = conn
                .connection()
                .uid_search(format!("UID {lo}:{hi}"), self.account.command_timeout())
                .await
                .map_err(|err| {
                    self.account
                        .folder_error(err, &folder, ImapErrorContext::operation(op))
                })?
                .ids;
            found.retain(|uid| (lo..=hi).contains(uid));
            found.sort_unstable();
            found.dedup();
            let room = limit - uids.len();
            if found.len() > room {
                // `room` is nonzero here: a full page breaks out below
                // before the next window is searched.
                found.truncate(room);
                if let Some(&last) = found.last() {
                    completed = last;
                }
                uids.extend(found);
                break;
            }
            uids.extend(found);
            completed = hi;
            if uids.len() == limit || hi == bound {
                break;
            }
            lo = hi + 1;
        }
        let next_from = if completed < bound {
            NonZeroU32::new(completed + 1)
        } else {
            None
        };
        let refs = uids
            .into_iter()
            .map(|uid| ImapMessageRef::from_parts(&folder, observation.uid_validity, nonzero(uid)))
            .collect();
        Ok(UidListing::Listed(UidPage {
            observation,
            refs,
            completed_through: NonZeroU32::new(completed),
            next_from,
        }))
    }

    /// Read one message's whole RFC822 octets, distinguishing a delivered
    /// message, an expunged UID and a voided generation.
    ///
    /// # Errors
    ///
    /// A classified `AccountError` for every other failure, including a
    /// server that answers the UID without its body, and a message past the
    /// 256 MiB read budget. Bytes received before a failed completion are
    /// discarded.
    pub async fn fetch_raw(&self, message: &ImapMessageRef) -> Result<RawMessage, AccountError> {
        let op = AccountOperation::OpenRawRfc822;
        let folder = message.folder();
        let (tx, mut rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
        // Owns the sender so it drops as soon as the read finishes, which is
        // what ends the collector below.
        let read = async move {
            super::blob::run_fetch(
                &self.account,
                &folder,
                message.uid_validity.get(),
                message.uid.get(),
                op,
                RAW_FETCH_BUDGET,
                super::blob::UidPersistence::Sticky,
                &tx,
            )
            .await
        };
        let collect = async {
            let mut body = Vec::new();
            while let Some(event) = rx.recv().await {
                if let SyncEvent::Batch(batch) = event {
                    body.extend(batch.items);
                }
            }
            body
        };
        let (outcome, body) = tokio::join!(read, collect);
        let folder = message.folder();
        match outcome {
            Ok(RawFetchOutcome::Delivered) => {
                let mut body = body.into_iter();
                Ok(RawMessage::Message(body.next().unwrap_or_default()))
            }
            Ok(RawFetchOutcome::Absent) => Ok(RawMessage::Absent),
            Ok(RawFetchOutcome::NotSticky) => Err(not_sticky(&folder, op)),
            Ok(RawFetchOutcome::GenerationChanged { observed }) => {
                match NonZeroU32::new(observed) {
                    Some(observed) => Ok(RawMessage::GenerationChanged { observed }),
                    None => Err(folder_error(
                        Error::ProtocolMissing("SELECT reported UIDVALIDITY 0".into()),
                        &folder,
                        op,
                    )),
                }
            }
            Err(BlobError::Account(err)) => Err(err),
            Err(BlobError::Imap(err)) => Err(self.account.folder_error(
                err,
                &folder,
                ImapErrorContext::operation(op).with_message_id(message.object_id().0),
            )),
            Err(BlobError::ChannelDropped) => {
                unreachable!("the collector holds the receiver until the read finishes")
            }
        }
    }
}

fn nonzero(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).expect("the id decoder rejects zero UIDVALIDITY and UID")
}

fn folder_of(container: &ContainerId, op: AccountOperation) -> Result<MailboxName, AccountError> {
    MailboxName::from_account_id(&container.0).map_err(|err| {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only(format!("not an IMAP folder identity: {err}")),
            }),
        )
        .protocol(Protocol::Imap)
        .operation(op)
        .try_build()
        .expect("valid account error classification")
    })
}

fn folder_error(err: Error, folder: &MailboxName, op: AccountOperation) -> AccountError {
    super::account_error_with(
        err,
        ImapErrorContext::operation(op).with_folder_scope(folder),
    )
}

/// EXAMINE `folder` on a pooled connection, mapping a permission denial on a
/// shared folder to that folder's `ScopeRevoked` as the raw read does.
async fn examine(
    account: &ImapAccount,
    folder: &MailboxName,
    op: AccountOperation,
) -> Result<(PooledConn, SelectedMailbox), AccountError> {
    if account.shutdown.is_cancelled() {
        return Err(AccountError::account_closed(
            op,
            bifrost_types::Protocol::Imap,
        ));
    }
    let mut conn = account
        .checkout_for_folder(folder)
        .await
        .map_err(|err| folder_error(err, folder, op))?;
    let shared_owner = account
        .folders
        .get(folder)
        .and_then(|entry| entry.shared_owner.clone());
    match account
        .select_folder(&mut conn, folder, super::SelectPurpose::Read)
        .await
    {
        Ok(selected) => Ok((conn, selected.mailbox)),
        Err(err) => Err(super::error::shared_folder_error(
            err,
            folder,
            shared_owner.as_ref(),
            ImapErrorContext::operation(op).with_folder_scope(folder),
        )),
    }
}

/// `Unsupported`, scoped to the folder: a `UIDNOTSTICKY` folder's UIDs do
/// not persist across selections, so it cannot be tracked by UID.
fn not_sticky(folder: &MailboxName, op: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .protocol(Protocol::Imap)
    .operation(op)
    .scope(ErrorScope::Cursor(folder_scope(folder)))
    .text(DiagnosticText::support_only(
        "folder is UIDNOTSTICKY: its UIDs do not persist across selections",
    ))
    .try_build()
    .expect("valid account error classification")
}

fn observation(
    selected: &SelectedMailbox,
    folder: &MailboxName,
    op: AccountOperation,
) -> Result<FolderObservation, AccountError> {
    if selected.uid_not_sticky {
        return Err(not_sticky(folder, op));
    }
    let uid_validity =
        super::selected_uidvalidity(selected).map_err(|error| folder_error(error, folder, op))?;
    let uid_validity = nonzero(uid_validity);
    let uid_next = selected.uid_next.and_then(NonZeroU32::new).ok_or_else(|| {
        folder_error(
            Error::ProtocolMissing("SELECT missing or zero UIDNEXT".into()),
            folder,
            op,
        )
    })?;
    Ok(FolderObservation {
        container: ContainerId(folder.account_id()),
        uid_validity,
        uid_next,
        exists: selected.exists,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).unwrap()
    }

    #[test]
    fn a_ref_normalises_every_accepted_spelling() {
        let canonical = ImapMessageRef::parse(&ObjectId("imap2:5:INBOX:7:9".into())).unwrap();
        assert_eq!(canonical.container(), &ContainerId("INBOX".into()));
        assert_eq!((canonical.uid_validity(), canonical.uid()), (nz(7), nz(9)));
        for spelling in ["imap1:5:INBOX:7:9", "imap2:5:INBOX:007:09"] {
            assert_eq!(
                ImapMessageRef::parse(&ObjectId(spelling.into())).unwrap(),
                canonical,
                "{spelling}"
            );
        }
        assert_eq!(canonical.object_id(), ObjectId("imap2:5:INBOX:7:9".into()));
        assert_eq!(
            ImapMessageRef::new(&ContainerId("INBOX".into()), nz(7), nz(9)).unwrap(),
            canonical
        );
    }

    #[test]
    fn a_ref_round_trips_a_noncanonical_folder_identity() {
        // Two folders that display as "a" but differ on the wire stay two
        // containers through parse and re-mint.
        let folder = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let id = encode_object_id(&folder, 3, 4);
        let parsed = ImapMessageRef::parse(&id).unwrap();
        assert_eq!(parsed.container(), &ContainerId(folder.account_id()));
        assert_ne!(parsed.container(), &ContainerId("a".into()));
        assert_eq!(parsed.object_id(), id);
        assert_eq!(
            ImapMessageRef::new(parsed.container(), nz(3), nz(4)).unwrap(),
            parsed
        );
    }

    #[test]
    fn a_ref_refuses_what_is_not_an_imap_message_id() {
        for value in [
            "imap2:5:INBOX:0:9",
            "imap2:5:INBOX:7:0",
            "imapthread2:5:INBOX:7:9,10",
            "gmail:abc",
            "",
        ] {
            assert!(
                ImapMessageRef::parse(&ObjectId(value.into())).is_err(),
                "{value}"
            );
        }
    }
}
