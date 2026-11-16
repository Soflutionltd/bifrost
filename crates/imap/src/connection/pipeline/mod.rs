//! Pipeline builder for batching multiple pipelinable IMAP commands.
//!
//! Only commands that are safe to pipeline per RFC 3501 Section5.5 have
//! methods on [`Pipeline`]. State-changing commands (SELECT, STARTTLS,
//! IDLE, LOGOUT, etc.) intentionally lack pipeline methods.
//!
//! Audit boundary: not line-audited by the 2026-09-04 bug hunt, whose depth
//! was spent on the driver, framing, pool, push, auth, change strategies,
//! mutations, inventory, hydration and cursor envelope. An absence of
//! findings in this module is an absence of reading. Noted so a later
//! auditor knows where coverage stops.

// Retained pending the separate decision on pipelined account transport.
#![allow(dead_code)]

use crate::types::{
    Flag, MailboxAttribute, MailboxName, NotifySetParams, SequenceSet, StoreOperation,
};

// ============================================================================
// Pipeline builder with typed handle tuple
// ============================================================================

use std::any::Any;
use std::marker::PhantomData;

use crate::error::Error;
use crate::types::response::{
    AclEntry, EsearchResponse, ListRightsResponse, MetadataResult, NamespaceResponse,
    QuotaResource, QuotaRootResponse, ThreadNode, UidRange,
};
use crate::types::{
    Capability, Command, CopyResult, ExpungeResult, FetchResponse, MoveResult, StatusResult,
    StoreResult,
};

use super::MailboxInfo;
use super::SearchResult;
#[cfg(test)]
use super::dispatch;
use super::driver::ConsumerErased;
use super::request::{self, Request};

mod error;
mod unfold;

pub(crate) use error::PipelineError;
pub(crate) use unfold::UnfoldTuple;

// ---------------------------------------------------------------------------
// Pipeline builder
// ---------------------------------------------------------------------------

/// Typed pipeline builder for batching multiple pipelinable IMAP commands.
///
/// Built via [`ImapConnection::pipeline`]. Commands are added via
/// type-specific methods (e.g., [`fetch`](Self::fetch),
/// [`noop`](Self::noop)). Each method returns a new `Pipeline` with the
/// command's output type prepended to the `Accumulated` type-level list.
///
/// The accumulated type grows as a nested tuple:
/// `()` -> `(A, ())` -> `(B, (A, ()))` -> etc.
///
/// Call [`execute`](Self::execute) to submit the batch and receive a
/// flat, typed tuple of per-command results, or
/// [`execute_dynamic`](Self::execute_dynamic) for a `Vec<Result<...>>`.
///
/// # Example (illustrative  -  no wire activity)
///
/// ```text
/// let pipeline = conn.pipeline()
///     .noop()
///     .capability();
/// // pipeline: Pipeline<'_, (Vec<Capability>, ((), ()))>
/// // execute() would return (Result<(), Error>, Result<Vec<Capability>, Error>)
/// ```
// pub: returned by ImapConnection::pipeline for direct pipelined command batches.
pub struct Pipeline<'conn, Accumulated> {
    conn: &'conn super::ImapConnection,
    pending: Vec<Box<dyn ConsumerErased>>,
    commands: Vec<Command>,
    _marker: PhantomData<Accumulated>,
}

impl<'conn> Pipeline<'conn, ()> {
    /// Create a new empty pipeline for the given connection.
    pub(in crate::connection) fn new(conn: &'conn super::ImapConnection) -> Self {
        Self {
            conn,
            pending: Vec::new(),
            commands: Vec::new(),
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline command methods
// ---------------------------------------------------------------------------

/// Helper macro to generate pipeline command methods.
///
/// Each invocation adds a public method on `Pipeline<'conn, T>` that
/// pushes a `Command` variant and its consumer, returning a new pipeline
/// with the command's output type prepended to the accumulator.
macro_rules! pipeline_method {
    (
        $(#[$meta:meta])*
        fn $name:ident($($param:ident : $param_ty:ty),* $(,)?) -> $output:ty;
        request = $request:expr;
    ) => {
        $(#[$meta])*
        pub fn $name(self $(, $param: $param_ty)*) -> Pipeline<'conn, ($output, T)> {
            self.push($request)
        }
    };
}

impl<'conn, T> Pipeline<'conn, T> {
    fn push<R: Request>(
        mut self,
        request: R,
    ) -> Pipeline<'conn, (<R::Consumer as super::dispatch::Consumer>::Output, T)> {
        let (command, consumer) = request.into_parts();
        self.commands.push(command);
        self.pending.push(Box::new(consumer));
        Pipeline {
            conn: self.conn,
            pending: self.pending,
            commands: self.commands,
            _marker: PhantomData,
        }
    }

    // =======================================================================
    // Any-state commands (RFC 3501 Section6.1)
    // =======================================================================

    pipeline_method! {
        /// NOOP command (RFC 3501 Section6.1.2).
        fn noop() -> ();
        request = request::Noop {};
    }

    pipeline_method! {
        /// CAPABILITY command (RFC 3501 Section6.1.1).
        fn capability() -> Vec<Capability>;
        request = request::Capability {};
    }

    // =======================================================================
    // Authenticated-state commands (RFC 3501 Section6.3)
    // =======================================================================

    pipeline_method! {
        /// LIST command (RFC 3501 Section6.3.8).
        fn list(reference: String, pattern: String) -> Vec<MailboxInfo>;
        request = request::List { reference: reference.into(), pattern: pattern.into() };
    }

    /// LIST with selection/return options (RFC 5258 Section3, RFC 9051 Section6.3.9).
    ///
    /// The consumer's NOTIFY marker filter is derived from the
    /// `selection_options`: when `SUBSCRIBED` is absent, `\NonExistent`
    /// / `\NoAccess` responses are treated as NOTIFY markers and
    /// reclassified as events.
    pub fn list_extended(
        self,
        selection_options: Vec<crate::types::command_options::ListSelect>,
        reference: String,
        patterns: Vec<String>,
        return_options: Vec<crate::types::command_options::ListReturn>,
    ) -> Pipeline<'conn, (Vec<MailboxInfo>, T)> {
        self.push(request::ListExtended {
            selection_options,
            reference: reference.into(),
            patterns: patterns.into_iter().map(Into::into).collect(),
            return_options,
        })
    }

    pipeline_method! {
        /// LIST with STATUS return option (RFC 5819 Section2).
        fn list_status(
            reference: String,
            pattern: String,
            status_items: impl Into<crate::types::command_options::StatusItems>,
        ) -> Vec<crate::types::ListStatusEntry>;
        request = request::ListStatus { reference: reference.into(), pattern: pattern.into(), status_items: status_items.into() };
    }

    pipeline_method! {
        /// LSUB command (RFC 3501 Section6.3.9).
        fn lsub(reference: String, pattern: String) -> Vec<MailboxInfo>;
        request = request::Lsub { reference: reference.into(), pattern: pattern.into() };
    }

    pipeline_method! {
        /// CREATE command (RFC 3501 Section6.3.3).
        fn create(mailbox: MailboxName) -> Option<String>;
        request = request::Create { mailbox };
    }

    pipeline_method! {
        /// CREATE with USE special-use attributes (RFC 6154 Section3).
        fn create_special_use(
            mailbox: MailboxName,
            special_use: Vec<MailboxAttribute>,
        ) -> Option<String>;
        request = request::CreateSpecialUse { mailbox, special_use };
    }

    pipeline_method! {
        /// DELETE command (RFC 3501 Section6.3.4).
        fn delete(mailbox: MailboxName) -> ();
        request = request::Delete { mailbox };
    }

    pipeline_method! {
        /// RENAME command (RFC 3501 Section6.3.5).
        fn rename(mailbox: MailboxName, new_name: MailboxName) -> ();
        request = request::Rename { mailbox, new_name };
    }

    pipeline_method! {
        /// SUBSCRIBE command (RFC 3501 Section6.3.6).
        fn subscribe(mailbox: MailboxName) -> ();
        request = request::Subscribe { mailbox };
    }

    pipeline_method! {
        /// UNSUBSCRIBE command (RFC 3501 Section6.3.7).
        fn unsubscribe(mailbox: MailboxName) -> ();
        request = request::Unsubscribe { mailbox };
    }

    pipeline_method! {
        /// STATUS command (RFC 3501 Section6.3.10).
        fn status(mailbox: MailboxName, items: impl Into<crate::types::command_options::StatusItems>) -> StatusResult;
        request = request::Status { mailbox, items: items.into() };
    }

    pipeline_method! {
        /// NAMESPACE command (RFC 2342).
        fn namespace() -> NamespaceResponse;
        request = request::Namespace {};
    }

    // =======================================================================
    // Selected-state commands (RFC 3501 Section6.4)
    // =======================================================================

    pipeline_method! {
        /// CHECK command (RFC 3501 Section6.4.1).
        fn check() -> ();
        request = request::Check {};
    }

    pipeline_method! {
        /// EXPUNGE command (RFC 3501 Section6.4.3).
        ///
        /// Pipelining EXPUNGE before sequence-number-based commands
        /// creates ambiguity because EXPUNGE renumbers messages
        /// (RFC 3501 Section5.5). Callers must ensure no such ambiguity
        /// exists in their pipeline.
        fn expunge() -> ExpungeResult;
        request = request::Expunge {};
    }

    pipeline_method! {
        /// SEARCH command (RFC 3501 Section6.4.4).
        fn search(criteria: impl Into<crate::types::SearchCriteria>) -> SearchResult<crate::types::Seq>;
        request = request::Search { criteria: criteria.into() };
    }

    pipeline_method! {
        /// SEARCH RETURN command (RFC 4731 Section3.2).
        fn search_return(
            criteria: impl Into<crate::types::SearchCriteria>,
            return_opts: Vec<crate::types::command_options::SearchReturn>,
        ) -> EsearchResponse;
        request = request::SearchReturn { criteria: criteria.into(), return_opts };
    }

    pipeline_method! {
        /// SEARCH RETURN (SAVE) command (RFC 5182 Section2).
        fn search_save(criteria: impl Into<crate::types::SearchCriteria>) -> ();
        request = request::SearchSave { criteria: criteria.into() };
    }

    pipeline_method! {
        /// FETCH command (RFC 3501 Section6.4.5).
        fn fetch(
            sequence_set: SequenceSet<crate::types::Seq>,
            items: impl Into<crate::types::fetch::FetchItems>,
            changed_since: Option<crate::types::ModSeq>,
        ) -> Vec<FetchResponse>;
        request = request::Fetch { sequence_set, items: items.into(), changed_since };
    }

    pipeline_method! {
        /// STORE command (RFC 3501 Section6.4.6).
        fn store(
            sequence_set: SequenceSet<crate::types::Seq>,
            operation: StoreOperation,
            flags: Vec<Flag>,
            unchanged_since: Option<crate::types::ModSeqValzer>,
        ) -> StoreResult;
        request = request::Store { sequence_set, operation, flags, unchanged_since };
    }

    pipeline_method! {
        /// COPY command (RFC 3501 Section6.4.7).
        fn copy(sequence_set: SequenceSet<crate::types::Seq>, mailbox: MailboxName) -> CopyResult;
        request = request::Copy { sequence_set, mailbox };
    }

    pipeline_method! {
        /// MOVE command (RFC 6851 Section3).
        fn move_messages(sequence_set: SequenceSet<crate::types::Seq>, mailbox: MailboxName) -> MoveResult;
        request = request::Move { sequence_set, mailbox };
    }

    // =======================================================================
    // UID variants (RFC 3501 Section6.4.8)
    // =======================================================================

    pipeline_method! {
        /// UID SEARCH command (RFC 3501 Section6.4.4).
        fn uid_search(criteria: impl Into<crate::types::SearchCriteria>) -> SearchResult;
        request = request::UidSearch { criteria: criteria.into() };
    }

    pipeline_method! {
        /// UID SEARCH RETURN command (RFC 4731 Section3.2).
        fn uid_search_return(
            criteria: impl Into<crate::types::SearchCriteria>,
            return_opts: Vec<crate::types::command_options::SearchReturn>,
        ) -> EsearchResponse;
        request = request::UidSearchReturn { criteria: criteria.into(), return_opts };
    }

    pipeline_method! {
        /// UID SEARCH RETURN (SAVE) command (RFC 5182 Section2).
        fn uid_search_save(criteria: impl Into<crate::types::SearchCriteria>) -> ();
        request = request::UidSearchSave { criteria: criteria.into() };
    }

    /// UID FETCH command (RFC 3501 Section6.4.5, RFC 7162 Section3.2.6).
    ///
    /// Uses the shared VANISHED-aware request
    /// regardless of the `vanished` flag so that VANISHED responses
    /// are always captured when the server sends them.
    ///
    /// Parses the `sequence_set` into a [`UidSet`] for defensive
    /// filtering of `VANISHED (EARLIER)` UIDs (RFC 7162 Section 3.2.6).
    #[allow(clippy::type_complexity)]
    pub fn uid_fetch(
        self,
        sequence_set: SequenceSet,
        items: impl Into<crate::types::fetch::FetchItems>,
        changed_since: Option<crate::types::ModSeq>,
        vanished: bool,
    ) -> Pipeline<'conn, ((Vec<FetchResponse>, Vec<UidRange>), T)> {
        self.push(request::UidFetchWithVanished {
            sequence_set,
            items: items.into(),
            changed_since,
            vanished,
        })
    }

    pipeline_method! {
        /// UID STORE command (RFC 3501 Section6.4.6).
        fn uid_store(
            sequence_set: SequenceSet,
            operation: StoreOperation,
            flags: Vec<Flag>,
            unchanged_since: Option<crate::types::ModSeqValzer>,
        ) -> StoreResult;
        request = request::UidStore { sequence_set, operation, flags, unchanged_since };
    }

    pipeline_method! {
        /// UID COPY command (RFC 3501 Section6.4.7).
        fn uid_copy(sequence_set: SequenceSet, mailbox: MailboxName) -> CopyResult;
        request = request::UidCopy { sequence_set, mailbox };
    }

    pipeline_method! {
        /// UID MOVE command (RFC 6851 Section3).
        fn uid_move_messages(sequence_set: SequenceSet, mailbox: MailboxName) -> MoveResult;
        request = request::UidMove { sequence_set, mailbox };
    }

    pipeline_method! {
        /// UID EXPUNGE command (RFC 4315).
        fn uid_expunge(sequence_set: SequenceSet) -> ExpungeResult;
        request = request::UidExpunge { sequence_set };
    }

    // =======================================================================
    // Extension commands
    // =======================================================================

    pipeline_method! {
        /// ID command (RFC 2971 Section3.1).
        fn id(params: Vec<(String, Option<String>)>) -> Vec<(String, Option<String>)>;
        request = request::Id { params };
    }

    /// GETMETADATA command (RFC 5464 Section4.2).
    pub fn get_metadata(
        self,
        mailbox: MailboxName,
        entries: Vec<String>,
        max_size: Option<u64>,
        depth: Option<String>,
    ) -> Pipeline<'conn, (MetadataResult, T)> {
        self.push(request::GetMetadata {
            mailbox,
            entries,
            max_size,
            depth,
        })
    }

    pipeline_method! {
        /// SETMETADATA command (RFC 5464 Section4.3).
        fn set_metadata(
            mailbox: MailboxName,
            entries: Vec<(String, Option<Vec<u8>>)>,
        ) -> ();
        request = request::SetMetadata { mailbox, entries };
    }

    pipeline_method! {
        /// THREAD command (RFC 5256 Section3).
        fn thread(algorithm: String, charset: String, criteria: impl Into<crate::types::SearchCriteria>) -> Vec<ThreadNode<crate::types::Seq>>;
        request = request::Thread { algorithm, charset, criteria: criteria.into() };
    }

    pipeline_method! {
        /// UID THREAD command (RFC 5256 Section3).
        fn uid_thread(algorithm: String, charset: String, criteria: impl Into<crate::types::SearchCriteria>) -> Vec<ThreadNode>;
        request = request::UidThread { algorithm, charset, criteria: criteria.into() };
    }

    pipeline_method! {
        /// SORT command (RFC 5256 Section2).
        fn sort(sort_criteria: String, charset: String, criteria: impl Into<crate::types::SearchCriteria>) -> SearchResult<crate::types::Seq>;
        request = request::Sort { sort_criteria, charset, criteria: criteria.into() };
    }

    pipeline_method! {
        /// UID SORT command (RFC 5256 Section2).
        fn uid_sort(sort_criteria: String, charset: String, criteria: impl Into<crate::types::SearchCriteria>) -> SearchResult;
        request = request::UidSort { sort_criteria, charset, criteria: criteria.into() };
    }

    pipeline_method! {
        /// NOTIFY SET command (RFC 5465 Section3).
        fn notify_set(params: NotifySetParams) -> bool;
        request = request::NotifySet { params };
    }

    pipeline_method! {
        /// NOTIFY NONE command (RFC 5465 Section3).
        fn notify_none() -> ();
        request = request::NotifyNone {};
    }

    /// GETQUOTA command (RFC 2087 Section4.2).
    pub fn get_quota(self, root: String) -> Pipeline<'conn, (Vec<QuotaResource>, T)> {
        self.push(request::GetQuota { root })
    }

    /// GETQUOTAROOT command (RFC 2087 Section4.3).
    pub fn get_quota_root(self, mailbox: MailboxName) -> Pipeline<'conn, (QuotaRootResponse, T)> {
        self.push(request::GetQuotaRoot { mailbox })
    }

    /// SETQUOTA command (RFC 2087 Section4.1).
    ///
    /// RFC 2087 Section4.1: the server MUST respond with QUOTA and QUOTAROOT
    /// untagged responses. The consumer captures the QUOTA response for
    /// the requested root.
    pub fn set_quota(
        self,
        root: String,
        resources: Vec<(String, u64)>,
    ) -> Pipeline<'conn, (Vec<QuotaResource>, T)> {
        self.push(request::SetQuota { root, resources })
    }

    pipeline_method! {
        /// SETACL command (RFC 4314 Section3.1).
        fn set_acl(mailbox: MailboxName, identifier: String, rights: String) -> ();
        request = request::SetAcl { mailbox, identifier, rights };
    }

    pipeline_method! {
        /// DELETEACL command (RFC 4314 Section3.2).
        fn delete_acl(mailbox: MailboxName, identifier: String) -> ();
        request = request::DeleteAcl { mailbox, identifier };
    }

    /// GETACL command (RFC 4314 Section3.3).
    pub fn get_acl(self, mailbox: MailboxName) -> Pipeline<'conn, (Vec<AclEntry>, T)> {
        self.push(request::GetAcl { mailbox })
    }

    /// LISTRIGHTS command (RFC 4314 Section3.4).
    pub fn list_rights(
        self,
        mailbox: MailboxName,
        identifier: String,
    ) -> Pipeline<'conn, (ListRightsResponse, T)> {
        self.push(request::ListRights {
            mailbox,
            identifier,
        })
    }

    /// MYRIGHTS command (RFC 4314 Section3.5).
    pub fn my_rights(self, mailbox: MailboxName) -> Pipeline<'conn, (String, T)> {
        self.push(request::MyRights { mailbox })
    }
}

// ---------------------------------------------------------------------------
// Pipeline execution
// ---------------------------------------------------------------------------

impl<Accumulated: UnfoldTuple> Pipeline<'_, Accumulated> {
    /// Submit the accumulated commands as a single batch and await the
    /// typed result tuple. The timeout covers admission and execution.
    ///
    /// Each element in the returned tuple is `Result<T, Error>` where
    /// `T` is the command's consumer output type. Per-command errors
    /// (e.g., a server `NO` response) are captured per-element, not
    /// as a pipeline-level error.
    ///
    /// So is a failure that ends the batch once it is on the wire (a lost
    /// connection, a BYE, a desynchronized response): commands the server
    /// already answered keep their results, and every other element carries
    /// the failure, stamped `InFlight` when the command may have reached the
    /// server and `Unsent` when no byte of it was written. The connection is
    /// retired.
    ///
    /// # Errors
    ///
    /// - [`PipelineError::Disconnected`]  -  the driver task has exited.
    /// - [`PipelineError::Timeout`]  -  the admission/execution deadline elapsed;
    ///   transmission evidence says whether the batch may have been sent.
    /// - [`PipelineError::Driver`]  -  the batch was refused before any
    ///   byte was written (a command the session does not permit, an
    ///   encoding refusal); nothing ran.
    /// - [`PipelineError::TypeMismatch`]  -  internal bug in the
    ///   command-to-consumer mapping.
    pub async fn execute(
        self,
        timeout: std::time::Duration,
    ) -> Result<Accumulated::Output, PipelineError> {
        let results = self.execute_raw(timeout).await?;
        Accumulated::unfold(results)
    }
}

impl<T> Pipeline<'_, T> {
    /// Submit the accumulated commands and return the raw per-command
    /// results without typed unfolding.
    ///
    /// Each entry in the returned `Vec` corresponds to a command in
    /// push order. Callers must downcast `Box<dyn Any + Send>` to the
    /// expected consumer output type.
    ///
    /// The timeout covers admission and execution.
    /// Prefer [`execute`](Pipeline::execute) for type-safe access.
    pub async fn execute_dynamic(
        self,
        timeout: std::time::Duration,
    ) -> Result<Vec<Result<Box<dyn Any + Send>, Error>>, PipelineError> {
        self.execute_raw(timeout).await
    }

    /// Internal: send the pipeline to the driver and await the results.
    ///
    /// Admission happens HERE, at execution, not as commands are added: a
    /// pipeline is built before it runs, and only the state at execution says
    /// whether its commands may be sent. A command the session does not permit
    /// is caller sequencing (`InvalidState`), refused before anything is
    /// submitted; the driver checks again at the head of its queue.
    ///
    /// Admission shares the execution deadline: a channel slot is reserved before
    /// anything is submitted, so dropping this future while it waits for
    /// capacity submits nothing and leaves the connection alone. Dropping it
    /// after submission but before the driver commits the batch withdraws it
    /// (never written, connection reusable); dropping it after the commit
    /// without an answer marks the connection abandoned.
    async fn execute_raw(
        self,
        timeout: std::time::Duration,
    ) -> Result<Vec<Result<Box<dyn Any + Send>, Error>>, PipelineError> {
        let admitted = self
            .conn
            .admit_commands(&self.commands)
            .map_err(PipelineError::Driver)?;
        // One deadline covers queue admission and the wire exchange. A user
        // waiting on a batch expects the same bounded wait as a single command.
        let deadline = tokio::time::Instant::now() + timeout;
        self.conn
            .submit_driver_command(
                admitted,
                (self.commands, self.pending),
                Some(deadline),
                |reply| super::driver::DriverCommand::Pipeline { reply },
            )
            .await
            .map_err(|error| match error {
                Error::Transport {
                    kind: crate::error::TransportFailure::DriverGone,
                    ..
                }
                | Error::Internal {
                    kind: crate::error::InternalFailure::Panicked(_),
                    ..
                } => PipelineError::Disconnected(error),
                Error::Transport {
                    kind: crate::error::TransportFailure::Timeout,
                    ..
                } => PipelineError::Timeout(error),
                _ => PipelineError::Driver(error),
            })
    }
}

// ---------------------------------------------------------------------------
// ImapConnection::pipeline()
// ---------------------------------------------------------------------------

impl super::ImapConnection {
    /// Begin building a pipelined command batch (RFC 3501 Section5.5).
    ///
    /// Returns a [`Pipeline`] builder. Chain command methods to
    /// accumulate commands, then call [`Pipeline::execute`] to submit
    /// them all in one batch.
    ///
    /// Only commands that are safe to pipeline have methods on
    /// `Pipeline`. State-changing commands (SELECT, STARTTLS, IDLE,
    /// LOGOUT, AUTHENTICATE, APPEND, etc.) are intentionally absent; the
    /// driver also refuses, before writing a byte, any batch holding a
    /// command whose `CommandKind::pipelinable` is false.
    pub fn pipeline(&self) -> Pipeline<'_, ()> {
        Pipeline::new(self)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
