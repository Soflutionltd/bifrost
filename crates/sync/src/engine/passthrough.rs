//! Read-only hydration and PIM passthrough onto the live account handle.
//!
//! On the ~900 lines of near-identical forwarders below, observed and
//! deliberately left alone: every one of the roughly 60 methods here is a 1:1
//! passthrough - `live_account(id)?` then forward - carrying an identical doc
//! comment shape and inventing no semantics of its own. A macro or a blanket
//! forwarding trait would collapse them. It was not done because this is a
//! PUBLISHED-SURFACE question rather than a structural one: these methods are
//! `SyncEngine`'s public API, and generating them changes how they read in
//! rustdoc, how their docs are written, and how easily a single method can
//! deviate later when one of them eventually does need to invent semantics.
//! The verbosity is the cost of keeping each published signature explicit and
//! independently documentable. Re-raise it as an API decision with the
//! repository owner, not as a refactor.

use super::*;

impl SyncEngine {
    // ---------- hydration passthrough ----------
    //
    // The change and inventory streams the engine broadcasts are
    // projection-only: a `Change` carries `{ id, kind }` and an
    // inventory entry carries a fingerprint, never message content. A
    // consumer that turns those signals into real rows therefore has to
    // fetch full content out-of-band. The `Account` handle that can do
    // so lives behind the slot's `ArcSwap` and is otherwise private, so
    // the engine exposes this read-only passthrough cluster as the
    // consumer's single door to hydration.
    //
    // Two deliberate properties:
    //
    // - Every method resolves the handle through `live_account`, i.e.
    //   `ArcSwap::load_full`, so a hydrate issued after a reopen runs
    //   against the freshly-installed connection, never a stale snapshot
    //   the consumer cached. This is the same discipline the spawned
    //   workers follow on their hot paths.
    // - Only the *read* surface is forwarded. Mutations stay funnelled
    //   through `bulk_set_flags` (and its siblings) so the idempotency /
    //   read-back / recovery pipeline remains the one chokepoint for
    //   writes; cursor and push driving stay engine-owned. Handing out a
    //   raw `Arc<dyn Account>` would leak both, so we do not.
    //
    // The forwarded methods return `'static` streams / futures that
    // capture their own internal `Arc` clones, so they outlive the
    // short-lived handle resolved per call.

    /// Announce that a forwarded page came back with a non-empty loss
    /// lane, on the account's normal warning channel.
    ///
    /// This surfaces nothing the caller does not already hold - both
    /// lanes ride out in the returned `Page`, and that copy stays the
    /// actionable one (it names the scopes and carries their classified
    /// errors). What it fixes is an asymmetry: an open-time skip is
    /// announced (`open_skipped_scopes` plus a log line) while a
    /// page-time skip was entirely silent, so a consumer had to know to
    /// look. A warning gives them a reason to.
    ///
    /// Deliberately an event rather than engine state. A page lane is
    /// true of ONE walk at ONE moment: there is no later point at which
    /// it can be said to have healed, so accumulating it would need an
    /// invented expiry, an invented dedupe key, and a cap. It would also
    /// be systematically incomplete - `search`, `search_messages`,
    /// `contacts_search`, and the calendar range walks are not exposed
    /// by the engine at all, so an accessor that looked authoritative
    /// would report "no skips" while a direct `Account` call had just
    /// quarantined three scopes. A warning stream carries no such claim.
    ///
    /// The message counts; it never names ids. `failed_ids` holds native
    /// provider identifiers, which are not user-safe text.
    fn announce_page_loss<T>(
        &self,
        account_id: &AccountId,
        method: &'static str,
        page: &bifrost_types::Page<T>,
    ) {
        if page.failed_ids.is_empty() && page.skipped_scopes.is_empty() {
            return;
        }
        let Some(slot) = self.accounts.get(account_id) else {
            return;
        };
        let warning = bifrost_types::Warning::user_safe(
            bifrost_types::WarningKind::OperatorAttentionNeeded,
            format!(
                "{method} returned an incomplete page: {} unsearched scope(s), \
                 {} resource(s) the provider could not return",
                page.skipped_scopes.len(),
                page.failed_ids.len(),
            ),
        )
        .with_next_action(bifrost_types::DiagnosticText::user_safe(
            "inspect Page::skipped_scopes and Page::failed_ids; results from a \
             skipped scope are missing, not absent",
        ));
        let event = MultiplexerEvent {
            scope: CursorScope::Account,
            event: Arc::new(SyncEvent::Warning(warning)),
            checkpoint: None,
            publication: None,
        };
        let _ = slot.multiplexer.changes_tx.send(event);
    }

    /// Resolve the live `Account` handle for an attached account.
    ///
    /// Loads through the slot's `ArcSwap` so the caller sees the handle
    /// installed by the most recent reopen. Errors with
    /// `AccountNotAttached` when no slot exists for `account_id`.
    fn live_account(&self, account_id: &AccountId) -> Result<Arc<Arc<dyn Account>>, Error> {
        let slot = self
            .accounts
            .get(account_id)
            .map(|r| Arc::clone(r.value()))
            .ok_or_else(|| Error::AccountNotAttached(account_id.clone()))?;
        Ok(slot.current.load_full())
    }

    /// Hydrate a stream of known ids at a chosen projection.
    ///
    /// Forwards to [`Account::get_stream`]. The input ids are streamed
    /// so a long fetch pass backpressures cleanly; per-item results flow
    /// through `ItemOutcome<HydratedObject>` (`Succeeded` / `Failed` /
    /// `Uncertain`) on the returned stream. This is the primary entry
    /// for turning a broadcast `Change` into real content.
    pub fn get_stream(
        &self,
        account_id: &AccountId,
        ids: AccountStream<bifrost_types::ObjectId>,
        projection: bifrost_types::Projection,
    ) -> Result<AccountStream<SyncEvent<ItemOutcome<bifrost_types::HydratedObject>>>, Error> {
        Ok(self.live_account(account_id)?.get_stream(ids, projection))
    }

    /// Enumerate every object the server currently holds in `scope`.
    ///
    /// Forwards to [`Account::inventory_stream`]. Unlike the engine's own
    /// backfill walk, nothing is published to subscribers and no cursor or
    /// checkpoint moves: this is the consumer's door to comparing the
    /// server's full membership against its own store. Only a `Done` whose
    /// coverage is complete proves the listing whole.
    pub fn inventory_stream(
        &self,
        account_id: &AccountId,
        scope: CursorScope,
    ) -> Result<AccountStream<bifrost_types::InventoryEvent>, Error> {
        Ok(self.live_account(account_id)?.inventory_stream(scope))
    }

    /// Hydrate a single message at a specific projection level.
    ///
    /// Forwards to [`Account::message_hydrate`]. Prefer this over a
    /// one-id `get_stream` when the consumer wants a parsed `Message`
    /// rather than the raw projection envelope. Protocol errors surface
    /// as `Error::Account`; an unattached account yields
    /// `AccountNotAttached`.
    pub async fn message_hydrate(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
        projection: bifrost_types::HydrationProjection,
    ) -> Result<bifrost_types::Message, Error> {
        Ok(self
            .live_account(account_id)?
            .message_hydrate(message, projection)
            .await?)
    }

    /// Hydrate every message in a thread.
    ///
    /// Forwards to [`Account::thread_hydrate`]. The protocol crate picks
    /// the threading primitive for its backend (JMAP `Thread/get`, IMAP
    /// `THREAD REFERENCES`, Gmail `threads.get`, Graph conversation API).
    pub async fn thread_hydrate(
        &self,
        account_id: &AccountId,
        thread: bifrost_types::ThreadId,
    ) -> Result<bifrost_types::ThreadHydration, Error> {
        Ok(self
            .live_account(account_id)?
            .thread_hydrate(thread)
            .await?)
    }

    /// Open a message's assembled RFC822 octets for streaming download.
    ///
    /// Forwards to [`Account::open_raw_rfc822`]. Yields the verbatim
    /// server-assembled MIME bytes (never re-encoded), gated by
    /// `capabilities().pim_methods.open_raw_rfc822`; an account whose
    /// flag is false terminates the stream with `Unsupported`.
    pub fn open_raw_rfc822(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<AccountStream<SyncEvent<bytes::Bytes>>, Error> {
        Ok(self.live_account(account_id)?.open_raw_rfc822(message))
    }

    /// Open a blob for streaming download.
    ///
    /// Forwards to [`Account::open_blob`]. Used to pull attachment or
    /// inline-part bytes referenced by a hydrated object's `blobs`.
    pub fn open_blob(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::BlobHandle,
    ) -> Result<AccountStream<SyncEvent<bytes::Bytes>>, Error> {
        Ok(self.live_account(account_id)?.open_blob(handle))
    }

    /// Open a byte range of a blob for streaming download.
    ///
    /// Forwards to [`Account::open_blob_range`]. Errors with
    /// `Unsupported(OpenBlobRange)` on the stream where the blob's
    /// capability flag is false.
    pub fn open_blob_range(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::BlobHandle,
        range: bifrost_types::ByteRange,
    ) -> Result<AccountStream<SyncEvent<bytes::Bytes>>, Error> {
        Ok(self
            .live_account(account_id)?
            .open_blob_range(handle, range))
    }

    /// Host an over-limit attachment in the account's cloud drive and
    /// return a shareable link, in one call.
    ///
    /// Forwards to [`Account::host_attachment`]. Gated by
    /// `capabilities().pim_methods.host_attachment`; an account whose
    /// flag is false resolves the future with
    /// `Unsupported(HostAttachment)`. The synchronous `live_account`
    /// resolution surfaces `AccountNotAttached` here, while the returned
    /// `AccountFuture` surfaces the trait's `AccountError` when awaited.
    pub fn host_attachment(
        &self,
        account_id: &AccountId,
        bytes: bytes::Bytes,
        meta: bifrost_types::CloudUploadMeta,
    ) -> Result<AccountFuture<Result<bifrost_types::HostedAttachment, AccountError>>, Error> {
        Ok(self.live_account(account_id)?.host_attachment(bytes, meta))
    }

    // ---------- mutation passthrough (direct) ----------
    //
    // The write-side companion to the read-only hydration cluster above:
    // the single-object conveniences, membership primitives, and
    // container CRUD a consumer needs to drive object-level mutations
    // against the live attached connection without holding the
    // engine-private `Arc<dyn Account>`. Like the read cluster, every
    // method resolves through `live_account` (so a mutation issued after
    // a reopen runs against the freshly-installed connection, never a
    // stale snapshot the consumer cached) and forwards to the matching
    // `Account` method 1:1, inventing no new semantics.
    //
    // These are DIRECT - one wire op each - and deliberately bypass the
    // idempotency / read-back / recovery pipeline. That pipeline guards
    // the *volume* mutations (`bulk_set_flags`, `bulk_move`,
    // `bulk_destroy`), where a partial-apply replay across a retry would
    // corrupt state and the read-back guard earns its keep. A
    // single-object convenience carries no batch idempotency key and is
    // cheap to reissue, so routing it through the pipeline would buy
    // nothing. An unattached account yields `Error::AccountNotAttached`
    // up front; the forwarded `Account` future is `'static` and captures
    // its own `Arc` clones, so it outlives the short-lived handle
    // resolved per call.

    /// Toggle the starred / flagged bit on `target`. Forwards to
    /// [`Account::set_starred`].
    pub async fn set_starred(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        starred: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_starred(target, starred)
            .await?)
    }

    /// Set or clear `target`'s read state. Forwards to
    /// [`Account::set_read`].
    pub async fn set_read(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        is_read: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_read(target, is_read)
            .await?)
    }

    /// Apply `label` to `target`. Forwards to [`Account::apply_label`],
    /// which dispatches by `label.provenance` to the right primitive.
    pub async fn apply_label(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        label: bifrost_types::Label,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .apply_label(target, label)
            .await?)
    }

    /// Remove `label` from `target`. Forwards to
    /// [`Account::remove_label`].
    pub async fn remove_label(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        label: bifrost_types::Label,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .remove_label(target, label)
            .await?)
    }

    /// Mark `message` as replied. Forwards to [`Account::mark_replied`].
    pub async fn mark_replied(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.mark_replied(message).await?)
    }

    /// Mark `message` as forwarded. Forwards to
    /// [`Account::mark_forwarded`].
    pub async fn mark_forwarded(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .mark_forwarded(message)
            .await?)
    }

    /// Persist that an MDN (read receipt) was dispatched for `message`.
    /// Forwards to [`Account::mark_mdn_sent`].
    pub async fn mark_mdn_sent(
        &self,
        account_id: &AccountId,
        message: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .mark_mdn_sent(message)
            .await?)
    }

    /// Move a thread between containers. Forwards to
    /// [`Account::move_thread`]; the protocol crate composes the
    /// add-then-remove pair against its own `Arc`-shaped handle.
    pub async fn move_thread(
        &self,
        account_id: &AccountId,
        thread: bifrost_types::ThreadId,
        target: bifrost_types::ContainerId,
        source: Option<bifrost_types::ContainerId>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .move_thread(thread, target, source)
            .await?)
    }

    /// Move a thread to Trash, or delete-permanently if already in Trash.
    /// Forwards to [`Account::delete_thread`].
    pub async fn delete_thread(
        &self,
        account_id: &AccountId,
        thread: bifrost_types::ThreadId,
        current: Option<bifrost_types::ContainerId>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .delete_thread(thread, current)
            .await?)
    }

    /// Add `target` to `container`. Forwards to
    /// [`Account::add_to_container`].
    pub async fn add_to_container(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        container: bifrost_types::ContainerId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .add_to_container(target, container)
            .await?)
    }

    /// Remove `target` from `container`. Forwards to
    /// [`Account::remove_from_container`]. Providers without a symmetric
    /// remove (Graph) surface `Unsupported(RemoveFromContainer)`.
    pub async fn remove_from_container(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        container: bifrost_types::ContainerId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .remove_from_container(target, container)
            .await?)
    }

    /// Set or clear `target`'s read state. The membership-primitive
    /// spelling; forwards to [`Account::set_is_read`].
    pub async fn set_is_read(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        is_read: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_is_read(target, is_read)
            .await?)
    }

    /// Set or clear a single IMAP / JMAP keyword on `target`. Forwards to
    /// [`Account::set_keyword`].
    pub async fn set_keyword(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        keyword: String,
        value: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_keyword(target, keyword, value)
            .await?)
    }

    /// Set or clear membership in a Gmail-style label. Forwards to
    /// [`Account::set_label_membership`].
    pub async fn set_label_membership(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        label: bifrost_types::ContainerId,
        value: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_label_membership(target, label, value)
            .await?)
    }

    /// Set or clear a Graph category on `target`. Forwards to
    /// [`Account::set_category`].
    pub async fn set_category(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        category: String,
        value: bool,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_category(target, category, value)
            .await?)
    }

    /// Set `target`'s importance to exactly `level`. Forwards to
    /// [`Account::set_importance`].
    pub async fn set_importance(
        &self,
        account_id: &AccountId,
        target: bifrost_types::MutationTarget,
        level: bifrost_types::Importance,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .set_importance(target, level)
            .await?)
    }

    /// Enumerate an account's containers (folders, labels, mailboxes).
    /// Forwards 1:1 to [`Account::containers_list`].
    ///
    /// The read companion to the container-mutation cluster below: like
    /// every passthrough it resolves through `live_account` so a list
    /// issued after a `RestartAccount` reopen runs against the freshly
    /// installed connection, and yields `Error::AccountNotAttached` up
    /// front when no live slot exists. Read-only, so it does not pass
    /// through the idempotency / read-back pipeline.
    pub async fn containers_list(
        &self,
        account_id: &AccountId,
    ) -> Result<bifrost_types::ContainerList, Error> {
        Ok(self.live_account(account_id)?.containers_list().await?)
    }

    /// Forwards to the account's provider category-definition surface.
    pub async fn category_definitions_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::CategoryDefinition>, Error> {
        Ok(self
            .live_account(account_id)?
            .category_definitions_list()
            .await?)
    }

    /// Forwards to the account's provider category create/recolor surface.
    pub async fn category_definition_upsert(
        &self,
        account_id: &AccountId,
        definition: bifrost_types::CategoryDefinition,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .category_definition_upsert(definition)
            .await?)
    }

    /// Forwards to the account's provider category delete surface.
    pub async fn category_definition_delete(
        &self,
        account_id: &AccountId,
        name: String,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .category_definition_delete(name)
            .await?)
    }

    /// Forwards to the account's provider reaction-read surface.
    pub async fn message_reactions(
        &self,
        account_id: &AccountId,
        ids: &[bifrost_types::ObjectId],
    ) -> Result<bifrost_types::BatchOutcome<bifrost_types::MessageReactionState>, Error> {
        Ok(self
            .live_account(account_id)?
            .message_reactions(ids)
            .await?)
    }

    /// Create a new container of `kind` named `name` under `parent`.
    /// Forwards to [`Account::container_create`]; returns the
    /// engine-facing id. `style` carries an optional initial color
    /// (Gmail labels only; ignored by folder-shaped protocols).
    pub async fn container_create(
        &self,
        account_id: &AccountId,
        kind: bifrost_types::ContainerKind,
        name: String,
        parent: Option<bifrost_types::ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> Result<bifrost_types::ContainerId, Error> {
        Ok(self
            .live_account(account_id)?
            .container_create(kind, name, parent, style)
            .await?)
    }

    /// Rename a container. Forwards to [`Account::container_rename`].
    /// `style`, when `Some`, also recolors the container (Gmail labels
    /// only; ignored by folder-shaped protocols).
    pub async fn container_rename(
        &self,
        account_id: &AccountId,
        container: bifrost_types::ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .container_rename(container, name, style)
            .await?)
    }

    /// Move a container under a new parent. Forwards to
    /// [`Account::container_move`].
    pub async fn container_move(
        &self,
        account_id: &AccountId,
        container: bifrost_types::ContainerId,
        new_parent: Option<bifrost_types::ContainerId>,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .container_move(container, new_parent)
            .await?)
    }

    /// Delete a container. Forwards to [`Account::container_delete`].
    pub async fn container_delete(
        &self,
        account_id: &AccountId,
        container: bifrost_types::ContainerId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .container_delete(container)
            .await?)
    }

    // ---------- compose passthrough (direct) ----------
    //
    // The send / draft / scheduled-send companion to the mutation
    // passthrough cluster above. Same discipline: every method resolves
    // through `live_account` (so a compose issued after a reopen runs
    // against the freshly-installed connection), forwards 1:1 to the
    // matching `Account` method, invents no new semantics, and bails
    // `AccountNotAttached` up front. The forwarded `Account` future is
    // `'static` and captures its own `Arc` clones, so it outlives the
    // short-lived handle resolved per call. Capability gating
    // (`scheduled_send`, `send_as`) stays in the protocol crate exactly
    // as the direct call would; a consumer reads `account_capabilities`
    // below to decide whether to dispatch before paying the round trip.

    /// Send an RFC 5322 message. Forwards to [`Account::send_message`].
    pub async fn send_message(
        &self,
        account_id: &AccountId,
        request: bifrost_types::SendRequest,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self.live_account(account_id)?.send_message(request).await?)
    }

    /// Send pre-assembled RFC 5322 / RFC 8098 octets verbatim. Forwards to
    /// [`Account::send_raw_message`]; the MDN submission lane.
    pub async fn send_raw_message(
        &self,
        account_id: &AccountId,
        raw: bytes::Bytes,
        save_to_sent: Option<bool>,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self
            .live_account(account_id)?
            .send_raw_message(raw, save_to_sent)
            .await?)
    }

    /// Create a new draft. Forwards to [`Account::draft_create`].
    pub async fn draft_create(
        &self,
        account_id: &AccountId,
        patch: bifrost_types::DraftPatch,
    ) -> Result<bifrost_types::DraftHandle, Error> {
        Ok(self.live_account(account_id)?.draft_create(patch).await?)
    }

    /// Update an existing draft. Forwards to [`Account::draft_update`].
    pub async fn draft_update(
        &self,
        account_id: &AccountId,
        draft: bifrost_types::DraftHandle,
        patch: bifrost_types::DraftPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .draft_update(draft, patch)
            .await?)
    }

    /// Discard (delete) a draft without sending. Forwards to
    /// [`Account::draft_discard`].
    pub async fn draft_discard(
        &self,
        account_id: &AccountId,
        draft: bifrost_types::DraftHandle,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.draft_discard(draft).await?)
    }

    /// Convert a draft into a sent message. Forwards to
    /// [`Account::draft_send`].
    pub async fn draft_send(
        &self,
        account_id: &AccountId,
        draft: bifrost_types::DraftHandle,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self.live_account(account_id)?.draft_send(draft).await?)
    }

    /// Cancel a previously scheduled send. Forwards to
    /// [`Account::cancel_scheduled_send`]; gated by
    /// `capabilities().pim_methods.scheduled_send` at the protocol layer.
    pub async fn cancel_scheduled_send(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::ObjectId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .cancel_scheduled_send(handle)
            .await?)
    }

    /// Reschedule a previously scheduled send to a new instant. Forwards
    /// to [`Account::reschedule_send`]; returns the (possibly new)
    /// submission id.
    pub async fn reschedule_send(
        &self,
        account_id: &AccountId,
        handle: bifrost_types::ObjectId,
        scheduled: std::time::SystemTime,
    ) -> Result<bifrost_types::ObjectId, Error> {
        Ok(self
            .live_account(account_id)?
            .reschedule_send(handle, scheduled)
            .await?)
    }

    // ---------- contact passthrough (direct) ----------
    //
    // The contact companion to the container / compose passthrough
    // clusters above. Same discipline: every method resolves through
    // `live_account` (so a call issued after a `RestartAccount` reopen
    // runs against the freshly-installed connection, never a stale
    // snapshot the consumer cached), forwards 1:1 to the matching
    // `Account` method, invents no new semantics, and yields
    // `Error::AccountNotAttached` up front when no live slot exists. The
    // forwarded `Account` future is `'static` and captures its own `Arc`
    // clones, so it outlives the short-lived handle resolved per call.
    // The address-book / contact reads are read-only, and the single
    // contact mutations are direct one-wire-op conveniences, so - like the
    // container / compose clusters - they deliberately bypass the
    // idempotency / read-back / recovery pipeline that guards the volume
    // mutations. Capability gating (`pim_methods.directory_search`, etc.)
    // stays in the protocol crate; a consumer reads `account_capabilities`
    // to decide whether to dispatch before paying the round trip.

    /// List an account's address books / contact folders. Forwards 1:1 to
    /// [`Account::address_books_list`].
    pub async fn address_books_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::AddressBook>, Error> {
        Ok(self.live_account(account_id)?.address_books_list().await?)
    }

    /// List contacts, optionally scoped to one address book, resuming from
    /// a prior `Page::next_cursor`. Forwards to [`Account::contacts_list`].
    pub async fn contacts_list(
        &self,
        account_id: &AccountId,
        address_book: Option<bifrost_types::AddressBookId>,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::ContactCard>, Error> {
        let page = self
            .live_account(account_id)?
            .contacts_list(address_book, page_cursor)
            .await?;
        self.announce_page_loss(account_id, "contacts_list", &page);
        Ok(page)
    }

    /// Fetch one contact card by engine-facing id. Forwards to
    /// [`Account::contact_get`].
    pub async fn contact_get(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactId,
    ) -> Result<bifrost_types::ContactCard, Error> {
        Ok(self.live_account(account_id)?.contact_get(contact).await?)
    }

    /// Create one contact card. Forwards to [`Account::contact_create`].
    pub async fn contact_create(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactCreate,
    ) -> Result<bifrost_types::ContactId, Error> {
        Ok(self
            .live_account(account_id)?
            .contact_create(contact)
            .await?)
    }

    /// Partially update one contact card. Forwards to
    /// [`Account::contact_update`].
    pub async fn contact_update(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactId,
        patch: bifrost_types::ContactPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .contact_update(contact, patch)
            .await?)
    }

    /// Delete one contact card. Forwards to [`Account::contact_delete`].
    pub async fn contact_delete(
        &self,
        account_id: &AccountId,
        contact: bifrost_types::ContactId,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .contact_delete(contact)
            .await?)
    }

    /// Search the organization directory (Global Address List). Forwards
    /// to [`Account::directory_search`]; gated by
    /// `capabilities().pim_methods.directory_search` at the protocol layer.
    /// The argument order mirrors the trait: `(query, limit, page_cursor)`.
    pub async fn directory_search(
        &self,
        account_id: &AccountId,
        query: String,
        limit: Option<u32>,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::DirectoryCard>, Error> {
        let page = self
            .live_account(account_id)?
            .directory_search(query, limit, page_cursor)
            .await?;
        self.announce_page_loss(account_id, "directory_search", &page);
        Ok(page)
    }

    /// List the mail-enabled organization-directory groups the account's
    /// mailbox belongs to. Forwards 1:1 to
    /// [`Account::directory_groups_list`]; gated by
    /// `capabilities().pim_methods.directory_groups_list` at the protocol
    /// layer.
    pub async fn directory_groups_list(
        &self,
        account_id: &AccountId,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::DirectoryGroup>, Error> {
        let page = self
            .live_account(account_id)?
            .directory_groups_list(page_cursor)
            .await?;
        self.announce_page_loss(account_id, "directory_groups_list", &page);
        Ok(page)
    }

    /// Expand one directory group to its user members (transitive,
    /// provider-side). Forwards 1:1 to
    /// [`Account::directory_group_expand`]; gated by
    /// `capabilities().pim_methods.directory_group_expand` at the
    /// protocol layer.
    pub async fn directory_group_expand(
        &self,
        account_id: &AccountId,
        group: bifrost_types::DirectoryGroupId,
        page_cursor: Option<Vec<u8>>,
    ) -> Result<bifrost_types::Page<bifrost_types::DirectoryGroupMember>, Error> {
        let page = self
            .live_account(account_id)?
            .directory_group_expand(group, page_cursor)
            .await?;
        self.announce_page_loss(account_id, "directory_group_expand", &page);
        Ok(page)
    }

    /// List an account's server-side filter rules or scripts. Forwards
    /// 1:1 to [`Account::filters_list`]; the supported model is
    /// advertised through `capabilities().filter_rule_shape` and
    /// per-method support through `capabilities().pim_methods`.
    pub async fn filters_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::ServerFilter>, Error> {
        Ok(self.live_account(account_id)?.filters_list().await?)
    }

    /// Create one server-side filter rule or script. Forwards to
    /// [`Account::filter_create`].
    pub async fn filter_create(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterCreate,
    ) -> Result<bifrost_types::ServerFilterId, Error> {
        Ok(self.live_account(account_id)?.filter_create(filter).await?)
    }

    /// Partially update one server-side filter rule or script. Forwards
    /// to [`Account::filter_update`].
    pub async fn filter_update(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterId,
        patch: bifrost_types::ServerFilterPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .filter_update(filter, patch)
            .await?)
    }

    /// Delete one server-side filter rule or script. Forwards to
    /// [`Account::filter_delete`].
    pub async fn filter_delete(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterId,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.filter_delete(filter).await?)
    }

    /// Validate a server-side filter payload without storing it. Forwards
    /// to [`Account::filter_validate`].
    pub async fn filter_validate(
        &self,
        account_id: &AccountId,
        filter: bifrost_types::ServerFilterCreate,
    ) -> Result<bifrost_types::FilterValidation, Error> {
        Ok(self
            .live_account(account_id)?
            .filter_validate(filter)
            .await?)
    }

    /// List the account's sending identities. Forwards 1:1 to
    /// [`Account::identities_list`]; per-method support is advertised
    /// through `capabilities().pim_methods`.
    pub async fn identities_list(
        &self,
        account_id: &AccountId,
    ) -> Result<Vec<bifrost_types::Identity>, Error> {
        Ok(self.live_account(account_id)?.identities_list().await?)
    }

    /// Partially update one sending identity. Forwards to
    /// [`Account::identity_update`].
    pub async fn identity_update(
        &self,
        account_id: &AccountId,
        identity: bifrost_types::IdentityId,
        patch: bifrost_types::IdentityPatch,
    ) -> Result<(), Error> {
        Ok(self
            .live_account(account_id)?
            .identity_update(identity, patch)
            .await?)
    }

    /// Read the vacation responder config, when supported. Forwards to
    /// [`Account::vacation_get`].
    pub async fn vacation_get(
        &self,
        account_id: &AccountId,
    ) -> Result<Option<bifrost_types::VacationConfig>, Error> {
        Ok(self.live_account(account_id)?.vacation_get().await?)
    }

    /// Replace the vacation responder config. Forwards to
    /// [`Account::vacation_set`].
    pub async fn vacation_set(
        &self,
        account_id: &AccountId,
        config: bifrost_types::VacationConfig,
    ) -> Result<(), Error> {
        Ok(self.live_account(account_id)?.vacation_set(config).await?)
    }

    /// Read the storage quota readout, when supported. Forwards to
    /// [`Account::quota_get`].
    pub async fn quota_get(
        &self,
        account_id: &AccountId,
    ) -> Result<Option<bifrost_types::QuotaInfo>, Error> {
        Ok(self.live_account(account_id)?.quota_get().await?)
    }
}
