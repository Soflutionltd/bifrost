//! Shared collection-account orchestration. Protocol crates keep payload
//! projection and token semantics; this kernel owns cursor and stream contracts.

use std::{collections::HashSet, marker::PhantomData, time::Instant};

use bifrost_types::*;
use futures::{Future, FutureExt, StreamExt, stream};

use crate::collection::{Dialect, ResourceListing};
use crate::{
    DavByteTally, PutCondition, SnapshotEntry, decode_snapshot, encode_snapshot, inventory_entry,
    preserve_unobserved_entries, same_dav_url,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectionSnapshot {
    pub collection_url: String,
    /// Opaque storage only: the protocol hook interprets sync-token versus ctag.
    pub token: Option<String>,
    pub entries: Vec<SnapshotEntry>,
    /// Per-poll observations, deliberately absent from persisted cursor bytes.
    pub failed_hrefs: Vec<String>,
}

impl CollectionSnapshot {
    pub fn preserve_unobserved(&mut self, previous: &Self) {
        let observation =
            preserve_unobserved_entries(&previous.entries, &mut self.entries, &self.failed_hrefs);
        self.token = observation.advance_token(previous.token.clone(), self.token.take());
    }

    pub fn changes_from(&self, previous: &Self) -> Vec<Change> {
        crate::diff_snapshots(&previous.entries, &self.entries, &self.failed_hrefs)
    }
}

/// A stateless account kernel: closures retain protocol-owned token and access
/// policy, while every account uses one cursor, inventory and settlement path.
pub struct DavCollectionAccount<D: Dialect>(PhantomData<D>);

impl<D: Dialect> DavCollectionAccount<D> {
    pub fn validate_scope(
        scope: &CursorScope,
        operation: AccountOperation,
    ) -> Result<(), AccountError> {
        if matches!(scope, CursorScope::Folder(_))
            || matches!(scope, CursorScope::Type(ty) | CursorScope::FolderType { ty, .. } if *ty == D::OBJECT_TYPE)
        {
            Ok(())
        } else {
            Err(Self::local_error(
                operation,
                format!(
                    "{} only supports {:?} cursor scopes",
                    D::DAV.label(),
                    D::OBJECT_TYPE
                ),
            ))
        }
    }

    pub fn collection_url_for_scope(
        scope: &CursorScope,
        default: Option<&str>,
        collections: &[String],
        operation: AccountOperation,
    ) -> Result<String, AccountError> {
        Self::validate_scope(scope, operation)?;
        match scope {
            CursorScope::Type(_) => default
                .map(str::to_owned)
                .ok_or_else(|| Self::no_default(operation)),
            CursorScope::Folder(folder) | CursorScope::FolderType { folder, .. }
                if collections.contains(&folder.0) =>
            {
                Ok(folder.0.clone())
            }
            _ => Err(Self::local_error(
                operation,
                format!(
                    "{} cursor scope does not name a discovered collection",
                    D::DAV.label()
                ),
            )),
        }
    }

    pub fn scope_collection_url<'a>(
        scope: &'a CursorScope,
        default: Option<&'a str>,
    ) -> Option<&'a str> {
        match scope {
            CursorScope::Folder(folder) | CursorScope::FolderType { folder, .. } => Some(&folder.0),
            _ => default,
        }
    }

    pub fn coverage_domain(scope: CursorScope, collection: Option<&str>) -> CoverageDomain {
        match &scope {
            CursorScope::Folder(_) => CoverageDomain::full(scope),
            CursorScope::FolderType { ty, .. } if *ty == D::OBJECT_TYPE => {
                CoverageDomain::full(scope)
            }
            // An unroutable legacy scope still needs a domain for termination;
            // no coverage is emitted when routing fails.
            _ => CoverageDomain {
                scope,
                coordinate: CoverageCoordinate::ProviderRegion {
                    namespace: D::DAV.field().to_owned(),
                    region: collection.unwrap_or_default().as_bytes().to_vec(),
                },
                snapshot: SnapshotIdentity::unstable(),
            },
        }
    }

    pub fn no_default(operation: AccountOperation) -> AccountError {
        Self::local_error(
            operation,
            format!(
                "{} account has no collection to route a call that names none",
                D::DAV.label()
            ),
        )
    }

    pub fn discovered_collections<'a>(hrefs: impl IntoIterator<Item = &'a str>) -> Vec<String> {
        hrefs.into_iter().map(str::to_owned).collect()
    }

    pub fn snapshot_from_listing(
        collection: &str,
        token: Option<String>,
        listing: ResourceListing,
        operation: AccountOperation,
    ) -> Result<CollectionSnapshot, AccountError> {
        // Complete inventory coverage must never checkpoint across a refused
        // member. A failed observation preserves an enterprise user's local row.
        if operation == AccountOperation::SyncInventory
            && let Some(failure) = listing.failed.first()
        {
            return Err(
                match failure
                    .status
                    .and_then(|code| reqwest::StatusCode::from_u16(code).ok())
                {
                    Some(status) => crate::status_error(
                        &reqwest::header::HeaderMap::new(),
                        operation,
                        status,
                        format!(
                            "inventory member refused: {}",
                            crate::url::diagnostic_origin(&failure.href)
                        ),
                        D::DAV,
                    ),
                    None => {
                        Self::parse_error(operation, "inventory member omitted usable properties")
                    }
                },
            );
        }
        let failed_hrefs = listing.failed_hrefs();
        let mut entries: Vec<_> = listing
            .entries
            .into_iter()
            .map(|entry| SnapshotEntry {
                uri: entry.uri,
                etag: entry.etag.map(Into::into),
            })
            .collect();
        entries.sort_by(|left, right| left.uri.cmp(&right.uri));
        let observation = preserve_unobserved_entries(&[], &mut entries, &failed_hrefs);
        Ok(CollectionSnapshot {
            collection_url: collection.to_owned(),
            token: observation.advance_token(None, token),
            entries,
            failed_hrefs,
        })
    }

    pub fn encode_snapshot(snapshot: &CollectionSnapshot) -> Vec<u8> {
        encode_snapshot(
            D::CURSOR_MAGIC,
            &snapshot.collection_url,
            snapshot.token.as_deref(),
            &snapshot.entries,
        )
    }

    pub fn cursor_from_snapshot(scope: CursorScope, snapshot: &CollectionSnapshot) -> ChangeCursor {
        ChangeCursor {
            scope,
            server_state: OpaqueChangeState {
                protocol: D::PROTOCOL_KIND,
                envelope_version: PayloadVersion::new(D::CURSOR_VERSION),
                bytes: Self::encode_snapshot(snapshot),
            },
            advanced_through: None,
            envelope_version: TraitEnvelopeVersion::new(CHANGE_CURSOR_ENVELOPE_VERSION),
        }
    }

    pub fn decode_snapshot(cursor: &ChangeCursor) -> Result<CollectionSnapshot, AccountError> {
        Self::validate_scope(&cursor.scope, AccountOperation::SyncChanges)?;
        let scoped = |fault: CursorDecodeFault| {
            fault
                .builder(D::DAV.protocol(), AccountOperation::SyncChanges)
                .scope(ErrorScope::Cursor(cursor.scope.clone()))
        };
        let bytes = cursor
            .server_state
            .open(D::PROTOCOL_KIND, PayloadVersion::new(D::CURSOR_VERSION))
            .map_err(|fault| {
                scoped(fault)
                    .try_build()
                    .expect("valid cursor header fault")
            })?;
        let decoded = decode_snapshot(D::CURSOR_MAGIC, D::DAV.label(), bytes).map_err(|error| {
            scoped(CursorDecodeFault::Malformed)
                .text(DiagnosticText::support_only(error))
                .try_build()
                .expect("valid malformed cursor")
        })?;
        // Native ids and bytes stay unchanged. Folder scopes cross-check the
        // payload collection; legacy Type scopes have no URL to compare.
        #[allow(clippy::unwrap_in_result)]
        if let CursorScope::Folder(folder) | CursorScope::FolderType { folder, .. } = &cursor.scope
            && !same_dav_url(&folder.0, &decoded.collection_url)
        {
            return Err(scoped(CursorDecodeFault::ScopeMismatch {
                scope: cursor.scope.clone(),
            })
            .text(DiagnosticText::support_only(
                "cursor collection does not match its folder scope",
            ))
            .try_build()
            .expect("valid cursor scope fault"));
        }
        Ok(CollectionSnapshot {
            collection_url: decoded.collection_url,
            token: decoded.token,
            entries: decoded.entries,
            failed_hrefs: Vec::new(),
        })
    }

    pub fn establish<F, Fut>(
        scope: CursorScope,
        default: Option<String>,
        collections: Vec<String>,
        snapshot: F,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>>
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: Future<Output = Result<CollectionSnapshot, AccountError>> + Send + 'static,
    {
        Box::pin(async move {
            let collection = Self::collection_url_for_scope(
                &scope,
                default.as_deref(),
                &collections,
                AccountOperation::EstablishCursor,
            )?;
            let snapshot = snapshot(collection)
                .await
                .map_err(|error| Self::sync_error(error, &scope))?;
            Ok(CursorEstablishment::Ready(Self::cursor_from_snapshot(
                scope, &snapshot,
            )))
        })
    }

    pub fn inventory<F, Fut>(
        scope: CursorScope,
        default: Option<String>,
        collections: Vec<String>,
        tally: DavByteTally,
        snapshot: F,
    ) -> AccountStream<InventoryEvent>
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: Future<Output = Result<CollectionSnapshot, AccountError>> + Send + 'static,
    {
        let domain = Self::coverage_domain(
            scope.clone(),
            Self::scope_collection_url(&scope, default.as_deref()),
        );
        Box::pin(
            stream::once(async move {
                let result = async {
                    let collection = Self::collection_url_for_scope(
                        &scope,
                        default.as_deref(),
                        &collections,
                        AccountOperation::SyncInventory,
                    )?;
                    let started = Instant::now();
                    let snapshot = snapshot(collection).await?;
                    let checkpoint = Self::cursor_from_snapshot(scope.clone(), &snapshot);
                    let items = snapshot.entries.iter().map(inventory_entry::<D>).collect();
                    Ok::<_, AccountError>(vec![
                        SyncEvent::Batch(Batch {
                            items,
                            page_boundary: PageBoundary::Final,
                            server_latency: started.elapsed(),
                            bytes_in: tally.take(),
                            checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                        }),
                        SyncEvent::Done(Some(Checkpoint::Change(checkpoint))),
                    ])
                }
                .await;
                result.unwrap_or_else(|error| {
                    vec![SyncEvent::Terminated(Self::sync_error(error, &scope))]
                })
            })
            .flat_map(stream::iter)
            .map(lift_complete_walk(domain)),
        )
    }

    pub fn changes<F, Fut>(
        cursor: ChangeCursor,
        tally: DavByteTally,
        changes: F,
    ) -> AccountStream<SyncEvent<Change>>
    where
        F: FnOnce(CollectionSnapshot) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(CollectionSnapshot, Vec<Change>), AccountError>>
            + Send
            + 'static,
    {
        Box::pin(
            stream::once(async move {
                let result = async {
                    let previous = Self::decode_snapshot(&cursor)?;
                    let started = Instant::now();
                    let (current, items) = changes(previous).await?;
                    let checkpoint = Self::cursor_from_snapshot(cursor.scope.clone(), &current);
                    Ok::<_, AccountError>(vec![
                        SyncEvent::Batch(Batch {
                            items,
                            page_boundary: PageBoundary::Final,
                            server_latency: started.elapsed(),
                            bytes_in: tally.take(),
                            checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
                        }),
                        SyncEvent::Done(Some(Checkpoint::Change(checkpoint))),
                    ])
                }
                .await;
                result.unwrap_or_else(|error| {
                    vec![SyncEvent::Terminated(Self::sync_error(
                        error,
                        &cursor.scope,
                    ))]
                })
            })
            .flat_map(stream::iter),
        )
    }

    /// Keep recovery on the active cursor's exact key. A known directory is
    /// retired while retaining the per-hop guard's transmission evidence.
    fn sync_error(error: AccountError, scope: &CursorScope) -> AccountError {
        // Restart uses an exact stored key. Keep accepted legacy scopes and
        // equivalent URL spellings restartable instead of replacing the key
        // with the client's canonical collection/type scope.
        if matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ) && matches!(error.scope(), Some(ErrorScope::Cursor(_)))
        {
            return error
                .into_builder()
                .scope(ErrorScope::Cursor(scope.clone()))
                .try_build()
                .expect("rebinding a cursor invalidation preserves its classification");
        }
        if !matches!(error.scope(), Some(ErrorScope::DirectoryGateway { .. }))
            || !matches!(error.kind(), AccountErrorKind::Unsupported(_))
        {
            return error;
        }
        let mut builder = AccountErrorBuilder::new(PrimaryCause::State(StateCause::ScopeRevoked {
            scope: scope.clone(),
        }))
        .protocol(D::DAV.protocol())
        .scope(ErrorScope::Cursor(scope.clone()));
        if let Some(operation) = error.operation() {
            builder = builder.operation(operation);
        }
        if let Some(ErrorScope::DirectoryGateway { url }) = error.scope() {
            builder = builder.text(DiagnosticText::support_only(format!(
                "personal sync scope targets a directory: {}",
                crate::url::diagnostic_origin(url)
            )));
        }
        for cause in error.chain().iter().cloned() {
            builder = builder.push_cause(cause);
        }
        builder
            .try_build()
            .expect("valid directory scope retirement")
    }

    pub fn skipped_scope(collection: &str, degraded: Option<AccountError>) -> Vec<SkippedScope> {
        degraded
            .map(|error| SkippedScope {
                scope: D::skipped_scope(collection),
                error,
            })
            .into_iter()
            .collect()
    }

    pub fn decode_page_cursor(
        cursor: Option<PageCursor>,
        operation: AccountOperation,
    ) -> Result<Option<String>, AccountError> {
        crate::decode_watermark_cursor(cursor, operation, D::DAV)
    }

    pub fn local_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
        crate::local_error(operation, message, D::DAV)
    }
    pub fn parse_error(operation: AccountOperation, message: impl Into<String>) -> AccountError {
        crate::parse_error(operation, message, D::DAV)
    }
    pub fn unsupported_future<T: Send + 'static>(
        operation: AccountOperation,
    ) -> AccountFuture<Result<T, AccountError>> {
        Box::pin(async move { Err(crate::unsupported_error(operation, D::DAV)) })
    }
    pub fn unsupported_stream<T: Send + 'static>(
        operation: AccountOperation,
    ) -> AccountStream<SyncEvent<T>> {
        Box::pin(stream::iter([
            SyncEvent::Terminated(crate::unsupported_error(operation, D::DAV)),
            SyncEvent::Done(None),
        ]))
    }
}

pub fn one_outcome_per_id<'a>(
    failed: &mut Vec<String>,
    materialized: impl IntoIterator<Item = &'a str>,
) {
    let materialized: HashSet<_> = materialized.into_iter().collect();
    failed.retain(|id| !materialized.contains(id.as_str()));
    failed.sort_unstable();
    failed.dedup();
}

/// No dynamic scopes are advertised. Keep subscriptions alive until close;
/// completing early would tell the consumer to reconnect.
#[derive(Debug)]
pub struct IdleLifecycle {
    sender: std::sync::Mutex<Option<futures::channel::oneshot::Sender<()>>>,
    shutdown: futures::future::Shared<futures::channel::oneshot::Receiver<()>>,
}

impl Default for IdleLifecycle {
    fn default() -> Self {
        let (sender, receiver) = futures::channel::oneshot::channel();
        Self {
            sender: std::sync::Mutex::new(Some(sender)),
            shutdown: receiver.shared(),
        }
    }
}

impl IdleLifecycle {
    pub fn stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::pending().take_until(self.shutdown.clone()))
    }
    pub fn close(&self) {
        let sender = self.sender.lock().expect("lifecycle lock poisoned").take();
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum RelocateFailure {
    Before(AccountError),
    MovedThenPut {
        destination: String,
        error: AccountError,
    },
    CopiedThenDelete {
        destination: String,
        source: String,
        error: AccountError,
    },
}

impl<D: Dialect> DavCollectionAccount<D> {
    pub async fn relocate(
        dispatch: &crate::DavDispatch,
        source: &str,
        target: &str,
        body: String,
        changed: bool,
        operation: AccountOperation,
        fallback_name: impl FnOnce() -> String,
    ) -> Result<(), RelocateFailure> {
        let destination = crate::append_path(target, &resource_file_name(source, fallback_name));
        let client = crate::collection::DavCollectionClient::<D>::new(dispatch);
        if dispatch
            .move_resource(source, &destination, operation)
            .await
            .map_err(RelocateFailure::Before)?
        {
            if !changed {
                return Ok(());
            }
            return client
                .put(&destination, body, PutCondition::None, operation)
                .await
                .map(|_| ())
                .map_err(|error| RelocateFailure::MovedThenPut { destination, error });
        }
        // Copy before delete: a failed cleanup leaves the user's resource
        // readable twice, the recoverable direction of a non-atomic move.
        client
            .put(&destination, body, PutCondition::IfNoneMatch, operation)
            .await
            .map_err(RelocateFailure::Before)?;
        client
            .delete(source, operation)
            .await
            .map(|_| ())
            .map_err(|error| RelocateFailure::CopiedThenDelete {
                destination,
                source: source.to_owned(),
                error,
            })
    }

    pub fn relocation_error(failure: RelocateFailure, original: &str) -> AccountError {
        let (error, source, destination, committed, effect, subject) = match failure {
            RelocateFailure::Before(error) => return error,
            RelocateFailure::MovedThenPut { destination, error } => {
                let subject = destination.clone();
                (
                    error,
                    original.to_owned(),
                    destination,
                    StepEffect::Moved,
                    StepEffect::Updated,
                    subject,
                )
            }
            RelocateFailure::CopiedThenDelete {
                destination,
                source,
                error,
            } => {
                let subject = source.clone();
                (
                    error,
                    source,
                    destination,
                    StepEffect::Copied,
                    StepEffect::Deleted,
                    subject,
                )
            }
        };
        let mut record = PartialCommitBuilder::new(PrimaryLeg::new(
            effect,
            StepTargets::complete(vec![TargetRef::new(
                TargetRole::Subject,
                D::item_scope(subject.clone()),
            )]),
            StepReplay::Repeatable,
            Application::from_leaf(&error),
        ));
        record.committed(
            committed,
            StepTargets::complete(vec![
                TargetRef::new(TargetRole::Source, D::item_scope(source)),
                TargetRef::new(TargetRole::Destination, D::item_scope(destination)),
            ]),
            StepReplay::Blocking,
        );
        error
            .into_builder()
            .scope(D::item_scope(subject))
            .partial_commit(record)
            .try_build()
            .expect("valid relocation record")
    }
}

pub fn resource_file_name(url: &str, fallback: impl FnOnce() -> String) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| {
            parsed
                .path_segments()
                .and_then(|mut segments| segments.next_back().filter(|name| !name.is_empty()))
                .map(str::to_owned)
        })
        .unwrap_or_else(fallback)
}

/// Candidate resources and the watermark for one hydration page.
pub struct PageRequest<'a> {
    pub collection: &'a str,
    pub candidates: crate::HrefQuery,
    pub watermark: Option<&'a str>,
    pub page_size: usize,
}

impl<D: Dialect> DavCollectionAccount<D> {
    /// Page resources before hydration. Projection may expand one resource into
    /// several events, so the cursor always names an href, never a projected id.
    pub async fn hydrated_page<T, E, F, Fut>(
        request: PageRequest<'_>,
        fetch: F,
        mut project: impl FnMut(crate::collection::FetchedResource) -> Result<Vec<T>, E>,
        keep: impl Fn(&T) -> bool,
        key: impl Fn(&T) -> &str,
    ) -> Result<Page<T>, AccountError>
    where
        F: FnOnce(Vec<String>) -> Fut,
        Fut: Future<Output = Result<crate::collection::MultigetFetch, AccountError>>,
    {
        let PageRequest {
            collection,
            candidates,
            watermark,
            page_size,
        } = request;
        let crate::HrefQuery {
            hrefs,
            mut failed_hrefs,
            degraded,
        } = candidates;
        let hrefs = crate::sorted_candidate_hrefs(hrefs);
        let total = u64::try_from(hrefs.len()).unwrap_or(u64::MAX);
        let slice = crate::slice_after_watermark(hrefs, watermark, page_size, String::as_str);
        let fetched = fetch(slice.items).await?;
        let skipped_scopes = Self::skipped_scope(
            collection,
            crate::collection::worse_recovery_option(degraded, fetched.degraded),
        );
        failed_hrefs.extend(fetched.report.failed_hrefs());
        let mut items = Vec::new();
        let mut materialized = HashSet::new();
        for resource in fetched.report.items {
            let uri = resource.uri.clone();
            match project(resource) {
                Ok(projected) => {
                    materialized.insert(uri);
                    items.extend(projected.into_iter().filter(&keep));
                }
                Err(_) => failed_hrefs.push(uri),
            }
        }
        one_outcome_per_id(&mut failed_hrefs, materialized.iter().map(String::as_str));
        items.sort_by(|left, right| key(left).cmp(key(right)));
        Ok(Page {
            truncated: false,
            items,
            next_cursor: slice
                .next_watermark
                .as_deref()
                .map(crate::encode_watermark_cursor),
            estimated_total: Some(total),
            failed_ids: failed_hrefs,
            skipped_scopes,
        })
    }
}

impl<D: Dialect> DavCollectionAccount<D> {
    pub fn default_collection<'a>(hrefs: impl IntoIterator<Item = &'a str>) -> Option<String> {
        // No home fallback: an empty discovery means no routable collection,
        // not a phantom calendar/address book an enterprise user cannot open.
        hrefs.into_iter().next().map(str::to_owned)
    }

    pub fn discover_scopes(
        hrefs: impl IntoIterator<Item = String>,
    ) -> AccountStream<SyncEvent<CursorScope>> {
        let items = hrefs
            .into_iter()
            .map(|url| CursorScope::FolderType {
                folder: FolderId(url),
                ty: D::OBJECT_TYPE,
            })
            .collect();
        Box::pin(stream::iter([
            SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: Default::default(),
                bytes_in: 0,
                checkpoint: None,
            }),
            SyncEvent::Done(None),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::task::{Context, Poll, Wake, Waker};

    use super::IdleLifecycle;

    struct CloseWake {
        lifecycle: Arc<IdleLifecycle>,
        unlocked: AtomicBool,
    }

    impl Wake for CloseWake {
        fn wake(self: Arc<Self>) {
            // A subscriber's waker may synchronously ask the account to close
            // again. Sending under the close mutex would make that reentrant
            // call deadlock. Test availability without hanging the suite.
            self.unlocked
                .store(self.lifecycle.sender.try_lock().is_ok(), Ordering::SeqCst);
        }
    }

    #[test]
    fn lifecycle_releases_close_lock_before_waking_subscribers() {
        let lifecycle = Arc::new(IdleLifecycle::default());
        let wake = Arc::new(CloseWake {
            lifecycle: Arc::clone(&lifecycle),
            unlocked: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let mut stream = lifecycle.stream();
        assert!(matches!(
            stream.as_mut().poll_next(&mut context),
            Poll::Pending
        ));
        lifecycle.close();
        assert!(wake.unlocked.load(Ordering::SeqCst));
        assert!(matches!(
            stream.as_mut().poll_next(&mut context),
            Poll::Ready(None)
        ));
        lifecycle.close();
    }
}
