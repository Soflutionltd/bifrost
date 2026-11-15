//! Unsupported DAV account lanes, declared once for both collection dialects.

/// Implement the shared non-mail account refusals. Domain-specific supported
/// methods stay in their protocol crate; new primitives are checked by Account.
#[macro_export]
macro_rules! dav_unsupported_account_methods {
    ($dialect:ty) => {
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::DiscoverMemberships)
    }

    fn get_stream(
        &self,
        _ids: AccountStream<ObjectId>,
        _projection: bifrost_types::Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::Hydrate)
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::PushSubscribe)
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::PushUnsubscribe)
    }

    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::OpenBlob)
    }

    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::OpenBlobRange)
    }

    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::OpenRawRfc822)
    }

    fn bulk_set_flags(
        &self,
        _targets: AccountStream<ObjectId>,
        op: FlagOp,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        match op.validate_for_account(<$dialect as $crate::collection::Dialect>::DAV.protocol()) {
            Ok(()) => $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::UpdateFlags),
            Err(error) => Box::pin(futures::stream::once(async move {
                SyncEvent::Terminated(error)
            })),
        }
    }

    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::BulkMove)
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_stream(AccountOperation::BulkDestroy)
    }

    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::AddToContainer)
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::RemoveFromContainer)
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: bifrost_types::Flag,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::SetKeyword)
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::SetLabelMembership)
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::SetCategory)
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::SetExtendedProperty)
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::SetIsRead)
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::SetImportance)
    }

    fn send_message(
        &self,
        _request: SendRequest,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::Send)
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::AttachmentUpload)
    }

    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::HostAttachment)
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::DraftCreate)
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::DraftUpdate)
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::DraftDiscard)
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<SendReceipt, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::DraftSend)
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::CancelScheduledSend)
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::RescheduleSend)
    }

    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::Search)
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::SearchMessages)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::ContainersList)
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::ContainerCreate)
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::ContainerRename)
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::ContainerMove)
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::ContainerDelete)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::IdentitiesList)
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::IdentityUpdate)
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::VacationGet)
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::VacationSet)
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::QuotaGet)
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::FiltersList)
    }

    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::FilterCreate)
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::FilterUpdate)
    }

    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::FilterDelete)
    }

    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::FilterValidate)
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::DirectoryGroupsList)
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::DirectoryGroupExpand)
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::HydrateThread)
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        $crate::collection_account::DavCollectionAccount::<$dialect>::unsupported_future(AccountOperation::HydrateMessage)
    }

    };
}

/// Bind the shared DAV error constructors to one collection dialect.
#[macro_export]
macro_rules! dav_error_constructors {
    ($dialect:ty) => {
        pub(crate) fn unsupported_error(
            operation: bifrost_types::AccountOperation,
        ) -> bifrost_types::AccountError {
            $crate::unsupported_error(operation, <$dialect as $crate::collection::Dialect>::DAV)
        }
        pub(crate) fn local_error(
            operation: bifrost_types::AccountOperation,
            message: impl Into<String>,
        ) -> bifrost_types::AccountError {
            $crate::local_error(
                operation,
                message,
                <$dialect as $crate::collection::Dialect>::DAV,
            )
        }
        pub(crate) fn parse_error(
            operation: bifrost_types::AccountOperation,
            message: impl Into<String>,
        ) -> bifrost_types::AccountError {
            $crate::parse_error(
                operation,
                message,
                <$dialect as $crate::collection::Dialect>::DAV,
            )
        }
        #[cfg(test)]
        pub(crate) fn transport_error(
            operation: bifrost_types::AccountOperation,
            message: impl Into<String>,
        ) -> bifrost_types::AccountError {
            $crate::transport_error(
                operation,
                message,
                <$dialect as $crate::collection::Dialect>::DAV,
            )
        }
        pub(crate) fn status_error(
            headers: &reqwest::header::HeaderMap,
            operation: bifrost_types::AccountOperation,
            status: reqwest::StatusCode,
            body: String,
        ) -> bifrost_types::AccountError {
            $crate::status_error(
                headers,
                operation,
                status,
                body,
                <$dialect as $crate::collection::Dialect>::DAV,
            )
        }
    };
}
