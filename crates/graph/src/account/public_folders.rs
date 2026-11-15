//! Public-folder discovery and item-shape state.
use super::{
    PUBLIC_ITEM_CLASS_CACHE_CAP, PublicFolderScope, cursor, foreign, graph_error, public_folder,
};
use bifrost_types::ObjectId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Default)]
pub(crate) struct PublicFolderSnapshot {
    pub(crate) routing: HashMap<bifrost_types::FolderId, cursor::PublicFolderRouting>,
    pub(crate) metadata: HashMap<bifrost_types::FolderId, public_folder::PublicFolderMeta>,
}

#[derive(Clone)]
pub(crate) struct PublicFolders {
    policy: Option<PublicFolderScope>,
    discovery: Arc<RwLock<PublicFolderSnapshot>>,
    public_folder_discovery_complete: Arc<std::sync::atomic::AtomicBool>,
    public_item_classes: Arc<RwLock<HashMap<ObjectId, crate::ews::EwsItemShape>>>,
}
impl PublicFolders {
    pub(crate) fn new(policy: Option<PublicFolderScope>) -> Self {
        Self {
            policy,
            discovery: Arc::default(),
            public_folder_discovery_complete: Arc::default(),
            public_item_classes: Arc::default(),
        }
    }
    pub(crate) fn policy(&self) -> Option<&PublicFolderScope> {
        self.policy.as_ref()
    }
    #[cfg(test)]
    pub(crate) fn set_policy(&mut self, policy: PublicFolderScope) {
        self.policy = Some(policy);
    }
    pub(crate) fn set_discovery_complete(&self, complete: bool) {
        self.public_folder_discovery_complete
            .store(complete, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) async fn seed(
        &self,
        folder: bifrost_types::FolderId,
        routing: cursor::PublicFolderRouting,
        meta: public_folder::PublicFolderMeta,
    ) {
        // Publish routing and projection metadata together. A reader must
        // never see half a discovered folder, including a metered view.
        let mut discovery = self.discovery.write().await;
        discovery.routing.insert(folder.clone(), routing);
        discovery.metadata.insert(folder, meta);
    }
    pub(crate) async fn snapshot(&self) -> tokio::sync::RwLockReadGuard<'_, PublicFolderSnapshot> {
        self.discovery.read().await
    }
    #[cfg(test)]
    pub(crate) async fn item_classes(&self) -> HashMap<ObjectId, crate::ews::EwsItemShape> {
        self.public_item_classes.read().await.clone()
    }
    /// Look up the public-folder routing for a native EWS folder id, or
    /// `None` if the folder is not a public folder. Presence in the map
    /// is the discriminator that routes a `CursorScope::Folder` onto the
    /// public-folder sync path (vs the existing reject-on-delta path).
    pub(crate) async fn public_folder_routing(
        &self,
        folder: &bifrost_types::FolderId,
    ) -> Option<cursor::PublicFolderRouting> {
        self.discovery.read().await.routing.get(folder).cloned()
    }

    /// The refusal for a public-folder item whose folder has no routing entry.
    /// A retryable stale-local-state conflict until a complete discovery pass
    /// has run; after one, `NotFound(Message)` so the item stops retrying
    /// (ruling on `graph_error::unroutable_public_resource_error`).
    pub(crate) fn missing_public_route_error(
        &self,
        operation: bifrost_types::AccountOperation,
        id: &bifrost_types::ObjectId,
        folder: &bifrost_types::FolderId,
    ) -> bifrost_types::AccountError {
        self.missing_public_collection_route_error(
            operation,
            folder,
            bifrost_types::ErrorScope::Message {
                id: id.0.clone().into(),
            },
            bifrost_types::ResourceKind::Message,
            &id.0,
        )
    }

    /// Before discovery, preserve the visible resource while routing catches up.
    /// After a complete browse, an Outlook user expects an absent collection to
    /// stop retrying. Keep its actual resource kind and scope for the consumer.
    pub(crate) fn missing_public_collection_route_error(
        &self,
        operation: bifrost_types::AccountOperation,
        folder: &bifrost_types::FolderId,
        scope: bifrost_types::ErrorScope,
        kind: bifrost_types::ResourceKind,
        id: &str,
    ) -> bifrost_types::AccountError {
        let detail = format!("public folder {} has no routing entry", folder.0);
        if self
            .public_folder_discovery_complete
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            graph_error::unroutable_public_resource_error(
                operation,
                scope,
                kind,
                id,
                format!("{detail} after a complete public-folder discovery"),
            )
        } else {
            graph_error::stale_local_state_error(operation, scope, detail)
        }
    }

    /// Whether `folder` is a public folder this account has discovered: in
    /// the routing map (pinned for sync) or in the projection metadata
    /// (visible in the hierarchy). A public folder id is a bare EWS
    /// `FolderId`, so `foreign::parse_folder` reads it as primary; only
    /// discovery can tell the two apart. An undiscovered bare id cannot be
    /// classified here and is left to the server to refuse.
    pub(crate) async fn is_known_public_folder(&self, folder: &bifrost_types::FolderId) -> bool {
        let discovery = self.discovery.read().await;
        discovery.routing.contains_key(folder) || discovery.metadata.contains_key(folder)
    }

    /// Record the EWS item class of every item a public-folder inventory or
    /// poll walk returned, so a later `GetItem` for one of those ids can ask
    /// for the property shape that item's class accepts.
    ///
    /// Keyed by the same folder-qualified `ObjectId` the projections mint,
    /// so the hydration doors look up exactly the id they were handed. The
    /// map is bounded by `PUBLIC_ITEM_CLASS_CACHE_CAP`, which bounds
    /// admission of NEW ids only: past it hydration requests common
    /// properties for ids that never got in. The cache is only a property
    /// selection hint, never evidence of an item's domain.
    /// An id already resident is always refreshed, cap or no cap, so an item
    /// whose `ItemClass` changed does not keep a stale shape.
    pub(crate) async fn record_public_item_classes(
        &self,
        folder: &bifrost_types::FolderId,
        items: &[crate::ews::EwsItem],
    ) {
        if items.is_empty() {
            return;
        }
        let mut map = self.public_item_classes.write().await;
        let mut len = map.len();
        for item in items {
            let id = foreign::encode_public_item_id(folder, &item.item_id);
            let shape = crate::ews::EwsItemShape::from_item_class(&item.item_class);
            match map.entry(id) {
                // An id already in the map is refreshed unconditionally: the
                // cap bounds ADMISSION of new keys, and overwriting an
                // existing key does not grow the map. Without this an item
                // whose `ItemClass` changed would keep its stale shape for
                // the account's lifetime once the map filled up.
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    slot.insert(shape);
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    if len < PUBLIC_ITEM_CLASS_CACHE_CAP {
                        slot.insert(shape);
                        len += 1;
                    }
                }
            }
        }
    }

    /// Drop the cached item classes for ids the public-folder poll just
    /// emitted as `Destroyed`.
    ///
    /// The map is filled at every EWS walk and nothing else removed from
    /// it, so without this the entry for a deleted item lived until the
    /// account was dropped. That is not merely wasted memory: the cap is
    /// an admission bound, not an eviction policy (see
    /// `record_public_item_classes`), so a churning folder whose dead
    /// entries fill the map locks every LATER item out of the cache and
    /// silently demotes hydration to the neutral shape for the rest of
    /// the process. Pruning at the destroy site keeps the resident set
    /// proportional to the live set; the cap stays as the backstop for a
    /// folder that is genuinely that large.
    pub(crate) async fn forget_public_item_classes(&self, ids: &[ObjectId]) {
        if ids.is_empty() {
            return;
        }
        let mut map = self.public_item_classes.write().await;
        for id in ids {
            map.remove(id);
        }
    }

    /// The `GetItem` property shape to request for a public-folder item.
    ///
    /// The cache is a property-selection hint only. Without a cached item
    /// class request common properties; folder class cannot establish the
    /// domain of an item in a mixed folder.
    pub(crate) async fn public_item_shape(
        &self,
        id: &ObjectId,
        folder: &bifrost_types::FolderId,
    ) -> crate::ews::EwsItemShape {
        if let Some(shape) = self.public_item_classes.read().await.get(id) {
            return *shape;
        }
        let _ = folder;
        crate::ews::EwsItemShape::default()
    }
}
