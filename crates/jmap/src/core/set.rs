use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use super::id::{AccountId, ObjectState};
use super::{Object, SetCreate, request::ResultReference};

/// Escape one token of an RFC 8620 s5.3 patch path (a JSON Pointer,
/// RFC 6901): `~` becomes `~0` and `/` becomes `~1`.
///
/// New dynamic paths use `core::patch::PatchPath::new`. This encoder stays
/// available for account-layer callers that already build encoded wire paths.
pub(crate) fn escape_json_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

/// Implemented by a JMAP object's canonical (Get-shape) type. Declares
/// the input shapes used by `SetRequest`/`CopyRequest`.
///
/// `Create` and `Patch` only need to be `Serialize`. The
/// constructable / creatable side is gated by separate trait bounds
/// (`Create: SetCreate`, `Patch: Default`) on the methods that need
/// them, so destroy-only objects (like `ShareNotification` which
/// cannot be created or updated) can declare uninhabited or unit-like
/// `Create`/`Patch` types and have `create()`/`update()` simply not
/// resolve.
pub(crate) trait SetObject: Object {
    type Create: Serialize + Send;
    type Patch: Serialize + Send;
    type SetArguments: Default + Serialize + Send;
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SetRequest<O: SetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    account_id: Option<AccountId>,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_in_state: Option<ObjectState<O>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    create: Option<HashMap<String, O::Create>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    update: Option<HashMap<O::Id, O::Patch>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    destroy: Option<Vec<O::Id>>,

    #[serde(rename = "#destroy")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    destroy_ref: Option<ResultReference>,

    #[serde(flatten)]
    arguments: O::SetArguments,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SetResponse<O: SetObject> {
    #[serde(skip)]
    pub(crate) answer_context: crate::core::answer::AnswerContext,
    #[serde(rename = "accountId")]
    account_id: Option<AccountId>,

    #[serde(rename = "oldState")]
    old_state: Option<ObjectState<O>>,

    #[serde(rename = "newState")]
    new_state: Option<ObjectState<O>>,

    /// Successful creates, keyed by the consumer-provided create-id
    /// (e.g. "c1"). Create-ids are not real server IDs and stay
    /// `String`-keyed.
    #[serde(rename = "created")]
    created: Option<HashMap<String, O>>,

    /// Successful updates, keyed by the real server ID.
    #[serde(rename = "updated")]
    updated: Option<HashMap<O::Id, Option<O>>>,

    /// Successfully destroyed real server IDs.
    #[serde(rename = "destroyed")]
    destroyed: Option<Vec<O::Id>>,

    /// Failed creates, keyed by the consumer-provided create-id.
    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, SetError<O::Property>>>,

    #[serde(rename = "notUpdated")]
    not_updated: Option<HashMap<O::Id, SetError<O::Property>>>,

    #[serde(rename = "notDestroyed")]
    not_destroyed: Option<HashMap<O::Id, SetError<O::Property>>>,
}

#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub(crate) struct SetError<U>
where
    U: Display,
{
    #[serde(rename = "type")]
    type_: SetErrorType,
    description: Option<String>,
    properties: Option<Vec<U>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum SetErrorType {
    Forbidden,
    OverQuota,
    TooLarge,
    RateLimit,
    NotFound,
    InvalidPatch,
    WillDestroy,
    InvalidProperties,
    Singleton,
    MailboxHasChild,
    MailboxHasEmail,
    BlobNotFound,
    TooManyKeywords,
    TooManyMailboxes,
    ForbiddenFrom,
    InvalidEmail,
    TooManyRecipients,
    NoRecipients,
    InvalidRecipients,
    ForbiddenMailFrom,
    ForbiddenToSend,
    CannotUnsend,
    AlreadyExists,
    InvalidScript,
    ScriptIsActive,
    /// Unknown wire code. Carries the actual code string so the
    /// `into_account_error` boundary can surface it through
    /// `JmapMethod::Unknown { code }` rather than synthesizing a
    /// placeholder `"other"` literal the server never sent.
    Other(String),
}

impl<'de> serde::Deserialize<'de> for SetErrorType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "forbidden" => Self::Forbidden,
            "overQuota" => Self::OverQuota,
            "tooLarge" => Self::TooLarge,
            "rateLimit" => Self::RateLimit,
            "notFound" => Self::NotFound,
            "invalidPatch" => Self::InvalidPatch,
            "willDestroy" => Self::WillDestroy,
            "invalidProperties" => Self::InvalidProperties,
            "singleton" => Self::Singleton,
            "mailboxHasChild" => Self::MailboxHasChild,
            "mailboxHasEmail" => Self::MailboxHasEmail,
            "blobNotFound" => Self::BlobNotFound,
            "tooManyKeywords" => Self::TooManyKeywords,
            "tooManyMailboxes" => Self::TooManyMailboxes,
            "forbiddenFrom" => Self::ForbiddenFrom,
            "invalidEmail" => Self::InvalidEmail,
            "tooManyRecipients" => Self::TooManyRecipients,
            "noRecipients" => Self::NoRecipients,
            "invalidRecipients" => Self::InvalidRecipients,
            "forbiddenMailFrom" => Self::ForbiddenMailFrom,
            "forbiddenToSend" => Self::ForbiddenToSend,
            "cannotUnsend" => Self::CannotUnsend,
            "alreadyExists" => Self::AlreadyExists,
            "invalidScript" => Self::InvalidScript,
            "scriptIsActive" => Self::ScriptIsActive,
            _ => Self::Other(value),
        })
    }
}

impl<O: SetObject> SetRequest<O> {
    /// Construct an empty `SetRequest`. The `accountId` field is left
    /// unset (or `None` for non-account-scoped objects); it is filled
    /// in by [`crate::core::request::Request::call`] when the method
    /// is added to a request batch.
    pub(crate) fn new() -> Self {
        Self {
            account_id: if O::requires_account_id() {
                Some(AccountId::new(""))
            } else {
                None
            },
            if_in_state: None,
            create: None,
            update: None,
            destroy: None,
            destroy_ref: None,
            arguments: O::SetArguments::default(),
        }
    }

    pub(crate) fn account_id(&mut self, account_id: impl Into<AccountId>) -> &mut Self {
        if O::requires_account_id() {
            self.account_id = Some(account_id.into());
        }
        self
    }

    pub(crate) fn if_in_state(&mut self, if_in_state: impl Into<ObjectState<O>>) -> &mut Self {
        self.if_in_state = Some(if_in_state.into());
        self
    }

    pub(crate) fn destroy<U, V>(&mut self, ids: U) -> &mut Self
    where
        U: IntoIterator<Item = V>,
        V: Into<O::Id>,
    {
        self.destroy
            .get_or_insert_with(Vec::new)
            .extend(ids.into_iter().map(std::convert::Into::into));
        self.destroy_ref = None;
        self
    }

    pub(crate) fn destroy_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.destroy_ref = reference.into();
        self.destroy = None;
        self
    }

    pub(crate) fn arguments(&mut self) -> &mut O::SetArguments {
        &mut self.arguments
    }
}

impl<O: SetObject> SetRequest<O>
where
    O::Create: SetCreate,
{
    /// Get or insert a fresh create entry with auto-assigned `cN` id.
    pub(crate) fn create(&mut self) -> &mut O::Create {
        let create_id = crate::core::creation::CreationId::next('c', self.create.as_ref());
        let create_id_str = create_id.to_string();
        self.create
            .get_or_insert_with(HashMap::new)
            .entry(create_id_str)
            .or_insert_with(|| O::Create::new(Some(create_id)))
    }

    pub(crate) fn create_with_id(&mut self, create_id: impl Into<String>) -> &mut O::Create {
        let create_id = crate::core::creation::CreationId::named(create_id);
        let create_id_str = create_id.to_string();
        self.create
            .get_or_insert_with(HashMap::new)
            .entry(create_id_str)
            .or_insert_with(|| O::Create::new(Some(create_id)))
    }

    pub(crate) fn create_item(&mut self, mut item: O::Create) -> String {
        let create_id = crate::core::creation::CreationId::next('c', self.create.as_ref());
        let create_id_str = create_id.to_string();
        item.set_create_id(create_id);
        self.create
            .get_or_insert_with(HashMap::new)
            .insert(create_id_str.clone(), item);
        create_id_str
    }

    pub(crate) fn update_item(&mut self, id: impl Into<O::Id>, item: O::Patch) {
        self.update
            .get_or_insert_with(HashMap::new)
            .insert(id.into(), item);
    }
}

impl<O: SetObject> SetRequest<O>
where
    O::Patch: Default,
{
    pub(crate) fn update(&mut self, id: impl Into<O::Id>) -> &mut O::Patch {
        let id: O::Id = id.into();
        self.update
            .get_or_insert_with(HashMap::new)
            .entry(id)
            .or_default()
    }
}

impl<O: SetObject> Default for SetRequest<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: SetObject> SetResponse<O> {
    pub(crate) fn account_id(&self) -> Option<&AccountId> {
        self.account_id.as_ref()
    }

    pub(crate) fn old_state(&self) -> Option<&ObjectState<O>> {
        self.old_state.as_ref()
    }

    /// The `newState` the server reported, or `None` when it sent none.
    ///
    /// RFC 8620 s5.3 makes `newState` mandatory, so `None` is a
    /// non-conforming server - but the two cases must stay
    /// distinguishable here. Folding absence into `""` made a missing
    /// state indistinguishable from a state the server really did report
    /// as empty, and the state cache gives an explicitly empty entry its
    /// own meaning ("probed, empty") separate from "never probed". A
    /// fabricated `""` therefore poisoned a cache entry with a value no
    /// server had ever sent. Callers decide what absence costs them; the
    /// accessor does not decide for them by inventing a value.
    pub(crate) fn new_state(&self) -> Option<&ObjectState<O>> {
        self.new_state.as_ref()
    }

    pub(crate) fn into_new_state(self) -> Option<ObjectState<O>> {
        self.new_state
    }

    /// Look up a successful or failed create by its create-id (e.g. "c1").
    pub(crate) fn created(&mut self, id: &str) -> crate::Result<O> {
        crate::core::answer::IdAnswer::from_parts(
            self.created.as_mut().and_then(|values| values.remove(id)),
            self.not_created
                .as_mut()
                .and_then(|values| values.remove(id))
                .map(|error| crate::Error::Set(error.to_string_error())),
            false,
        )
        .into_result(id, self.answer_context)
    }

    pub(crate) fn updated(&mut self, id: &O::Id) -> crate::Result<Option<O>> {
        crate::core::answer::IdAnswer::from_parts(
            self.updated.as_mut().and_then(|values| values.remove(id)),
            self.not_updated
                .as_mut()
                .and_then(|values| values.remove(id))
                .map(|error| crate::Error::Set(error.to_string_error())),
            false,
        )
        .into_result(&id.to_string(), self.answer_context)
    }

    pub(crate) fn destroyed(&mut self, id: &O::Id) -> crate::Result<()> {
        crate::core::answer::IdAnswer::from_parts(
            self.destroyed
                .as_ref()
                .filter(|ids| ids.contains(id))
                .map(|_| ()),
            self.not_destroyed
                .as_mut()
                .and_then(|values| values.remove(id))
                .map(|error| crate::Error::Set(error.to_string_error())),
            false,
        )
        .into_result(&id.to_string(), self.answer_context)
    }

    /// Iterate the create-ids of successful creates. Create-ids are
    /// the "c1"/"c2"/... identifiers the consumer passed in, not real
    /// server IDs - real IDs live on each `O` value.
    pub(crate) fn created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.created.as_ref().map(|map| map.keys())
    }

    pub(crate) fn updated_ids(&self) -> Option<impl Iterator<Item = &O::Id>> {
        self.updated.as_ref().map(|map| map.keys())
    }

    pub(crate) fn into_updated_ids(self) -> Option<Vec<O::Id>> {
        self.updated.map(|map| map.into_keys().collect())
    }

    pub(crate) fn destroyed_ids(&self) -> Option<impl Iterator<Item = &O::Id>> {
        self.destroyed.as_ref().map(|list| list.iter())
    }

    pub(crate) fn into_destroyed_ids(self) -> Option<Vec<O::Id>> {
        self.destroyed
    }

    /// Iterate failed-create create-ids (consumer-provided, not real IDs).
    pub(crate) fn not_created_ids(&self) -> Option<impl Iterator<Item = &String>> {
        self.not_created.as_ref().map(|map| map.keys())
    }

    pub(crate) fn not_updated_ids(&self) -> Option<impl Iterator<Item = &O::Id>> {
        self.not_updated.as_ref().map(|map| map.keys())
    }

    pub(crate) fn not_destroyed_ids(&self) -> Option<impl Iterator<Item = &O::Id>> {
        self.not_destroyed.as_ref().map(|map| map.keys())
    }

    pub(crate) fn has_updated(&self) -> bool {
        self.updated.as_ref().is_some_and(|m| !m.is_empty())
    }

    pub(crate) fn has_created(&self) -> bool {
        self.created.as_ref().is_some_and(|m| !m.is_empty())
    }

    pub(crate) fn has_destroyed(&self) -> bool {
        self.destroyed.as_ref().is_some_and(|m| !m.is_empty())
    }

    /// All failed updates with their object identities, without selecting a
    /// hash-iteration winner or consuming sibling failures.
    pub(crate) fn update_errors(&self) -> impl Iterator<Item = (&O::Id, &SetError<O::Property>)> {
        self.not_updated.iter().flat_map(|errors| errors.iter())
    }

    /// All failed creates with their request-owned creation identities.
    pub(crate) fn create_errors(&self) -> impl Iterator<Item = (&String, &SetError<O::Property>)> {
        self.not_created.iter().flat_map(|errors| errors.iter())
    }
}

impl<U: Display> SetError<U> {
    pub(crate) fn error_type(&self) -> &SetErrorType {
        &self.type_
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn properties(&self) -> Option<&[U]> {
        self.properties.as_deref()
    }

    pub(crate) fn to_string_error(&self) -> SetError<String> {
        SetError {
            type_: self.type_.clone(),
            description: self.description.clone(),
            properties: self
                .properties
                .as_ref()
                .map(|s| s.iter().map(std::string::ToString::to_string).collect()),
        }
    }
}

impl<U: Display> Display for SetError<U> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.type_.fmt(f)?;
        if let Some(description) = &self.description {
            write!(f, ": {description}")?;
        }
        if let Some(properties) = &self.properties {
            write!(
                f,
                " (properties: {})",
                properties
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<String>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

impl Display for SetErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SetErrorType::Forbidden => write!(f, "forbidden"),
            SetErrorType::OverQuota => write!(f, "overQuota"),
            SetErrorType::TooLarge => write!(f, "tooLarge"),
            SetErrorType::RateLimit => write!(f, "rateLimit"),
            SetErrorType::NotFound => write!(f, "notFound"),
            SetErrorType::InvalidPatch => write!(f, "invalidPatch"),
            SetErrorType::WillDestroy => write!(f, "willDestroy"),
            SetErrorType::InvalidProperties => write!(f, "invalidProperties"),
            SetErrorType::Singleton => write!(f, "singleton"),
            SetErrorType::MailboxHasChild => write!(f, "mailboxHasChild"),
            SetErrorType::MailboxHasEmail => write!(f, "mailboxHasEmail"),
            SetErrorType::BlobNotFound => write!(f, "blobNotFound"),
            SetErrorType::TooManyKeywords => write!(f, "tooManyKeywords"),
            SetErrorType::TooManyMailboxes => write!(f, "tooManyMailboxes"),
            SetErrorType::ForbiddenFrom => write!(f, "forbiddenFrom"),
            SetErrorType::InvalidEmail => write!(f, "invalidEmail"),
            SetErrorType::TooManyRecipients => write!(f, "tooManyRecipients"),
            SetErrorType::NoRecipients => write!(f, "noRecipients"),
            SetErrorType::InvalidRecipients => write!(f, "invalidRecipients"),
            SetErrorType::ForbiddenMailFrom => write!(f, "forbiddenMailFrom"),
            SetErrorType::ForbiddenToSend => write!(f, "forbiddenToSend"),
            SetErrorType::CannotUnsend => write!(f, "cannotUnsend"),
            SetErrorType::AlreadyExists => write!(f, "alreadyExists"),
            SetErrorType::InvalidScript => write!(f, "invalidScript"),
            SetErrorType::ScriptIsActive => write!(f, "scriptIsActive"),
            SetErrorType::Other(code) => write!(f, "{code}"),
        }
    }
}

// Legacy integer-taking filters, vacation dates and push expiry still use
// this conversion. New builders should accept Timestamp directly: silently
// replacing an invalid date with the epoch changes the user's requested date.
pub(crate) fn from_timestamp(timestamp: i64) -> Timestamp {
    Timestamp::from_second(timestamp).unwrap_or(Timestamp::UNIX_EPOCH)
}
