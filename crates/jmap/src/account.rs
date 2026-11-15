use crate::{
    client::Client,
    core::{capability::Capability, id::AccountId, method::JmapMethod, transport::HttpTransport},
};

/// An account-scoped view of a [`Client`].
///
/// Pairs an owned `Client` (cheap `Arc`-clone, see [`Client`]) with a
/// specific account ID. Use [`Account::build`] to create request batches
/// pre-scoped to this account, avoiding the need to thread account IDs
/// manually.
///
/// `Account` carries no public lifetime: it can be stored in long-lived
/// structs, cloned freely, and moved across tasks.
///
/// ```ignore
/// use crate::core::capability;
///
/// let mail_account = client.primary_account::<capability::Mail>()?;
/// let mut request = mail_account.build();
/// // ... add method calls scoped to this account ...
/// ```
///
/// JMAP allows different primary account IDs per capability (RFC 8620
/// §2). Use [`Client::primary_account`] with a capability marker to
/// pick the correct one rather than assuming
/// [`Client::default_account_id`] applies to every capability.
// Capability selection below currently erases C. Fixing this requires typed
// handles throughout the sync account's mail/submission/vacation/quota/sieve/
// contacts/calendars fields and its foreign-account helpers, not just a new
// generic here. A method-equality bound alone would also reject legitimate
// cross-capability calls on accounts advertising both capabilities, including
// foreign mail submission and core Blob/copy. Those need an explicit checked
// capability view. Retaining an untyped default to avoid migrating the callers
// would preserve the very wrong-account call path the type is meant to close.
pub(crate) struct Account<Tr: HttpTransport> {
    client: Client<Tr>,
    account_id: AccountId,
}

impl<Tr: HttpTransport> Clone for Account<Tr> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            account_id: self.account_id.clone(),
        }
    }
}

impl<Tr: HttpTransport> Account<Tr> {
    /// An account view whose every `/jmap/api` call reports its inbound
    /// payload bytes into a fresh batch accumulator.
    pub(crate) fn metered(&self) -> (Self, crate::client::ByteTally) {
        let tally = crate::client::ByteTally::default();
        (self.metered_into(&tally), tally)
    }

    /// An account view reporting into an EXISTING accumulator, so a
    /// batch whose traffic runs through both an account view and the
    /// bare client counts both legs once.
    pub(crate) fn metered_into(&self, tally: &crate::client::ByteTally) -> Self {
        Self {
            client: self.client.metered_into(tally),
            account_id: self.account_id.clone(),
        }
    }

    /// Construct an `Account` directly. The account ID is not validated
    /// against the session; prefer [`Client::primary_account`] for
    /// capability-aware selection.
    pub(crate) fn new(client: Client<Tr>, account_id: impl Into<AccountId>) -> Self {
        Self {
            client,
            account_id: account_id.into(),
        }
    }

    /// The account ID.
    pub(crate) fn id(&self) -> &AccountId {
        &self.account_id
    }

    /// The account ID as a string slice.
    pub(crate) fn id_str(&self) -> &str {
        self.account_id.as_str()
    }

    /// Access the underlying client.
    pub(crate) fn client(&self) -> &Client<Tr> {
        &self.client
    }

    /// Build a request batch scoped to this account.
    pub(crate) fn build(&self) -> crate::core::request::Request<'_, Tr> {
        self.client
            .build()
            .account_id(self.account_id.as_str().to_string())
    }

    /// Send a single method call against this account and return its
    /// typed response.
    ///
    /// This is the protocol-layer entry point for one-off method
    /// calls. For batched / cross-method workflows (using result
    /// references, multiple calls in one round-trip, etc.), use
    /// [`Account::build`] to construct a [`Request`] and add calls via
    /// [`Request::call`] manually.
    ///
    /// The method's `accountId` field is injected from this account,
    /// so callers do not pass it through the method-struct
    /// constructor.
    ///
    /// [`Request`]: crate::core::request::Request
    /// [`Request::call`]: crate::core::request::Request::call
    pub(crate) async fn call<M: JmapMethod>(&self, method: M) -> crate::Result<M::Response> {
        let mut request = self.build();
        let handle = request.call(method)?;
        request.send_single(&handle).await
    }
}

impl<Tr: HttpTransport> Client<Tr> {
    /// Resolve the primary account for a given capability.
    ///
    /// JMAP servers advertise per-capability primary accounts in the
    /// session's `primaryAccounts` map (RFC 8620 §2). The mail and
    /// calendar primary accounts may share an ID or differ; passing the
    /// capability marker forces an explicit choice at the type level
    /// rather than silently using whichever primary the session
    /// happens to list first.
    ///
    /// Returns [`crate::Error::NoPrimaryAccount`] if the server does
    /// not list a primary account for `C::URI`.
    ///
    /// ```ignore
    /// use crate::core::capability;
    ///
    /// let mail = client.primary_account::<capability::Mail>()?;
    /// let cal  = client.primary_account::<capability::Calendars>()?;
    /// ```
    pub(crate) fn primary_account<C: Capability>(&self) -> crate::Result<Account<Tr>> {
        let session = self.session();
        let account_id = session
            .primary_accounts()
            .find(|(uri, _)| uri.as_str() == C::URI)
            .map(|(_, id)| id.clone())
            .ok_or(crate::Error::NoPrimaryAccount { capability: C::URI })?;

        Ok(Account::new(self.clone(), account_id))
    }
}
