use crate::{
    client::Client,
    core::{capability::Capability, id::AccountId, method::JmapMethod, transport::HttpTransport},
};

/// A capability-scoped account. The capability survives selection so a Mail
/// handle cannot send Calendar methods to the wrong primary account. Accounts
/// serving several families expose explicit, session-checked views.
pub(crate) struct Account<Tr: HttpTransport, C: Capability = crate::core::capability::Mail> {
    client: Client<Tr>,
    account_id: AccountId,
    capability: std::marker::PhantomData<fn() -> C>,
}

impl<Tr: HttpTransport, C: Capability> Clone for Account<Tr, C> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            account_id: self.account_id.clone(),
            capability: std::marker::PhantomData,
        }
    }
}

impl<Tr: HttpTransport, C: Capability> Account<Tr, C> {
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
            capability: std::marker::PhantomData,
        }
    }

    fn selected(client: Client<Tr>, account_id: AccountId) -> Self {
        Self {
            client,
            account_id,
            capability: std::marker::PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) fn new(client: Client<Tr>, account_id: impl Into<AccountId>) -> Self {
        Self::selected(client, account_id.into())
    }

    /// Keep this exact accountId; never switch to another family's primary.
    pub(crate) fn view<D: Capability>(&self) -> crate::Result<Account<Tr, D>> {
        self.client.account::<D>(self.account_id.clone())
    }

    /// Core methods are served by every account, including Blob/copy.
    pub(crate) fn core(&self) -> Account<Tr, crate::core::capability::Core> {
        Account::selected(self.client.clone(), self.account_id.clone())
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
    pub(crate) fn build(&self) -> crate::core::request::Request<'_, Tr, C> {
        crate::core::request::Request::for_account(self)
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
    pub(crate) async fn call<M: JmapMethod<Cap = C>>(
        &self,
        method: M,
    ) -> crate::Result<M::Response> {
        let mut request = self.build();
        let handle = request.call(method)?;
        request.send_single(&handle).await
    }
}

impl<Tr: HttpTransport> Client<Tr> {
    /// Select an explicit account only when the session says it serves C.
    pub(crate) fn account<C: Capability>(
        &self,
        id: impl Into<AccountId>,
    ) -> crate::Result<Account<Tr, C>> {
        let id = id.into();
        crate::core::request::require_capability::<C>(&self.session(), &id)?;
        Ok(Account::selected(self.clone(), id))
    }

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
    pub(crate) fn primary_account<C: Capability>(&self) -> crate::Result<Account<Tr, C>> {
        let session = self.session();
        let account_id = session
            .primary_accounts()
            .find(|(uri, _)| uri.as_str() == C::URI)
            .map(|(_, id)| id.clone())
            .ok_or(crate::Error::NoPrimaryAccount { capability: C::URI })?;

        Ok(Account::selected(self.clone(), account_id))
    }
}
