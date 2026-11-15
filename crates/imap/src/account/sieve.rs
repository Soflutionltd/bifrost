//! Account filter-lane adapter for the sibling ManageSieve protocol.

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, FilterScript, FilterValidation, ScriptLanguage,
    ServerFilter, ServerFilterCreate, ServerFilterId, ServerFilterPatch,
};

use super::error::ImapErrorContext;
use super::{ImapAccount, account_error_with};
use crate::Error;
pub use crate::sieve::ManageSieveConfig;
use crate::sieve::{Error as SieveError, ManageSieveClient, validation_error};

pub(crate) fn filters_list(
    account: ImapAccount,
) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
    Box::pin(async move {
        let mut client = open_client(&account, AccountOperation::FiltersList).await?;
        let scripts = client
            .list_scripts()
            .await
            .map_err(to_acct_err(AccountOperation::FiltersList))?;
        let mut filters = Vec::with_capacity(scripts.len());
        for script in scripts {
            let body = client
                .get_script(&script.name)
                .await
                .map_err(to_acct_err(AccountOperation::FiltersList))?;
            filters.push(ServerFilter::Script(FilterScript {
                id: ServerFilterId(script.name.clone()),
                name: Some(script.name),
                language: ScriptLanguage::Sieve,
                body,
                is_active: script.is_active,
            }));
        }
        Ok(filters)
    })
}

pub(crate) fn filter_create(
    account: ImapAccount,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<ServerFilterId, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Script(script) = filter else {
            return Err(super::error::unsupported(AccountOperation::FilterCreate));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Err(invalid(
                AccountOperation::FilterCreate,
                "IMAP ManageSieve only supports Sieve scripts",
            ));
        }
        let name = script.name.ok_or_else(|| {
            invalid(
                AccountOperation::FilterCreate,
                "IMAP ManageSieve script creation requires a name",
            )
        })?;
        let mut client = open_client(&account, AccountOperation::FilterCreate).await?;
        client
            .put_script(&name, &script.body)
            .await
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        if script.is_active {
            client
                .set_active(Some(&name))
                .await
                .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        }
        Ok(ServerFilterId(name))
    })
}

pub(crate) fn filter_update(
    account: ImapAccount,
    filter: ServerFilterId,
    patch: ServerFilterPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ServerFilterPatch::Script(patch) = patch else {
            return Err(super::error::unsupported(AccountOperation::FilterUpdate));
        };
        if patch.name.is_some() {
            return Err(invalid(
                AccountOperation::FilterUpdate,
                "IMAP ManageSieve script names are filter ids and cannot be patched",
            ));
        }
        let mut client = open_client(&account, AccountOperation::FilterUpdate).await?;
        if let Some(body) = patch.body {
            client
                .put_script(&filter.0, &body)
                .await
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        if let Some(is_active) = patch.is_active {
            let active = is_active.then_some(filter.0.as_str());
            client
                .set_active(active)
                .await
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        Ok(())
    })
}

pub(crate) fn filter_delete(
    account: ImapAccount,
    filter: ServerFilterId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mut client = open_client(&account, AccountOperation::FilterDelete).await?;
        client
            .delete_script(&filter.0)
            .await
            .map_err(to_acct_err(AccountOperation::FilterDelete))
    })
}

pub(crate) fn filter_validate(
    account: ImapAccount,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<FilterValidation, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Script(script) = filter else {
            return Ok(validation_error(
                "IMAP ManageSieve only validates literal script filters",
            ));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Ok(validation_error(
                "IMAP ManageSieve only validates Sieve scripts",
            ));
        }
        let mut client = open_client(&account, AccountOperation::FilterValidate).await?;
        client
            .check_script(&script.body)
            .await
            .map_err(to_acct_err(AccountOperation::FilterValidate))
    })
}

async fn open_client(
    account: &ImapAccount,
    op: AccountOperation,
) -> Result<ManageSieveClient, AccountError> {
    let config = account
        .config
        .sieve
        .clone()
        .ok_or_else(|| super::error::unsupported(op))?;
    ManageSieveClient::connect(
        &config,
        &account.config.credentials,
        &account.config.auth_policy,
        account.bandwidth_budget.clone(),
        account.pool.sieve_meter(),
    )
    .await
    .map_err(to_acct_err(op))
}

fn to_acct_err(op: AccountOperation) -> impl Fn(SieveError) -> AccountError {
    move |err| super::error::sieve_account_error(err, ImapErrorContext::operation(op))
}

fn invalid(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    let detail: String = detail.into();
    account_error_with(
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(detail.into())),
        ImapErrorContext::operation(op),
    )
}
