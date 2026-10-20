# bifrost-smtp and bifrost-sasl defect findings

1. An entry is removed entirely when completely resolved. No historical record
   stays here.
2. Stable IDs never change and are never reused; removal leaves a gap.
3. An entry adjudicated against, verified incorrect, or whose outcome is that no
   action is taken owes comments at the code sites it names - and, where the
   claim touches a documented contract, the relevant `reference/` or `docs/`
   page - before the entry is removed, so the finding is not hunted again.
4. Once all findings are resolved, the file gets deleted.
5. Finding IDs are never written into the code or other documents. They are
   stable only until this document is drained; the next hunt writes new ones,
   and they are never deduplicated through git history. Carry the context
   inline instead.

Filed from the October 2026 defect hunt: the smtp and sasl scope, plus an SMTP
lateral from net adoption. Severity labels are the hunter's. The IMAP-side
SCRAM continuation defect is IMAP-002.

## SMTP-012 - MAIL parameter gating and duplicate checks are incomplete

Hunter severity: P3.

A caller-supplied `MailParameter::Size` is emitted even when the server does
not advertise SIZE (`validate_mail_parameter` checks only the limit), while
every other extension parameter is gated on advertisement ("Server-advertised
`DSN` is verified before DSN parameters are emitted"). User-supplied MAIL
parameters are also never checked for duplicate keywords (two `RET=`, `BODY=`
beside a user `BODY=`), though the RFC 3461 no-duplicate rule is applied on the
RCPT side.

The hunter's structural note: the send-option rules exist four times.
`mail_options` and `mail_options_for_batch` in `connection.rs` are near
copies (SMTPUTF8, 8BITMIME, SIZE inference, parameter validation), as are
`rcpt_options` and `rcpt_options_for_batch`, and the async connection carries
its own copies of both. `reference/smtp.md` presents the sans-I/O core as the
end of half-to-half drift, but this validation is pure (reads only
`ServerInfo`, `is_encrypted` and inputs) and still lives on the adapters, the
local-restatement shape. Moving `build_envelope` / `build_batch` into `core` (or
a sibling sans-I/O module taking `&ServerInfo`) leaves one copy, and this entry
gets fixed once instead of four times.

## SMTP-003 - The blocking transport cannot tell a busy refresher from one needing refresh

Residue: a stale or empty `OAuthRefresher` can still panic in `tokio::spawn`
on a runtime-less blocking thread, and a momentarily held state lock rejects a
fresh token as "requires a network refresh". SMTP cannot tell them apart through
the opaque `TokenSource` future; a safe fix needs a synchronous cached-token or
readiness API in `bifrost-net`. The one-poll rejection is pinned by a test.

## SMTP-015 - `refresh_failed` in net ignores its `retry_after`

Lateral, in `crates/net/src/account_error.rs`: `refresh_failed` drops its
`retry_after` argument and relies on a hint from the nested source cause, so the
outer deadline is lost when the cause carries none.
