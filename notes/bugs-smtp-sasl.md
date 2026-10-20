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

## SMTP-003 - The blocking transport panics on an `OAuthRefresher` needing refresh, and spuriously fails one that does not

Hunter severity: P1.

- `Credentials::oauth2_token_blocking` polls `token_source.current()` once with
  a noop waker. The reference says a source "needing a network refresh is
  rejected".
- `OAuthRefresher::token_inner` (`net/src/auth.rs`) runs
  `self.state.lock().await` (`tokio::sync::Mutex`), and on the Driver path calls
  `tokio::spawn(...)` before its first await. On a plain blocking thread with no
  runtime a stale or empty refresher panics in `tokio::spawn` instead of
  returning the documented rejection.
- If the tokio mutex is momentarily held by a concurrent async reader of the
  same shared source (the ratatoskr shape: one `Arc<dyn TokenSource>` shared
  everywhere), the single poll returns `Pending` even when the token is fresh,
  and the send fails with "requires a network refresh", contradicting "an
  already-fresh `OAuthRefresher` resolves immediately" (`reference/smtp.md`
  "Auth").
- That failure lands as `InvalidInput` plus Auth phase ->
  `Authorization(PolicyBlocked)`, terminal `NeedsPolicyChange`, for a lock race
  (SMTP-005).

## SMTP-004 - SCRAM never uses the GS2 `y` flag, so stripping `-PLUS` from the advertisement goes undetected

Hunter severity: P1.

RFC 5802 s6 makes downgrade protection depend on the GS2 header: a client that
supports channel binding but does not see `-PLUS` advertised must send `y,,`, so
a server that does support binding detects the stripped advertisement.
`ScramChannelBinding` has only `None` (`n,,`) and `TlsServerEndPoint` (`p=...`).
SMTP sends `n,,` whenever the chosen rung is unbound, even over TLS with a
computable binding. The defence both references describe (drop unbound
`SCRAM-SHA-N` whenever `SCRAM-SHA-N-PLUS` is advertised) protects only against
a peer that ADVERTISES PLUS, not one that REMOVES it from EHLO: the client picks
unbound SCRAM with `n,,`, and a TLS-terminating relay can forward the exchange.
`reference/sasl.md` and `reference/smtp.md` both claim RFC 5802 s6 downgrade
protection; half is implemented. Fix in `bifrost-sasl`: a third variant
(`y,,`, `c=eSws`), which applies to IMAP too.

Structural note from the hunter: RFC 5802 s6 handling is a SASL property, not a
protocol one, yet each consumer must remember to drop the unbound rung and
neither can express `y,,`. A `bifrost-sasl` helper taking the advertised set
and binding availability and returning the rung plus a `ScramChannelBinding`
(including `y`) would close it once for IMAP and SMTP, leaving the ladder and
no-ladder difference to callers.

## SMTP-005 - OAuth token-source failures are classified as a terminal policy block

Hunter severity: P2.

`oauth_token_error` maps every `bifrost_net::Error` from `current()` to
`invalid_input(...)` plus Auth, which `account_error.rs` routes to
`Authorization(PolicyBlocked)` (`NeedsPolicyChange`). A transient refresh
failure (network blip, IdP 5xx, backoff) tells the consumer to change policy
instead of retrying, and a revoked refresh token is not reported as `AuthLost`.
The code comment admits the projection "is not refined here", and nothing
downstream refines it, because the `AccountError` is built here. Claims broken:
the error model's Authentication rows (`RefreshTransient` -> Retry, `Revoked` /
`ReauthorizationRequired` -> `AuthLost`) and "the TokenSource failure projection
is the error model's concern", which the model cannot reach once the kind is
`PolicyBlocked`. The blocking `Pending` rejection (SMTP-003) takes the same
lane.

## SMTP-010 - `build_recipient_commands` silently drops recipients on short options

Hunter severity: P3 (latent).

It `zip`s addresses with `rcpt_options`, so a shorter options vector drops
recipients instead of failing. Every current caller builds one entry per
recipient, but this sits on the path whose contract is "validate before MAIL
FROM", and a dropped RCPT is a silent non-delivery.

## SMTP-011 - Duplicate EHLO `SIZE` lines make the size ceiling depend on hash seed

Hunter severity: P3.

`ServerInfo` stores features in a `HashSet<Extension>`, and
`Size(Option<usize>)` / `FutureRelease { .. }` / `DeliverBy(..)` are distinct
values. A server repeating `SIZE` with different limits yields two entries, and
`size_limit()` returns whichever iteration yields first, randomized per
process, so the "hard SIZE ceiling" depends on hash seed. Reject duplicates or
keep the minimum.

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

## SMTP-013 - Smaller smtp and sasl defects

Hunter severity: P3.

- `validate_esmtp_raw_value` (used for `OtherRaw`, HOLDUNTIL and
  `ClientId::domain`) accepts `=`; RFC 5321 `esmtp-value` excludes `%d61`.
- In `build_batch`, a `Mail::new` failure is returned without the `Unsent` /
  `MailFrom` stamps its sibling validation failures get two lines above.
- The pool worker with `min_idle > 0` and `idle_timeout == 0` busy-loops: every
  connection is instantly expired and it sleeps zero between dial rounds.
  Nothing rejects that configuration.
- `channel_binding::read_tlv` accepts non-minimal long-form lengths and
  high-tag-number forms. Harmless today because the whole DER is hashed, but
  looser than the module's "anything it cannot read unambiguously is a hard
  error" policy.
- `crates/smtp/src/transport/smtp/client/metering.rs` `blocking_admit_with_wait`
  compares a Tokio-clock `ready_at().into_std()` against
  `std::time::Instant::now()`. Identical in production, but under paused Tokio
  time the clocks diverge, so the blocking path cannot be tested the way the
  async one is. (From net adoption.)

## SMTP-014 - Document-versus-code disagreements in smtp and sasl

- `reference/smtp.md` "DKIM": "Signing keys: `From<rsa::RsaPrivateKey>` and
  `From<ed25519_dalek::SigningKey>`" and "SHA-2 via `sha2` 0.10 to match `rsa`
  0.9". The code uses `ed25519_compact::KeyPair` (`From<KeyPair> for
  DkimSigningKey`), and `Cargo.toml` pins `rsa = "0.10.0-rc.18"` and
  `ed25519-compact = "2"`. Reword around the trait shape rather than versions.
- `SaslError::AuthFailed` doc: "or server-signature verification failed". The
  code returns `Protocol` for a signature mismatch, as `reference/sasl.md` and
  the tests say. The doc comment is stale.
- `crates/sasl/src/lib.rs` module doc: "(`bifrost-imap`, and later
  `bifrost-smtp`)". SMTP has consumed it for a long time.
- `connection.rs` `resolve_scram_binding` doc: "`None` when the DER is absent or
  `tls_server_end_point` errors (`EdDSA` leaf." Unfinished, and the first half
  contradicts the second (errors propagate and do not become `None`).
- `secret.rs` test comment: "The length difference is folded into the
  accumulator". The impl comment directly above says `subtle` short-circuits on
  a length mismatch; the test comment is wrong.
