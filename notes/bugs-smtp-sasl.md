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
