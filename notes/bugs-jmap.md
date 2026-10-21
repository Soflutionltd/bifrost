# bifrost-jmap defect findings

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

Filed from the October 2026 defect hunt: the jmap protocol scope, the jmap
account scope, and the net and sync adoption scopes where they reached JMAP.
Severity labels are the hunters' own.

## JMAP-027 - The WebSocket handshake does not inherit the account proxy

Residue: the handshake now sends trace context through net's public
`trace::current_traceparent()`. It still opens a direct TCP connection, because
reqwest owns implicit proxy selection and `bifrost-net` has no shared proxy
resolver or tunnel connector the handshake could use. Documented at the code
site and in both references.

## JMAP-034 - The PIM send path as a state machine with compensations

**Needs a technical implementation specification** (owner, 2026-10-21),
together with GOOG-027. The push-health-as-state proposal once filed here is
folded into SHARED-014.

The PIM send path as a small state machine with compensations (create email ->
submit -> on failure destroy; cancel -> confirm -> create) instead of single
batched requests whose partial outcomes read as total failure.

## JMAP-037 - Upload size limits do not bound buffering, and may be absent

Laterals from the upload-limit work. `upload_to` now refuses a body over
`maxSizeUpload` before transport, but `sync/pim.rs` `attachment_upload` buffers
the whole input stream first, so the check stops an oversized network upload
without limiting peak memory. And `validate_session` (`sync/capabilities.rs`)
does not require `maxSizeUpload`, so when a server omits it uploads have no
local size ceiling at all.
