use tracing::debug;

use crate::error::Error;
use crate::types::Command;
use crate::types::response::Capability;

use super::super::dispatch::{CapabilityConsumer, TaggedOkConsumer};
use super::super::{CompressedStream, ImapStream, InnerStream, SessionState};
use super::{ConsumerErased, DriverConsumer, UpgradePayload, event_sink};

/// Execute a stream upgrade atomically.
///
/// Dispatches to the appropriate upgrade handler based on the payload.
/// The driver runs the protocol command internally (using a
/// `TaggedOkConsumer`), then atomically swaps the stream using the
/// `Poisoned` sentinel (I9, I10).
pub(super) async fn run_upgrade(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    payload: UpgradePayload,
) -> Result<(), Error> {
    match payload {
        UpgradePayload::StartTls {
            tls_connector,
            server_name,
            tls_active,
        } => {
            run_starttls_upgrade(
                wire_reader,
                state,
                tag_gen,
                event_sink,
                tls_connector,
                server_name,
                Some(tls_active),
            )
            .await
        }
        UpgradePayload::Compress => {
            run_compress_upgrade(wire_reader, state, tag_gen, event_sink).await
        }
    }
}

/// STARTTLS upgrade (RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1).
///
/// 1. Send STARTTLS, await tagged OK.
/// 2. Verify the wire buffer is empty (B10 fix: no injected bytes).
/// 3. `mem::replace` the reader with a `Poisoned`-stream reader. No
///    `.await` between the buffer check and the replace.
/// 4. TLS handshake (may suspend). If the handshake fails or the
///    future is cancelled, the reader stays wrapping `Poisoned`
///    forever and the connection is dead (I9).
/// 5. Install a fresh `WireReader` on the new TLS stream. The fresh
///    reader has an empty buffer; the old buffer was dropped with
///    the old reader in step 3 (I10).
/// 6. Re-fetch capabilities (RFC 3501 Section6.2.1).
pub(in crate::connection) async fn run_starttls_upgrade(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    tls_connector: native_tls::TlsConnector,
    server_name: String,
    tls_active: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> Result<(), Error> {
    // Step 1: Send STARTTLS command, await tagged OK (RFC 3501 Section6.2.1).
    let consumer =
        DriverConsumer::Regular(Box::new(TaggedOkConsumer::default()) as Box<dyn ConsumerErased>);
    super::run_one_command(
        wire_reader,
        state,
        tag_gen,
        event_sink,
        Command::StartTls,
        consumer,
    )
    .await?;

    // Step 2: Verify buffer is empty BEFORE the swap (B10 fix).
    // RFC 3501 Section6.2.1: After STARTTLS OK, the client MUST discard
    // cached data. Extra bytes here could be injected by a MITM
    // before TLS was established.
    // No .await between this check and the mem::replace below.
    if !wire_reader.buffer_is_empty() {
        let metering = wire_reader.metering();
        *wire_reader =
            super::super::wire::WireReader::with_metering(ImapStream::Poisoned, metering);
        state.apply_infrastructure_failure();
        return Err(Error::Protocol(
            "STARTTLS: unexpected bytes in buffer at upgrade boundary \
             (possible MITM; RFC 3501 Section 6.2.1)"
                .into(),
        ));
    }

    // Step 3: Atomic swap. Replace the reader with one on a Poisoned
    // stream. The old reader is consumed, its buffer is dropped, and
    // we get the old stream back (I10).
    let metering = wire_reader.metering();
    let old_reader = std::mem::replace(
        wire_reader,
        super::super::wire::WireReader::with_metering(ImapStream::Poisoned, metering.clone()),
    );
    let old_stream = old_reader.into_stream();
    let Some(tcp) = old_stream.into_tcp() else {
        // Should be unreachable: the handle validates the stream
        // type before submitting the upgrade. Defensive: leave the
        // connection dead (Poisoned is already installed).
        state.apply_infrastructure_failure();
        // Local invariant after the server's tagged OK: the client's fault,
        // and the stream is already swapped out, so the connection retires.
        return Err(Error::internal_mid_exchange(
            "STARTTLS requires a plain TCP stream (already TLS or compressed)",
            bifrost_types::TransmissionState::Acknowledged,
        ));
    };

    // Step 4: TLS handshake (may suspend). If the handshake fails
    // or the future is cancelled, wire_reader stays wrapping Poisoned
    // forever and the connection is dead (I9).
    let connector = tokio_native_tls::TlsConnector::from(tls_connector);
    let peer_budget = wire_reader.peer_budget();
    let handshake = super::super::wire::peer_io(peer_budget, async {
        connector
            .connect(&server_name, tcp)
            .await
            .map_err(std::io::Error::other)
    });
    let tls_stream = match handshake.await {
        Ok(s) => s,
        Err(error) => {
            // TLS handshake failed; connection is dead (Poisoned stays).
            state.apply_infrastructure_failure();
            return Err(error);
        }
    };

    // Step 5: Install a fresh WireReader on the new TLS stream.
    // The reader has a fresh empty buffer; the old buffer was
    // dropped with old_reader in Step 3 (I10).
    *wire_reader =
        super::super::wire::WireReader::with_metering(ImapStream::Tls(tls_stream), metering);
    if let Some(tls_active) = tls_active {
        // Publish the security boundary at the stream swap, not after the
        // later CAPABILITY round trip. A failed refresh retires the driver,
        // but callers must not observe an installed TLS stream as cleartext.
        tls_active.store(true, std::sync::atomic::Ordering::Release);
    }

    // Step 6: Re-read capabilities after TLS upgrade (RFC 3501 Section6.2.1).
    state.apply_capability_fetch(Vec::new());
    let cap_consumer =
        DriverConsumer::Regular(Box::new(CapabilityConsumer::default()) as Box<dyn ConsumerErased>);
    let result = super::run_one_command(
        wire_reader,
        state,
        tag_gen,
        event_sink,
        Command::Capability,
        cap_consumer,
    )
    .await
    .map_err(|error| post_starttls_capability_failure(state, error))?;
    let caps = result.downcast::<Vec<Capability>>().map_err(|_| {
        // After the TLS swap and a completed post-TLS CAPABILITY: the
        // driver's view of this connection cannot be trusted, so it
        // retires rather than returning to the pool.
        Error::internal_mid_exchange(
            "CapabilityConsumer output downcast failed after STARTTLS",
            bifrost_types::TransmissionState::Acknowledged,
        )
    })?;
    state.apply_capability_fetch(*caps);

    debug!("STARTTLS upgrade complete (RFC 3501 Section 6.2.1)");
    Ok(())
}

/// Once STARTTLS has installed a new stream, returning a reusable connection
/// without the required post-upgrade CAPABILITY result would leave callers
/// with a half-upgraded session. Preserve existing fatal errors, and make a
/// completed server refusal or other non-fatal refresh error retire the stream.
fn post_starttls_capability_failure(
    state: &mut super::super::state::ProtocolState,
    error: Error,
) -> Error {
    // The TLS stream has replaced the cleartext stream. Preserve the actual
    // response error, but mark the session dead so the driver retires it even
    // when that response (such as a tagged NO) is normally reusable.
    state.apply_infrastructure_failure();
    error
}

/// COMPRESS=DEFLATE upgrade (RFC 4978).
///
/// 1. Send COMPRESS, await tagged OK.
/// 2. Take remaining buffer bytes (already compressed data).
/// 3. `mem::replace` the reader with a `Poisoned`-stream reader.
/// 4. Wrap the old stream in a `CompressedStream`.
/// 5. Install a fresh `WireReader` on the compressed stream,
///    preserving any buffered compressed bytes.
async fn run_compress_upgrade(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
) -> Result<(), Error> {
    // Defer the final inbound grant until the plaintext boundary is known.
    wire_reader.begin_compress_boundary();
    let consumer =
        DriverConsumer::Regular(Box::new(TaggedOkConsumer::default()) as Box<dyn ConsumerErased>);
    let result = super::run_one_command(
        wire_reader,
        state,
        tag_gen,
        event_sink,
        Command::Compress,
        consumer,
    )
    .await;
    if let Err(error) = result {
        wire_reader.finish_compress_boundary(0);
        return Err(error);
    }

    // The suffix is compressed framing, below the new admission boundary.
    let remaining = wire_reader.take_buffer();
    wire_reader.finish_compress_boundary(remaining.len());

    // Step 3: Atomic swap with Poisoned sentinel.
    let metering = wire_reader.metering();
    let old_reader = std::mem::replace(
        wire_reader,
        super::super::wire::WireReader::with_metering(ImapStream::Poisoned, metering.clone()),
    );
    let old_stream = old_reader.into_stream();

    // Step 4: Wrap the old stream in a CompressedStream.
    // After the mem::replace above, Poisoned is installed. If any of
    // these error paths fire, the connection is dead. Transition state
    // to Logout so `require_state` rejects subsequent commands cleanly.
    let inner = match old_stream {
        ImapStream::Plain(tcp) => InnerStream::Plain(tcp),
        ImapStream::Tls(tls) => InnerStream::Tls(tls),
        // Local invariants after the server's tagged OK to COMPRESS, with the
        // stream already swapped for the poison sentinel: the client's
        // fault, and the connection retires.
        ImapStream::Compressed(_) => {
            state.apply_infrastructure_failure();
            return Err(Error::internal_mid_exchange(
                "COMPRESS=DEFLATE already active on this connection",
                bifrost_types::TransmissionState::Acknowledged,
            ));
        }
        ImapStream::Poisoned => {
            state.apply_infrastructure_failure();
            return Err(Error::internal_mid_exchange(
                "stream poisoned; connection is dead",
                bifrost_types::TransmissionState::Acknowledged,
            ));
        }
        #[cfg(test)]
        ImapStream::Memory(memory) => InnerStream::Memory(memory),
    };

    // Step 5: Build the CompressedStream and install.
    // RFC 4978 Section3: the server begins compressing immediately after the
    // CRLF ending the tagged OK.
    let mut compressed = CompressedStream::new(inner);
    if !remaining.is_empty() {
        compressed.raw_read_buf.extend_from_slice(&remaining);
    }
    *wire_reader =
        super::super::wire::WireReader::with_metering(ImapStream::Compressed(compressed), metering);

    debug!("COMPRESS=DEFLATE activated (RFC 4978)");
    Ok(())
}

/// Best-effort LOGOUT on graceful shutdown (RFC 3501 Section6.1.3).
///
/// Sends LOGOUT and reads the BYE/OK response. Errors are ignored; the
/// connection is being torn down and the caller has already dropped `cmd_tx`.
pub(super) async fn logout_best_effort(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
) -> Result<(), Error> {
    if state.session_state() == SessionState::Logout {
        return Ok(());
    }
    let tag = tag_gen.next();
    // LOGOUT is a trivial command with no literals; write raw bytes.
    let logout_line = format!("{tag} LOGOUT\r\n");
    wire_reader.write_all(logout_line.as_bytes()).await?;

    // Read responses until the tagged OK or an error. Apply side
    // effects so state transitions to Logout on the BYE/tagged OK.
    loop {
        let utf8 = super::utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            // Accepted deviation: this is the only tagged arm in the crate that
            // does not call `emit_tagged_response_code_events`, so an [ALERT] or
            // [NOTIFICATIONOVERFLOW] code riding the LOGOUT completion is
            // dropped instead of published. It is deliberate and low impact:
            // the event sink is torn down immediately after this function
            // returns, so an event emitted here would very likely never be
            // observed by anyone. If the shutdown path ever grows a drain that
            // flushes pending events to the consumer before the sink closes,
            // this arm should emit like every other tagged arm does.
            crate::types::Response::Tagged(t) if t.tag == tag => break,
            // A foreign tag also ends the drain: nothing is still outstanding
            // that we care about, and the connection is going away.
            crate::types::Response::Tagged(_) => break,
            crate::types::Response::Untagged(u) => {
                // LOGOUT is on the wire. The drain's errors are discarded at
                // teardown, but the phase is stated like every other loop's.
                super::process_untagged_as_event(
                    digest,
                    u,
                    event_sink,
                    bifrost_types::TransmissionState::InFlight,
                )?;
            }
            crate::types::Response::Continuation(_) | crate::types::Response::Greeting(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod bandwidth_review_tests {
    use super::*;
    use crate::connection::{
        state::ProtocolState, stream::Tracked, tag::TagGenerator, wire::WireReader,
    };
    use crate::error::ImapAttempt;
    use bifrost_net::{BandwidthBudget, BandwidthDirection, MeterSink, MeterSinkHandle};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::AsyncWriteExt;
    use tokio::time::{Duration, Instant};

    #[test]
    fn nonfatal_post_starttls_capability_refusal_retires_the_connection() {
        let mut state = ProtocolState::new();
        let error = Error::No {
            text: "CAPABILITY refused".into(),
            code: None,
            attempt: Some(ImapAttempt::new(
                bifrost_types::TransmissionState::Acknowledged,
            )),
        };

        let error = post_starttls_capability_failure(&mut state, error);
        assert!(matches!(error, Error::No { text, .. } if text == "CAPABILITY refused"));
        assert_eq!(state.session_state(), SessionState::Logout);
    }

    #[test]
    fn fatal_post_starttls_error_keeps_its_original_class() {
        let mut state = ProtocolState::new();
        let error = Error::Protocol("malformed post-upgrade response".into());
        let result = post_starttls_capability_failure(&mut state, error);
        assert!(
            matches!(result, Error::Protocol(message) if message == "malformed post-upgrade response")
        );
        assert_eq!(state.session_state(), SessionState::Logout);
    }

    #[derive(Default)]
    struct Counter(AtomicU64);
    impl MeterSink for Counter {
        fn record_bytes_in(&self, _: &bifrost_types::AccountId, n: u64) {
            self.0.fetch_add(n, Ordering::Relaxed);
        }
        fn record_bytes_out(&self, _: &bifrost_types::AccountId, _: u64) {}
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_compress_coalesced_suffix_is_charged_only_as_plaintext() {
        let budget = BandwidthBudget::new(Some(1024));
        let counter = std::sync::Arc::new(Counter::default());
        let sink = MeterSinkHandle::new(
            std::sync::Arc::<Counter>::clone(&counter),
            bifrost_types::AccountId("compress".into()),
        );
        let (client, mut peer) = tokio::io::duplex(4096);
        let mut reader = WireReader::new_metered(
            ImapStream::Memory(Tracked::new(client)),
            Some(sink),
            Some(budget.clone()),
        );
        let mut state = ProtocolState::new();
        let (_, crate::types::Response::Greeting(greeting)) = crate::codec::decode::parse_greeting(
            b"* PREAUTH [CAPABILITY IMAP4rev1 COMPRESS=DEFLATE] ready\r\n",
        )
        .unwrap() else {
            panic!("greeting")
        };
        state.apply_greeting(&greeting).unwrap();
        let mut tags = TagGenerator::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(8);
        let mut events = event_sink::DriverEventSink::new(tx, None);
        let plaintext = b"* 1 EXISTS\r\n";
        let mut compressor = flate2::Compress::new(flate2::Compression::default(), false);
        let mut compressed = [0; 128];
        compressor
            .compress(plaintext, &mut compressed, flate2::FlushCompress::Sync)
            .unwrap();
        let compressed_len = usize::try_from(compressor.total_out()).unwrap();
        let start = Instant::now();
        let script = async {
            let command = crate::connection::test_support::read_line(&mut peer).await;
            let ok = format!(
                "{} OK compressed\r\n",
                crate::connection::test_support::tag_of(&command)
            );
            let mut coalesced = ok.as_bytes().to_vec();
            coalesced.extend_from_slice(&compressed[..compressed_len]);
            // One duplex write makes the tagged OK and suffix available in
            // the same admitted read, whose grant is larger than both.
            peer.write_all(&coalesced).await.unwrap();
            ok.len()
        };
        let (result, ok_len) = tokio::join!(
            run_compress_upgrade(&mut reader, &mut state, &mut tags, &mut events),
            script
        );
        result.unwrap();
        assert_eq!(
            counter.0.load(Ordering::Relaxed),
            u64::try_from(ok_len).unwrap()
        );
        reader.read_one(false).await.unwrap();
        assert_eq!(
            counter.0.load(Ordering::Relaxed),
            u64::try_from(ok_len + plaintext.len()).unwrap()
        );
        let next = budget.reserve(BandwidthDirection::Inbound, 1024);
        let charged = next.ready_at().duration_since(start);
        let expected = Duration::from_nanos(
            u64::try_from((ok_len + plaintext.len()) * 1_000_000_000)
                .unwrap()
                .div_ceil(1024),
        );
        assert!(
            charged.abs_diff(expected) <= Duration::from_nanos(2),
            "charged {charged:?}, expected {expected:?}"
        );
    }
}
