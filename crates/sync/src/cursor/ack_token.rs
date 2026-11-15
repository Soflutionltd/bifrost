//! Durable acknowledgement tokens. Keep the authentication key separate from
//! the consumer batch store and retain it across writer and process restarts.

use sha2::{Digest, Sha256};

use super::{
    AckToken, CoverageClaim, DebtLedger, PublicationNumber, PublicationReceipt, UndeliveredMark,
    WalkGeneration, decode_envelope, decode_ledger, encode_envelope, encode_ledger,
};
use crate::Error;

const HEADER: &[u8] = b"bifrost-ack\x01";

/// Encode the whole token beside the consumer transaction it acknowledges.
/// `key` is an application-owned persistent secret, stored separately from the
/// batch store. Use the same key when decoding after a process restart.
/// The tag authenticates storage; this is not an authority boundary against the
/// application holding that key. Token payloads are not encrypted.
///
/// # Panics
/// Panics if a token contains a future checkpoint or ledger variant that its
/// underlying codec cannot encode, or a field exceeds the u32 framing limit.
#[must_use]
pub fn encode_ack_token(token: &AckToken, key: &[u8; 32]) -> Vec<u8> {
    let mut out = HEADER.to_vec();
    out.extend_from_slice(&token.0.0.to_le_bytes());
    out.extend_from_slice(&token.1.claim.generation.to_le_bytes());
    match token.1.walk_watermark {
        None => out.push(0),
        Some(mark) => {
            out.push(1);
            out.extend_from_slice(&mark.0.to_le_bytes());
        }
    }
    match token.checkpoint() {
        None => out.push(0),
        Some(checkpoint) => {
            out.push(1);
            blob(&mut out, &encode_envelope(checkpoint));
        }
    }
    let normalized;
    let reports = match &token.1.replay {
        Some(reports) => reports,
        None => {
            normalized = token
                .1
                .claim
                .reports
                .iter()
                .map(|report| {
                    let mut ledger = DebtLedger::default();
                    // One independent ledger per report preserves both order and
                    // duplicate keys. Barriers are recorded by the walk, not ack.
                    ledger.ingest_report_evidence(report, token.1.claim.generation, 0);
                    ledger
                })
                .collect::<Vec<_>>();
            &normalized
        }
    };
    out.extend_from_slice(
        &u32::try_from(reports.len())
            .expect("ack report count")
            .to_le_bytes(),
    );
    for report in reports {
        blob(&mut out, &encode_ledger(report));
    }
    let tag = authenticate(key, &out);
    out.extend_from_slice(&tag);
    out
}

/// Restore a token without reconstructing coverage reports in application code.
/// Authentication is checked before parsing any cursor or evidence. Invalid,
/// truncated, tampered and trailing data are rejected without ledger mutation.
/// Changing or losing the key makes retained tokens unreadable; retain the key
/// for as long as any batch may still need acknowledgement.
pub fn decode_ack_token(bytes: &[u8], key: &[u8; 32]) -> Result<AckToken, Error> {
    let payload_len = bytes.len().checked_sub(32).ok_or_else(invalid)?;
    let (payload, supplied) = bytes.split_at(payload_len);
    let expected = authenticate(key, payload);
    let difference = supplied
        .iter()
        .zip(expected)
        .fold(0u8, |acc, (a, b)| acc | (a ^ b));
    if difference != 0 {
        return Err(invalid());
    }
    let mut reader = Reader(payload);
    if reader.take(HEADER.len())? != HEADER {
        return Err(Error::SchemaIncompatible);
    }
    let number = PublicationNumber(reader.u64()?);
    let generation = WalkGeneration::restore(reader.u64()?);
    let walk_watermark = match reader.byte()? {
        0 => None,
        1 => Some(UndeliveredMark(reader.u64()?)),
        _ => return Err(invalid()),
    };
    let checkpoint = match reader.byte()? {
        0 => None,
        1 => Some(decode_envelope(reader.blob()?)?),
        _ => return Err(invalid()),
    };
    let count = reader.u32()?;
    let mut reports = Vec::new();
    for _ in 0..count {
        reports.push(decode_ledger(reader.blob()?)?);
    }
    if !reader.0.is_empty() {
        return Err(invalid());
    }
    Ok(AckToken(
        number,
        std::sync::Arc::new(PublicationReceipt {
            checkpoint,
            claim: CoverageClaim {
                reports: Vec::new(),
                generation,
            },
            walk_watermark,
            replay: Some(reports),
        }),
    ))
}

fn blob(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(
        &u32::try_from(bytes.len())
            .expect("ack blob length")
            .to_le_bytes(),
    );
    out.extend_from_slice(bytes);
}

// HMAC-SHA-256 with a fixed 32-byte key, below SHA-256's 64-byte block size.
fn authenticate(key: &[u8; 32], bytes: &[u8]) -> [u8; 32] {
    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for (i, byte) in key.iter().enumerate() {
        inner_pad[i] ^= byte;
        outer_pad[i] ^= byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(bytes);
    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn invalid() -> Error {
    Error::Other("invalid acknowledgement token".into())
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        if len > self.0.len() {
            return Err(invalid());
        }
        let (value, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| invalid())?,
        ))
    }
    fn blob(&mut self) -> Result<&'a [u8], Error> {
        let len = self.u32()? as usize;
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::{ClaimLookup, PendingCoverage};
    use bifrost_types::{
        AccountErrorBuilder, AccountErrorKind, Cause, ChangeCursor, Checkpoint, CoverageDomain,
        CursorScope, DiagnosticText, InventoryCoverageReport, InventoryObligation, ObjectId,
        ObligationKey, OpaqueChangeState, PayloadVersion, ProtocolKind, RequestCause,
        RequestErrorKind, TraitEnvelopeVersion,
    };

    const KEY: [u8; 32] = [7; 32];

    fn checkpoint(state: &[u8]) -> Checkpoint {
        Checkpoint::Change(ChangeCursor {
            scope: CursorScope::Account,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: PayloadVersion::new(1),
                bytes: state.to_vec(),
            },
            advanced_through: None,
            envelope_version: TraitEnvelopeVersion::new(1),
        })
    }

    fn degraded() -> InventoryCoverageReport {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("unrepresentable"),
            }),
        )
        .try_build()
        .expect("valid error");
        InventoryCoverageReport::degraded(
            CoverageDomain::full(CursorScope::Account),
            vec![InventoryObligation::Object {
                key: ObligationKey(b"owed".to_vec()),
                id: ObjectId("object".into()),
                error,
                repair: b"repair".to_vec(),
            }],
        )
    }

    #[test]
    fn restored_reports_preserve_order_debt_diagnostics_and_history() {
        let issuing = PendingCoverage::new();
        let generation = issuing.next_generation();
        let reports = vec![
            degraded(),
            InventoryCoverageReport::complete(CoverageDomain::full(CursorScope::Account)),
            degraded(),
        ];
        let token = issuing.register(
            checkpoint(b"saved"),
            CoverageClaim {
                reports: reports.clone(),
                generation,
            },
        );
        let bytes = encode_ack_token(&token, &KEY);
        let restored = decode_ack_token(&bytes, &KEY).expect("restore token");
        assert_eq!(restored, token);
        assert_eq!(restored.checkpoint(), token.checkpoint());
        assert_eq!(encode_ack_token(&restored, &KEY), bytes);
        // A different owner forces receipt fallback rather than the live claim.
        let restarted = PendingCoverage::new();
        let ClaimLookup::Replay(replayed) =
            restarted.claim_checkpoint(restored, &checkpoint(b"saved"))
        else {
            panic!("restored evidence must replay")
        };
        let mut direct = DebtLedger::default();
        direct.ingest(&degraded(), generation, 1);
        direct.block(&ObligationKey(b"owed".to_vec()));
        let mut replay = direct.clone();
        for report in &reports {
            direct.ingest(report, generation, 99);
        }
        for report in &replayed {
            replay.replay_ack_report(report, 99, true);
        }
        assert_eq!(encode_ledger(&replay), encode_ledger(&direct));
    }

    #[test]
    fn superseded_evidence_is_frozen_into_the_published_restart_receipt() {
        let issuing = PendingCoverage::new();
        let generation = issuing.next_generation();
        issuing.register(
            checkpoint(b"older"),
            CoverageClaim::new(degraded(), generation),
        );
        let token = issuing.register(
            checkpoint(b"newer"),
            CoverageClaim::new(
                InventoryCoverageReport::complete(CoverageDomain::full(CursorScope::Account)),
                generation,
            ),
        );
        let restored = decode_ack_token(&encode_ack_token(&token, &KEY), &KEY).expect("restore");
        let ClaimLookup::Replay(reports) =
            PendingCoverage::new().claim_checkpoint(restored, &checkpoint(b"newer"))
        else {
            panic!("restart replay")
        };
        assert_eq!(reports.len(), 2, "supersession must retain the older debt");
        assert_eq!(reports[0].entries().count(), 1);
        assert_eq!(reports[1].proved().len(), 1);
    }

    #[test]
    fn restored_proof_cannot_escape_a_failed_candidate() {
        let issuing = PendingCoverage::new();
        let generation = issuing.next_generation();
        let token = issuing.register(
            checkpoint(b"saved"),
            CoverageClaim::new(
                InventoryCoverageReport::complete(CoverageDomain::full(CursorScope::Account)),
                generation,
            ),
        );
        let restored = decode_ack_token(&encode_ack_token(&token, &KEY), &KEY).expect("restore");
        let report = &restored.1.replay.as_ref().expect("decoded reports")[0];
        let mut live = DebtLedger::default();
        live.ingest(&degraded(), generation, 1);
        let before = encode_ledger(&live);
        let mut candidate = live.stage();
        live.accrue().replay_ack_debt(report, 2);
        candidate.replay_ack_report(report, 2, true);
        assert!(candidate.completion_permitted(&CursorScope::Account));
        drop(candidate); // Store refusal leaves the candidate uncommitted.
        assert_eq!(encode_ledger(&live), before);
    }

    #[test]
    fn token_rejects_corruption_truncation_trailing_bytes_and_a_wrong_key() {
        let issuing = PendingCoverage::new();
        let token = issuing.publish_without_report(issuing.next_generation());
        let bytes = encode_ack_token(&token, &KEY);
        for end in 0..bytes.len() {
            assert!(decode_ack_token(&bytes[..end], &KEY).is_err());
        }
        for at in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[at] ^= 1;
            assert!(decode_ack_token(&corrupt, &KEY).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_ack_token(&trailing, &KEY).is_err());
        assert!(decode_ack_token(&bytes, &[8; 32]).is_err());
        let restored = decode_ack_token(&bytes, &KEY).expect("repair token");
        assert!(restored.checkpoint().is_none());
        assert!(matches!(
            PendingCoverage::new().claim_repair(restored),
            ClaimLookup::Replay(_)
        ));
    }

    #[test]
    fn checkpoint_claim_requires_the_exact_checkpoint_before_consumption() {
        let owner = PendingCoverage::new();
        let token = owner.register(
            checkpoint(b"saved"),
            CoverageClaim::new(degraded(), owner.next_generation()),
        );
        assert!(matches!(
            owner.claim_checkpoint(token.clone(), &checkpoint(b"other")),
            ClaimLookup::Unknown
        ));
        assert!(matches!(
            owner.claim_checkpoint(token, &checkpoint(b"saved")),
            ClaimLookup::Apply(_)
        ));
    }

    #[test]
    fn hmac_sha256_matches_the_standard_vector() {
        // RFC 4231 case 1, zero padding a 20-byte key does not change HMAC.
        let mut key = [0; 32];
        key[..20].fill(0x0b);
        assert_eq!(
            authenticate(&key, b"Hi There"),
            [
                0xb0, 0x34, 0x4c, 0x61, 0xd8, 0xdb, 0x38, 0x53, 0x5c, 0xa8, 0xaf, 0xce, 0xaf, 0x0b,
                0xf1, 0x2b, 0x88, 0x1d, 0xc2, 0x00, 0xc9, 0x83, 0x3d, 0xa7, 0x26, 0xe9, 0x37, 0x6c,
                0x2e, 0x32, 0xcf, 0xf7,
            ]
        );
    }
}
