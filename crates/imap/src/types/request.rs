//! Validated SELECT and synchronization FETCH request operands.

use super::{FetchAttr, ModSeq, SeqSet, SequenceSet, UidSet, UidValidity};

/// Finite cached UID knowledge. Wildcards and saved-search references are
/// live queries, not a statement of the client's cached mailbox state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KnownUids {
    pub uids: SequenceSet,
    pub seq_match: Option<(SequenceSet, SequenceSet)>,
}

/// QRESYNC operands retain their validated protocol identities until encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QresyncParams {
    pub uid_validity: UidValidity,
    pub mod_seq: ModSeq,
    known: Option<KnownUids>,
}

impl QresyncParams {
    pub fn new(uid_validity: UidValidity, mod_seq: ModSeq) -> Self {
        Self {
            uid_validity,
            mod_seq,
            known: None,
        }
    }

    pub fn with_known(mut self, uids: UidSet) -> Result<Self, crate::Error> {
        let uids = SequenceSet::new_known(uids.to_string())?;
        self.known = Some(KnownUids {
            uids,
            seq_match: None,
        });
        Ok(self)
    }

    pub fn with_seq_match(mut self, seqs: SeqSet, uids: UidSet) -> Result<Self, crate::Error> {
        let known = self.known.as_mut().ok_or_else(|| {
            crate::Error::InvalidInput("QRESYNC seq-match-data requires known-uids".into())
        })?;
        known.seq_match = Some((
            SequenceSet::new_known(seqs.to_string())?,
            SequenceSet::new_known(uids.to_string())?,
        ));
        Ok(self)
    }

    pub fn known(&self) -> Option<&KnownUids> {
        self.known.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn from_raw(
        uidvalidity: u32,
        modseq: u64,
        known: Option<String>,
        seq_match: Option<(String, String)>,
    ) -> Result<Self, crate::Error> {
        let validity = UidValidity::new(uidvalidity).ok_or_else(|| {
            crate::Error::InvalidInput("QRESYNC UIDVALIDITY must be nonzero".into())
        })?;
        let modseq = ModSeq::new(modseq).ok_or_else(|| {
            crate::Error::InvalidInput(
                "QRESYNC MODSEQ must be positive and at most ModSeq::MAX".into(),
            )
        })?;
        let mut params = Self::new(validity, modseq);
        if let Some(known) = known {
            params = params.with_known(UidSet::parse(known)?)?;
        }
        if let Some((seqs, uids)) = seq_match {
            params = params.with_seq_match(SeqSet::parse(seqs)?, UidSet::parse(uids)?)?;
        }
        Ok(params)
    }
}

/// Access mode for SELECT or EXAMINE.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SelectAccess {
    #[default]
    ReadWrite,
    ReadOnly,
}

/// One request shape shared by direct and account SELECT helpers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct SelectOptions {
    pub access: SelectAccess,
    pub condstore: bool,
    pub qresync: Option<QresyncParams>,
}

impl SelectOptions {
    pub fn condstore() -> Self {
        Self {
            condstore: true,
            ..Self::default()
        }
    }
    pub fn qresync(params: QresyncParams) -> Self {
        Self {
            qresync: Some(params),
            ..Self::default()
        }
    }
    pub fn read_write() -> Self {
        Self::condstore()
    }
    pub fn read_only() -> Self {
        Self {
            access: SelectAccess::ReadOnly,
            ..Self::condstore()
        }
    }
    pub fn with_qresync(
        mut self,
        validity: UidValidity,
        modseq: ModSeq,
        known: Option<UidSet>,
    ) -> Result<Self, crate::Error> {
        let mut params = QresyncParams::new(validity, modseq);
        if let Some(known) = known {
            params = params.with_known(known)?;
        }
        self.qresync = Some(params);
        Ok(self)
    }
}

/// Delta modes for UID FETCH.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FetchChanges {
    All,
    Since(ModSeq),
    SinceWithVanished(ModSeq),
}

/// Common fetch shape for mailbox synchronization.
// protocol-specific: direct IMAP sync helpers carry UID FETCH operands.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncFetchRequest {
    /// UID set to fetch.
    pub uids: UidSet,
    /// Attributes to request.
    pub attrs: Vec<FetchAttr>,
    /// Delta mode, with VANISHED structurally requiring CHANGEDSINCE.
    pub changes: FetchChanges,
}

impl SyncFetchRequest {
    /// Fetch flags and UID for all messages.
    pub fn flags() -> Self {
        Self {
            uids: UidSet::all(),
            attrs: vec![FetchAttr::Uid, FetchAttr::Flags, FetchAttr::ModSeq],
            changes: FetchChanges::All,
        }
    }

    /// Fetch envelope-sized metadata for all messages.
    pub fn envelope() -> Self {
        Self {
            uids: UidSet::all(),
            attrs: vec![
                FetchAttr::Uid,
                FetchAttr::Flags,
                FetchAttr::Envelope,
                FetchAttr::InternalDate,
                FetchAttr::Rfc822Size,
                FetchAttr::ModSeq,
            ],
            changes: FetchChanges::All,
        }
    }

    /// Fetch full RFC 5322 message bytes for the given UID set without
    /// setting `\Seen`.
    pub fn full_messages(uids: UidSet) -> Self {
        Self {
            uids,
            attrs: vec![
                FetchAttr::Uid,
                FetchAttr::Flags,
                FetchAttr::Rfc822Size,
                FetchAttr::BodySection {
                    peek: true,
                    section: None,
                    partial: None,
                },
            ],
            changes: FetchChanges::All,
        }
    }

    /// Add CHANGEDSINCE to the request.
    pub fn changed_since(mut self, mod_seq: ModSeq) -> Self {
        self.changes = FetchChanges::Since(mod_seq);
        self
    }

    /// Request VANISHED data for a QRESYNC delta fetch.
    pub fn include_vanished(mut self, mod_seq: ModSeq) -> Self {
        self.changes = FetchChanges::SinceWithVanished(mod_seq);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qresync_params_preserve_seq_match_data() {
        let params = QresyncParams::new(UidValidity::new(77).unwrap(), ModSeq::new(9000).unwrap())
            .with_known(UidSet::parse("10:20").unwrap())
            .unwrap()
            .with_seq_match(
                SeqSet::parse("1:5").unwrap(),
                UidSet::parse("10:14").unwrap(),
            )
            .unwrap();
        assert_eq!(params.uid_validity.get(), 77);
        assert_eq!(params.mod_seq.get(), 9000);
        let known = params.known().unwrap();
        assert_eq!(known.uids.as_str(), "10:20");
        let (seqs, uids) = known.seq_match.as_ref().unwrap();
        assert_eq!(seqs.as_str(), "1:5");
        assert_eq!(uids.as_str(), "10:14");
    }
}
