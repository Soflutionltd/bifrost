//! Validated SELECT and synchronization FETCH request operands.

use super::{FetchAttr, ModSeq, SeqSetExpr, SequenceSet, UidSetExpr, UidValidity};

/// Finite cached UID knowledge. Wildcards and saved-search references are
/// live queries, not a statement of the client's cached mailbox state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KnownUids {
    pub uids: super::set::UidSet,
    pub seq_match: Option<(super::set::SeqNumSet, super::set::UidSet)>,
}

fn finite<I: super::set::SetId>(
    expr: &SequenceSet,
) -> Result<super::set::NumberSet<I>, crate::Error> {
    if !expr.is_finite() {
        return Err(crate::Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidInput("QRESYNC knowledge must be finite".into()),
        ));
    }
    super::set::NumberSet::from_expression(expr).ok_or_else(|| {
        crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
            "QRESYNC knowledge must be finite".into(),
        ))
    })
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

    pub fn with_known(mut self, uids: UidSetExpr) -> Result<Self, crate::Error> {
        let uids = finite::<super::Uid>(uids.as_sequence_set())?;
        self.known = Some(KnownUids {
            uids,
            seq_match: None,
        });
        Ok(self)
    }

    pub fn with_seq_match(
        mut self,
        seqs: SeqSetExpr,
        uids: UidSetExpr,
    ) -> Result<Self, crate::Error> {
        let known = self.known.as_mut().ok_or_else(|| {
            crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                "QRESYNC seq-match-data requires known-uids".into(),
            ))
        })?;
        known.seq_match = Some((
            finite::<super::ids::Seq>(seqs.as_sequence_set())?,
            finite::<super::Uid>(uids.as_sequence_set())?,
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
            crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                "QRESYNC UIDVALIDITY must be nonzero".into(),
            ))
        })?;
        let modseq = ModSeq::new(modseq).ok_or_else(|| {
            crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
                "QRESYNC MODSEQ must be positive and at most ModSeq::MAX".into(),
            ))
        })?;
        let mut params = Self::new(validity, modseq);
        if let Some(known) = known {
            params = params.with_known(UidSetExpr::parse(known)?)?;
        }
        if let Some((seqs, uids)) = seq_match {
            params = params.with_seq_match(SeqSetExpr::parse(seqs)?, UidSetExpr::parse(uids)?)?;
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
        known: Option<UidSetExpr>,
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
    pub uids: UidSetExpr,
    /// Attributes to request.
    pub attrs: Vec<FetchAttr>,
    /// Delta mode, with VANISHED structurally requiring CHANGEDSINCE.
    pub changes: FetchChanges,
}

impl SyncFetchRequest {
    /// Fetch flags and UID for all messages.
    pub fn flags() -> Self {
        Self {
            uids: UidSetExpr::all(),
            attrs: vec![FetchAttr::Uid, FetchAttr::Flags, FetchAttr::ModSeq],
            changes: FetchChanges::All,
        }
    }

    /// Fetch envelope-sized metadata for all messages.
    pub fn envelope() -> Self {
        Self {
            uids: UidSetExpr::all(),
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
    pub fn full_messages(uids: UidSetExpr) -> Self {
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
            .with_known(UidSetExpr::parse("10:20").unwrap())
            .unwrap()
            .with_seq_match(
                SeqSetExpr::parse("1:5").unwrap(),
                UidSetExpr::parse("10:14").unwrap(),
            )
            .unwrap();
        assert_eq!(params.uid_validity.get(), 77);
        assert_eq!(params.mod_seq.get(), 9000);
        let known = params.known().unwrap();
        assert_eq!(known.uids.to_string(), "10:20");
        let (seqs, uids) = known.seq_match.as_ref().unwrap();
        assert_eq!(seqs.to_string(), "1:5");
        assert_eq!(uids.to_string(), "10:14");
    }
}
