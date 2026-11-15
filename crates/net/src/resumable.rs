//! Provider-independent resumable upload sequencing. No requests are dispatched here.

/// Provider policy for a response that persisted no additional bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NoProgress {
    /// Legal provider response, refused by the client's finite work policy.
    LocalLimit,
    /// The provider promised an advancing offset.
    ContractViolation,
}

/// Refusal after a resumable response or before another chunk attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum UploadRefusal {
    AttemptBudget,
    InvalidWindow,
    NoProgress(NoProgress),
    Regressed,
    BeyondSent,
    IncompleteCompletion,
}

/// The next half-open byte window, with its wire header and commit evidence.
pub struct ChunkWindow {
    pub start: usize,
    pub end: usize,
    pub total: usize,
}

impl ChunkWindow {
    pub fn content_range(&self) -> String {
        format!("bytes {}-{}/{}", self.start, self.end - 1, self.total)
    }

    pub fn is_final(&self) -> bool {
        self.end == self.total
    }

    /// Decode at the provider boundary, then admit the authoritative offset.
    /// Never guess the sent window's end: that would silently skip file bytes.
    pub fn resume_with<E>(
        &self,
        parse: impl FnOnce() -> Result<usize, E>,
        policy: NoProgress,
        classify: impl FnOnce(UploadRefusal) -> E,
    ) -> Result<usize, E> {
        let next = parse()?;
        admit_resume(self.start, self.end, self.total, next, policy).map_err(classify)
    }
}

/// Validate progress independently of the provider's JSON/header syntax.
pub fn admit_resume(
    offset: usize,
    end: usize,
    total: usize,
    next: usize,
    policy: NoProgress,
) -> Result<usize, UploadRefusal> {
    if next < offset {
        return Err(UploadRefusal::Regressed);
    }
    if next > end {
        return Err(UploadRefusal::BeyondSent);
    }
    if next == offset {
        return Err(UploadRefusal::NoProgress(policy));
    }
    if next >= total {
        return Err(UploadRefusal::IncompleteCompletion);
    }
    Ok(next)
}

/// Owns upload windows and finite attempt admission; provider code owns I/O,
/// shutdown, deadlines, decoding and partial-commit records.
///
/// Drive uses a finite chunk-attempt budget in addition to strict progress;
/// OneDrive uses strict progress plus its existing absolute time deadline.
/// Both prevent endless work, without imposing a new size limit on OneDrive.
/// A no-progress refusal retains provider policy explicitly: users need a local
/// retry limit distinguished from a broken provider promise.
pub struct UploadDriver {
    total: usize,
    chunk_size: usize,
    attempts: usize,
    max_attempts: Option<usize>,
}

impl UploadDriver {
    /// Invalid dimensions are refused by `next_window` before dispatch.
    pub fn new(total: usize, chunk_size: usize, max_attempts: Option<usize>) -> Self {
        Self {
            total,
            chunk_size,
            attempts: 0,
            max_attempts,
        }
    }

    pub fn next_window(&mut self, offset: usize) -> Result<ChunkWindow, UploadRefusal> {
        if self.total == 0 || self.chunk_size == 0 || offset >= self.total {
            return Err(UploadRefusal::InvalidWindow);
        }
        if self.max_attempts.is_some_and(|max| self.attempts >= max) {
            return Err(UploadRefusal::AttemptBudget);
        }
        self.attempts = self.attempts.saturating_add(1);
        Ok(ChunkWindow {
            start: offset,
            end: offset.saturating_add(self.chunk_size).min(self.total),
            total: self.total,
        })
    }
}

/// Resume statuses without Location are protocol data; other 3xx are redirects.
/// The request itself must also disable redirect following before dispatch.
pub fn is_redirect(status: u16, has_location: bool, resume_status: Option<u16>) -> bool {
    (300..400).contains(&status) && (has_location || resume_status != Some(status))
}

#[cfg(test)]
mod tests {
    use super::{NoProgress, UploadDriver, UploadRefusal, admit_resume, is_redirect};

    #[test]
    fn partial_acceptance_drives_the_next_window_and_attempt_budget() {
        let mut driver = UploadDriver::new(10, 6, Some(2));
        let first = driver.next_window(0).expect("first attempt");
        assert_eq!(first.content_range(), "bytes 0-5/10");
        assert!(!first.is_final());
        let offset = first
            .resume_with(
                || Ok::<_, UploadRefusal>(4),
                NoProgress::LocalLimit,
                |refusal| refusal,
            )
            .expect("partial acceptance");
        let second = driver.next_window(offset).expect("second attempt");
        assert_eq!(second.content_range(), "bytes 4-9/10");
        assert!(second.is_final());
        assert!(matches!(
            driver.next_window(5),
            Err(UploadRefusal::AttemptBudget)
        ));
    }

    #[test]
    fn offset_admission_keeps_provider_policy_and_rejects_gaps() {
        for policy in [NoProgress::LocalLimit, NoProgress::ContractViolation] {
            assert_eq!(
                admit_resume(4, 8, 10, 4, policy),
                Err(UploadRefusal::NoProgress(policy))
            );
            assert_eq!(
                admit_resume(4, 8, 10, 3, policy),
                Err(UploadRefusal::Regressed)
            );
            assert_eq!(
                admit_resume(4, 8, 10, 9, policy),
                Err(UploadRefusal::BeyondSent)
            );
            assert_eq!(
                admit_resume(4, 10, 10, 10, policy),
                Err(UploadRefusal::IncompleteCompletion)
            );
        }
        let mut driver = UploadDriver::new(usize::MAX, 8, None);
        let window = driver.next_window(usize::MAX - 2).expect("bounded window");
        assert_eq!(window.end, usize::MAX);
        assert!(window.is_final());
    }

    #[test]
    fn only_a_locationless_resume_status_is_not_a_redirect() {
        assert!(!is_redirect(308, false, Some(308)));
        assert!(is_redirect(308, true, Some(308)));
        assert!(is_redirect(307, false, Some(308)));
        assert!(is_redirect(308, false, None));
        assert!(!is_redirect(202, false, None));
    }
}
