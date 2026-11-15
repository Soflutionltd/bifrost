//! Per-id wire accounting, independent of the consumer's absent-target policy.

/// Request-owned facts. Never infer a conditional write from its response.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AnswerContext {
    pub(crate) method: &'static str,
    pub(crate) guarded: bool,
    pub(crate) write: bool,
}

/// Every submitted id has one of these four observations. In particular an
/// omission is not a server-declared miss: an Outlook user expects an uncertain
/// write to reconcile rather than disappear as a terminal failure.
#[derive(Debug)]
pub(crate) enum IdAnswer<T> {
    Ok(T),
    Refused(crate::Error),
    /// A declared miss, optionally retaining the provider's typed set refusal.
    NotFound(Option<crate::Error>),
    Unanswered,
}

impl<T> IdAnswer<T> {
    pub(crate) fn from_parts(
        success: Option<T>,
        refusal: Option<crate::Error>,
        not_found: bool,
    ) -> Self {
        if let Some(value) = success {
            Self::Ok(value)
        } else if let Some(error) = refusal {
            if matches!(&error, crate::Error::Set(error) if matches!(error.error_type(), super::set::SetErrorType::NotFound))
            {
                Self::NotFound(Some(error))
            } else {
                Self::Refused(error)
            }
        } else if not_found {
            Self::NotFound(None)
        } else {
            Self::Unanswered
        }
    }

    pub(crate) fn into_result(self, id: &str, context: AnswerContext) -> crate::Result<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Refused(error) => Err(error),
            Self::NotFound(None) => Err(crate::Error::IdNotFound(id.to_owned())),
            Self::NotFound(Some(error)) => Err(error),
            Self::Unanswered => Err(crate::Error::IdUnanswered {
                id: id.to_owned(),
                context,
            }),
        }
    }
}

/// Correlate a bulk read against submitted identities. Unrelated and id-less
/// rows cannot answer a submitted id. First answer wins; repeated submissions
/// receive the same observation rather than consuming each other's result.
pub(crate) fn read_answers<I, T>(
    requested: &[I],
    list: Vec<T>,
    not_found: &[I],
    identity: impl Fn(&T) -> Option<I>,
) -> Vec<(I, IdAnswer<T>)>
where
    I: Clone + Eq + std::hash::Hash,
    T: Clone,
{
    let mut objects = std::collections::HashMap::new();
    for object in list {
        if let Some(id) = identity(&object) {
            objects.entry(id).or_insert(object);
        }
    }
    let missing: std::collections::HashSet<_> = not_found.iter().collect();
    requested
        .iter()
        .map(|id| {
            (
                id.clone(),
                IdAnswer::from_parts(objects.get(id).cloned(), None, missing.contains(id)),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bulk_reads_correlate_submissions_without_consuming_repeated_ids() {
        let answers = read_answers(
            &["wanted", "gone", "silent", "wanted"],
            vec![
                (Some("unrelated"), 0),
                (None, 1),
                (Some("wanted"), 2),
                (Some("wanted"), 3),
            ],
            &["gone"],
            |row| row.0,
        );
        assert_eq!(answers.len(), 4);
        assert!(matches!(answers[0].1, IdAnswer::Ok((Some("wanted"), 2))));
        assert!(matches!(answers[1].1, IdAnswer::NotFound(_)));
        assert!(matches!(answers[2].1, IdAnswer::Unanswered));
        assert!(matches!(answers[3].1, IdAnswer::Ok((Some("wanted"), 2))));
    }

    #[test]
    fn parse_requires_explicit_absence_instead_of_turning_omission_into_not_found() {
        let mut response: crate::core::parse::ParseResponse<String> =
            serde_json::from_value(serde_json::json!({
                "accountId": "a", "parsed": {"ok": "parsed"},
                "notParsable": ["invalid"], "notFound": ["gone"]
            }))
            .unwrap();
        assert_eq!(
            response
                .parsed(&super::super::id::BlobId::new("ok"))
                .unwrap(),
            "parsed"
        );
        assert!(matches!(
            response.parsed(&super::super::id::BlobId::new("invalid")),
            Err(crate::Error::NotParsable(_))
        ));
        assert!(matches!(
            response.parsed(&super::super::id::BlobId::new("gone")),
            Err(crate::Error::IdNotFound(_))
        ));
        assert!(matches!(
            response.parsed(&super::super::id::BlobId::new("silent")),
            Err(crate::Error::IdUnanswered { .. })
        ));
    }
}
