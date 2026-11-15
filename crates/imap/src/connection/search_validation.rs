#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // Tests exercise the same admission policy as live and queued commands.
    #[cfg(test)]
    fn validate_search_criteria_capabilities(&self, criteria: &str) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        super::admit_for_encoding(
            &Command::Search {
                criteria: criteria.into(),
            },
            &crate::codec::encode::EncodeOptions {
                utf8_mode: false,
                literal_mode: crate::codec::encode::LiteralMode::LiteralPlus,
                capabilities: snap.capabilities.clone(),
                enabled: snap.enabled.clone(),
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn search_criteria_contains_atom(criteria: &str, atom: &str) -> bool {
        crate::codec::search::contains_atom(criteria, atom)
    }
}

#[cfg(test)]
#[path = "search_validation_tests.rs"]
mod tests;
