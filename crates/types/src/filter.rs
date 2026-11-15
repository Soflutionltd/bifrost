//! Server-side mail filter rules and scripts.
//!
//! Providers expose two different models here. Gmail and Graph expose
//! structured rule objects; JMAP Sieve and IMAP-side Sieve expose
//! literal scripts. The shared `Account` surface carries both shapes
//! without pretending they are interchangeable.

use std::time::SystemTime;

use crate::container::ContainerId;
use crate::error::Provider;

/// Which server-side filter model an account supports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterRuleShape {
    /// No server-side filter primitive is available.
    #[default]
    None,
    /// Typed `FilterRule` CRUD.
    Rules,
    /// Literal `FilterScript` CRUD.
    Scripts,
    /// Both typed rules and literal scripts are available.
    RulesAndScripts,
}

impl FilterRuleShape {
    /// True iff the account can store typed rules.
    #[must_use]
    pub fn supports_rules(self) -> bool {
        matches!(self, Self::Rules | Self::RulesAndScripts)
    }

    /// True iff the account can store literal scripts.
    #[must_use]
    pub fn supports_scripts(self) -> bool {
        matches!(self, Self::Scripts | Self::RulesAndScripts)
    }
}

/// Engine-facing identifier for either a typed rule or a literal script.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServerFilterId(pub String);

/// One server-side filter entry.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ServerFilter {
    Rule(FilterRule),
    Script(FilterScript),
}

impl ServerFilter {
    /// Borrow this entry's id.
    #[must_use]
    pub fn id(&self) -> &ServerFilterId {
        match self {
            Self::Rule(rule) => &rule.id,
            Self::Script(script) => &script.id,
        }
    }
}

/// Create payload for one server-side filter entry.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ServerFilterCreate {
    Rule(FilterRuleCreate),
    Script(FilterScriptCreate),
}

/// Partial update payload for one server-side filter entry.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ServerFilterPatch {
    Rule(FilterRulePatch),
    Script(FilterScriptPatch),
}

/// Typed rule object returned by accounts with `FilterRuleShape::Rules`.
///
/// Not `#[non_exhaustive]` because protocol Account impls and
/// consumers construct values by field name.
#[derive(Debug, Clone)]
pub struct FilterRule {
    pub id: ServerFilterId,
    pub name: Option<String>,
    pub is_enabled: bool,
    pub condition: FilterCondition,
    pub actions: Vec<FilterAction>,
    /// Outlook-style "stop processing more rules" bit. Protocols
    /// without an equivalent preserve `false`.
    pub stop_processing: bool,
}

/// Create payload for a typed rule.
#[derive(Debug, Clone)]
pub struct FilterRuleCreate {
    pub name: Option<String>,
    pub is_enabled: bool,
    pub condition: FilterCondition,
    pub actions: Vec<FilterAction>,
    pub stop_processing: bool,
}

/// Partial update for a typed rule.
#[derive(Debug, Clone, Default)]
pub struct FilterRulePatch {
    pub name: Option<Option<String>>,
    pub is_enabled: Option<bool>,
    pub condition: Option<FilterCondition>,
    pub actions: Option<Vec<FilterAction>>,
    pub stop_processing: Option<bool>,
}

/// Canonical intersection for structured server-side rule conditions.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FilterCondition {
    From(String),
    To(String),
    Cc(String),
    Recipient(String),
    Subject(String),
    Body(String),
    HeaderContains {
        name: String,
        value: String,
    },
    HasAttachment(bool),
    InContainer(ContainerId),
    SizeGreaterThan(u64),
    SizeLessThan(u64),
    DateRange {
        after: Option<SystemTime>,
        before: Option<SystemTime>,
    },
    ProviderExpression {
        provider: Provider,
        expression: String,
    },
    And(Vec<FilterCondition>),
    Or(Vec<FilterCondition>),
    Not(Box<FilterCondition>),
}

/// Canonical intersection for structured server-side rule actions.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FilterAction {
    Keep,
    Discard,
    Delete,
    MoveTo(ContainerId),
    AddLabel(ContainerId),
    RemoveLabel(ContainerId),
    MarkRead,
    MarkUnread,
    Star,
    Unstar,
    SetKeyword(crate::Flag),
    ClearKeyword(crate::Flag),
    ForwardTo(String),
    RedirectTo(String),
    Reject { message: Option<String> },
}

/// Literal server-side filter language.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScriptLanguage {
    Sieve,
}

/// Literal filter script returned by accounts with `FilterRuleShape::Scripts`.
#[derive(Debug, Clone)]
pub struct FilterScript {
    pub id: ServerFilterId,
    pub name: Option<String>,
    pub language: ScriptLanguage,
    pub body: String,
    pub is_active: bool,
}

/// Create payload for a literal filter script.
#[derive(Debug, Clone)]
pub struct FilterScriptCreate {
    pub name: Option<String>,
    pub language: ScriptLanguage,
    pub body: String,
    pub is_active: bool,
}

/// Partial update for a literal filter script.
#[derive(Debug, Clone, Default)]
pub struct FilterScriptPatch {
    pub name: Option<Option<String>>,
    pub body: Option<String>,
    pub is_active: Option<bool>,
}

/// Result of provider-side validation for a rule or script payload.
#[derive(Debug, Clone, Default)]
pub struct FilterValidation {
    pub diagnostics: Vec<FilterDiagnostic>,
}

impl FilterValidation {
    /// Validation succeeded when no error-level diagnostic was returned.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity != FilterDiagnosticSeverity::Error)
    }
}

/// One validation diagnostic.
#[derive(Debug, Clone)]
pub struct FilterDiagnostic {
    pub severity: FilterDiagnosticSeverity,
    pub message: String,
    pub line: Option<u32>,
    pub column: Option<u32>,
}

/// Severity for validation diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shape_helpers_match_variants() {
        assert!(!FilterRuleShape::None.supports_rules());
        assert!(!FilterRuleShape::None.supports_scripts());
        assert!(FilterRuleShape::Rules.supports_rules());
        assert!(!FilterRuleShape::Rules.supports_scripts());
        assert!(!FilterRuleShape::Scripts.supports_rules());
        assert!(FilterRuleShape::Scripts.supports_scripts());
        assert!(FilterRuleShape::RulesAndScripts.supports_rules());
        assert!(FilterRuleShape::RulesAndScripts.supports_scripts());
    }

    #[test]
    fn validation_fails_on_error_diagnostic_only() {
        let warning = FilterValidation {
            diagnostics: vec![FilterDiagnostic {
                severity: FilterDiagnosticSeverity::Warning,
                message: "accepted with warning".to_owned(),
                line: Some(1),
                column: None,
            }],
        };
        assert!(warning.is_valid());

        let error = FilterValidation {
            diagnostics: vec![FilterDiagnostic {
                severity: FilterDiagnosticSeverity::Error,
                message: "syntax error".to_owned(),
                line: Some(2),
                column: Some(4),
            }],
        };
        assert!(!error.is_valid());
    }
}
