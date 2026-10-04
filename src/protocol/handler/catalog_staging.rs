//! Resolving one rule or schema statement to the [`CatalogChange`] it makes,
//! and reporting its outcome. Messages, error codes and notifications are the
//! ones these statements always had when they ran on their own.

use super::fact_staging::StageError;
use super::{format_rule_text, storage_error_code};
use crate::protocol::wire::ErrorCode;
use crate::schema::{ColumnSchema, RelationSchema};
use crate::statement::{self, SchemaDecl};
use crate::storage::StorageError;
use crate::storage_engine::{CatalogChange, CatalogOutcome};

/// A statement that changes a rule or schema catalog.
#[derive(Debug)]
pub(super) enum CatalogStatement {
    /// `+name(col: type, ...)` (persistent) or `name(col: type, ...)` (session).
    Schema(SchemaDecl),
    /// `+head(...) <- body`.
    Rule(crate::ast::Rule),
    /// `-name`.
    DropRule(String),
    /// `.rule drop <name>`.
    RuleDrop(String),
    /// `.rule drop prefix <prefix>`.
    RuleDropPrefix(String),
    /// `.rule remove <name> <index>` (`index` 0-based).
    RuleRemove { name: String, index: usize },
    /// `.rule clear <name>`.
    RuleClear(String),
}

impl CatalogStatement {
    /// The catalog change this statement makes.
    pub fn change(&self) -> Result<CatalogChange, StageError> {
        Ok(match self {
            Self::Schema(decl) => {
                let mut schema = RelationSchema::new(&decl.name);
                for col in &decl.columns {
                    schema = schema
                        .with_column(ColumnSchema::new(&col.name, col.col_type.to_schema_type()));
                }
                if decl.persistent {
                    CatalogChange::DefineSchema(schema)
                } else {
                    CatalogChange::DefineSessionSchema(schema)
                }
            }
            Self::Rule(rule) => {
                let rule_def =
                    statement::parse_rule_definition(&format_rule_text(rule)).map_err(|e| {
                        StageError {
                            code: ErrorCode::Validation,
                            message: format!("Failed to parse rule: {e}"),
                        }
                    })?;
                CatalogChange::RegisterRule(rule_def)
            }
            Self::DropRule(name) | Self::RuleDrop(name) => CatalogChange::DropRule(name.clone()),
            Self::RuleDropPrefix(prefix) => CatalogChange::DropRulesByPrefix(prefix.clone()),
            Self::RuleRemove { name, index } => CatalogChange::RemoveRuleClause {
                name: name.clone(),
                index: *index,
            },
            Self::RuleClear(name) => CatalogChange::ClearRule(name.clone()),
        })
    }

    /// Whether the statement changes rules (otherwise a schema).
    pub fn changes_rules(&self) -> bool {
        !matches!(self, Self::Schema(_))
    }

    /// The code and message reporting that this statement failed with `error`.
    pub fn failure(&self, error: &StorageError) -> StageError {
        let (code, message) = match self {
            _ if matches!(error, StorageError::RuleNegated { .. }) => {
                (ErrorCode::Conflict, error.to_string())
            }
            Self::Schema(decl) => (
                storage_error_code(error, ErrorCode::Validation),
                format!("Failed to register schema for '{}': {error}", decl.name),
            ),
            Self::Rule(_) => (
                storage_error_code(error, ErrorCode::Validation),
                error.to_string(),
            ),
            Self::DropRule(name) => (ErrorCode::NotFound, format!("'{name}' not found as rule.")),
            Self::RuleDrop(name) => (
                storage_error_code(error, ErrorCode::NotFound),
                format!("Rule '{name}' not found: {error}"),
            ),
            Self::RuleDropPrefix(_) => (
                storage_error_code(error, ErrorCode::Internal),
                format!("Error: {error}"),
            ),
            Self::RuleRemove { .. } | Self::RuleClear(_) => (
                storage_error_code(error, ErrorCode::NotFound),
                format!("Error: {error}"),
            ),
        };
        StageError { code, message }
    }

    /// The message reporting that this statement committed with `outcome`.
    pub fn success_message(&self, outcome: &CatalogOutcome) -> String {
        match (self, outcome) {
            (Self::Schema(decl), _) => format!(
                "Schema for '{}' registered with {} columns{}",
                decl.name,
                decl.columns.len(),
                if decl.persistent {
                    " (persistent)"
                } else {
                    " (session)"
                }
            ),
            (Self::Rule(rule), _) => format!("Rule '{}' registered.", rule.head.relation),
            (Self::DropRule(name) | Self::RuleDrop(name), _) => {
                format!("Rule '{name}' dropped.")
            }
            (Self::RuleDropPrefix(prefix), CatalogOutcome::RulesDropped(names)) => {
                if names.is_empty() {
                    format!("No rules matching prefix '{prefix}'.")
                } else {
                    format!(
                        "Dropped {} rule(s) with prefix '{prefix}': {}",
                        names.len(),
                        names.join(", ")
                    )
                }
            }
            (
                Self::RuleRemove { name, .. },
                CatalogOutcome::ClauseRemoved { rule_deleted: true },
            ) => {
                format!("Rule '{name}' deleted (last clause removed).")
            }
            (Self::RuleRemove { name, index }, _) => {
                format!("Clause {} removed from rule '{name}'.", index + 1)
            }
            (Self::RuleClear(name), _) => format!("Rule '{name}' cleared."),
            (Self::RuleDropPrefix(_), _) => {
                unreachable!("a prefix drop reports the rules it dropped")
            }
        }
    }

    /// The rule-change notifications (rule, operation) this statement sends
    /// once committed with `outcome`.
    pub fn rule_notices<'a>(&'a self, outcome: &'a CatalogOutcome) -> Vec<(&'a str, &'static str)> {
        match (self, outcome) {
            (Self::Schema(_), _) => Vec::new(),
            (Self::Rule(rule), _) => vec![(rule.head.relation.as_str(), "registered")],
            (Self::DropRule(name) | Self::RuleDrop(name), _) => vec![(name.as_str(), "dropped")],
            (Self::RuleDropPrefix(_), CatalogOutcome::RulesDropped(names)) => names
                .iter()
                .map(|name| (name.as_str(), "dropped"))
                .collect(),
            (Self::RuleDropPrefix(_), _) => Vec::new(),
            (Self::RuleRemove { name, .. } | Self::RuleClear(name), _) => {
                vec![(name.as_str(), "removed")]
            }
        }
    }
}
