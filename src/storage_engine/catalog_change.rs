//! Rule and schema changes inside a write program.
//!
//! A [`CatalogChange`] is one statement's edit of a knowledge graph's rule or
//! schema catalog. Under the KG's commit ownership, [`StagedCatalog`] applies a
//! program's edits in statement order to copies of the catalogs, with the same
//! validation as editing them directly, so later statements of the program
//! validate against the edited catalogs. The commit writes the result as
//! [`CatalogEntry`] operations of the program's WAL transaction (the new state of
//! each changed rule and persistent schema), then installs it and saves the
//! catalog files at the commit's revision. Startup replays WAL entries newer than
//! the files ([`KnowledgeGraph::replay_catalog`]).

use super::{validate_names, KnowledgeGraph};
use crate::derived_relations::CompiledRule;
use crate::rule_catalog::{RuleCatalog, RuleDefinition, RuleRegisterResult};
use crate::schema::{RelationSchema, SchemaCatalog};
use crate::statement::{RuleDef, SerializableBodyPred};
use crate::storage::persist::{CatalogEntry, CatalogRecord, Transaction};
use crate::storage::{StorageError, StorageResult};
use std::collections::{BTreeSet, HashSet};
use tracing::warn;

/// One edit of a knowledge graph's rule or schema catalog.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogChange {
    /// Add a clause to a rule, creating the rule if needed.
    RegisterRule(RuleDef),
    /// Remove a rule with all its clauses.
    DropRule(String),
    /// Remove every rule whose name starts with the prefix.
    DropRulesByPrefix(String),
    /// Remove clause `index` (0-based) of rule `name`; the rule goes with its last clause.
    RemoveRuleClause { name: String, index: usize },
    /// Remove every clause of a rule, keeping the rule registered.
    ClearRule(String),
    /// Register a persistent schema; fails if the relation already has one.
    CreateSchema(RelationSchema),
    /// Register or replace a persistent schema.
    DefineSchema(RelationSchema),
    /// Remove a relation's schema: its session schema if it has one, else its
    /// persistent schema.
    RemoveSchema(String),
    /// Register or replace a session schema (kept in memory, never persisted).
    DefineSessionSchema(RelationSchema),
}

/// What a committed [`CatalogChange`] did.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogOutcome {
    RuleRegistered(RuleRegisterResult),
    RuleDropped,
    /// The rules dropped by prefix, sorted.
    RulesDropped(Vec<String>),
    /// Whether removing the clause removed the whole rule.
    ClauseRemoved {
        rule_deleted: bool,
    },
    RuleCleared,
    SchemaDefined,
    /// The schema removed, if the relation had one.
    SchemaRemoved(Option<RelationSchema>),
}

impl CatalogChange {
    /// Whether the change edits rules (otherwise it edits schemas).
    pub fn edits_rules(&self) -> bool {
        matches!(
            self,
            Self::RegisterRule(_)
                | Self::DropRule(_)
                | Self::DropRulesByPrefix(_)
                | Self::RemoveRuleClause { .. }
                | Self::ClearRule(_)
        )
    }

    /// The one relation or rule name the change edits; `None` for prefix drops.
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::RegisterRule(def) => Some(&def.name),
            Self::DropRule(name)
            | Self::ClearRule(name)
            | Self::RemoveSchema(name)
            | Self::RemoveRuleClause { name, .. } => Some(name),
            Self::CreateSchema(schema)
            | Self::DefineSchema(schema)
            | Self::DefineSessionSchema(schema) => Some(&schema.name),
            Self::DropRulesByPrefix(_) => None,
        }
    }

    /// Apply the change to `rules` if it edits rules, with the same
    /// validation and errors as committing it. Schema changes leave `rules`
    /// as they are. Used to stage rule changes for evaluating later
    /// statements of a program on its [`view`](super::WriteProgram::view).
    ///
    /// # Errors
    /// The error the commit would reject the change with.
    pub fn apply_to_rules(&self, rules: &mut RuleCatalog) -> StorageResult<()> {
        if self.edits_rules() {
            self.apply_rule_change(rules)?;
        }
        Ok(())
    }

    fn apply_rule_change(&self, rules: &mut RuleCatalog) -> StorageResult<CatalogOutcome> {
        let failed =
            |action: &str, e: String| StorageError::Other(format!("Failed to {action}: {e}"));
        match self {
            Self::RegisterRule(def) => rules
                .register_rule(def)
                .map(CatalogOutcome::RuleRegistered)
                .map_err(|e| failed("register rule", e)),
            Self::DropRule(name) => rules
                .drop(name)
                .map(|()| CatalogOutcome::RuleDropped)
                .map_err(|e| failed("drop rule", e)),
            Self::DropRulesByPrefix(prefix) => rules
                .drop_by_prefix(prefix)
                .map(CatalogOutcome::RulesDropped)
                .map_err(|e| failed("drop rules by prefix", e)),
            Self::RemoveRuleClause { name, index } => rules
                .remove_rule_clause(name, *index)
                .map(|rule_deleted| CatalogOutcome::ClauseRemoved { rule_deleted })
                .map_err(|e| failed("remove rule clause", e)),
            Self::ClearRule(name) => rules
                .clear_rules(name)
                .map(|()| CatalogOutcome::RuleCleared)
                .map_err(|e| failed("clear rule", e)),
            Self::CreateSchema(_)
            | Self::DefineSchema(_)
            | Self::RemoveSchema(_)
            | Self::DefineSessionSchema(_) => {
                unreachable!("schema change applied as a rule change")
            }
        }
    }

    fn apply_schema_change(
        &self,
        kg: &str,
        schemas: &mut SchemaCatalog,
    ) -> StorageResult<CatalogOutcome> {
        let rejected = |e: crate::schema::catalog::SchemaError| StorageError::Other(e.to_string());
        match self {
            Self::CreateSchema(schema) => {
                validate_names(kg, &schema.name)?;
                schemas.register(schema.clone()).map_err(rejected)?;
            }
            Self::DefineSchema(schema) => {
                validate_names(kg, &schema.name)?;
                schemas
                    .register_or_update(schema.clone())
                    .map_err(rejected)?;
            }
            Self::DefineSessionSchema(schema) => {
                validate_names(kg, &schema.name)?;
                schemas
                    .register_or_update_session(schema.clone())
                    .map_err(rejected)?;
            }
            Self::RemoveSchema(relation) => {
                return Ok(CatalogOutcome::SchemaRemoved(schemas.remove(relation)));
            }
            _ => unreachable!("rule change applied as a schema change"),
        }
        Ok(CatalogOutcome::SchemaDefined)
    }
}

/// A KG's rule and schema catalogs with a program's staged changes applied.
/// Copies are made on the first change of each kind; until then the KG's own
/// catalogs are read.
pub(super) struct StagedCatalog<'a> {
    base_rules: &'a RuleCatalog,
    base_schemas: &'a SchemaCatalog,
    rules: Option<RuleCatalog>,
    schemas: Option<SchemaCatalog>,
    /// Rules and persistent schemas a staged change may have edited.
    touched_rules: BTreeSet<String>,
    touched_schemas: BTreeSet<String>,
}

impl<'a> StagedCatalog<'a> {
    pub fn new(rules: &'a RuleCatalog, schemas: &'a SchemaCatalog) -> Self {
        StagedCatalog {
            base_rules: rules,
            base_schemas: schemas,
            rules: None,
            schemas: None,
            touched_rules: BTreeSet::new(),
            touched_schemas: BTreeSet::new(),
        }
    }

    /// The rules with the changes staged so far.
    pub fn rules(&self) -> &RuleCatalog {
        self.rules.as_ref().unwrap_or(self.base_rules)
    }

    /// The schemas with the changes staged so far.
    pub fn schemas(&self) -> &SchemaCatalog {
        self.schemas.as_ref().unwrap_or(self.base_schemas)
    }

    /// Stage `change` on top of the changes staged so far.
    ///
    /// # Errors
    /// Why the catalog rejects the change; nothing is staged then.
    pub fn apply(&mut self, kg: &str, change: &CatalogChange) -> StorageResult<CatalogOutcome> {
        if change.edits_rules() {
            let base = self.base_rules;
            let rules = self.rules.get_or_insert_with(|| base.detached());
            let outcome = change.apply_rule_change(rules)?;
            match &outcome {
                CatalogOutcome::RulesDropped(names) => {
                    self.touched_rules.extend(names.iter().cloned());
                }
                _ => self.touched_rules.extend(change.name().map(str::to_string)),
            }
            Ok(outcome)
        } else {
            let base = self.base_schemas;
            let schemas = self.schemas.get_or_insert_with(|| base.clone());
            let outcome = change.apply_schema_change(kg, schemas)?;
            self.touched_schemas
                .extend(change.name().map(str::to_string));
            Ok(outcome)
        }
    }

    /// A rule the staged changes leave without clauses while a staged rule
    /// still negates it, with the rules that negate it: the program would
    /// make them fail open. Checked on the program's end state, so a program
    /// may remove a rule together with the rules negating it in any order.
    pub fn emptied_negated_rule(&self) -> Option<(String, Vec<String>)> {
        let staged = self.rules.as_ref()?;
        let has_clauses =
            |rules: &RuleCatalog, name: &str| rules.rule_count(name).is_some_and(|n| n > 0);
        self.touched_rules
            .iter()
            .filter(|name| has_clauses(self.base_rules, name) && !has_clauses(staged, name))
            .find_map(|name| {
                let negating = staged.rules_negating(name);
                (!negating.is_empty()).then(|| (name.clone(), negating))
            })
    }

    /// The net effect of the staged changes.
    pub fn into_delta(self) -> CatalogDelta {
        let rules = match &self.rules {
            Some(staged) => self
                .touched_rules
                .into_iter()
                .filter_map(|name| {
                    let definition = staged.get(&name).cloned();
                    (definition.as_ref() != self.base_rules.get(&name))
                        .then_some((name, definition))
                })
                .collect(),
            None => Vec::new(),
        };
        let schemas = match &self.schemas {
            Some(staged) => self
                .touched_schemas
                .into_iter()
                .filter_map(|relation| {
                    let schema = staged.persistent_schema(&relation).cloned();
                    (schema.as_ref() != self.base_schemas.persistent_schema(&relation))
                        .then_some((relation, schema))
                })
                .collect(),
            None => Vec::new(),
        };
        CatalogDelta {
            rules,
            schemas,
            schema_catalog: self.schemas,
        }
    }
}

/// The net catalog change of a program.
#[derive(Default)]
pub(super) struct CatalogDelta {
    /// Rules whose definition changes, with the new one (`None`: removed).
    rules: Vec<(String, Option<RuleDefinition>)>,
    /// Persistent schemas that change, with the new one (`None`: removed).
    schemas: Vec<(String, Option<RelationSchema>)>,
    /// The staged schema catalog, when the program changed any schema,
    /// persistent or session.
    schema_catalog: Option<SchemaCatalog>,
}

impl CatalogDelta {
    /// The delta that sets rules and persistent schemas to the states
    /// `entries` give (later entries win), leaving out those already in
    /// that state: how a replication follower applies its primary's catalog
    /// changes. Session schemas are kept.
    pub(super) fn replicated(
        rules: &RuleCatalog,
        schemas: &SchemaCatalog,
        entries: impl IntoIterator<Item = CatalogEntry>,
    ) -> Self {
        let mut delta = CatalogDelta::default();
        let mut staged: Option<SchemaCatalog> = None;
        for entry in entries {
            match entry {
                CatalogEntry::Rule { name, definition } => {
                    delta.rules.retain(|(n, _)| *n != name);
                    if rules.get(&name) != definition.as_ref() {
                        delta.rules.push((name, definition));
                    }
                }
                CatalogEntry::Schema { relation, schema } => {
                    delta.schemas.retain(|(r, _)| *r != relation);
                    if schemas.persistent_schema(&relation) != schema.as_ref() {
                        delta.schemas.push((relation, schema));
                    }
                }
            }
        }
        if !delta.schemas.is_empty() {
            let staged = staged.get_or_insert_with(|| schemas.clone());
            for (relation, schema) in &delta.schemas {
                staged.set_persistent(relation, schema.clone());
            }
        }
        delta.schema_catalog = staged;
        delta
    }

    /// Names of the rules the delta changes.
    pub(super) fn rule_names(&self) -> impl Iterator<Item = &str> {
        self.rules.iter().map(|(name, _)| name.as_str())
    }

    /// Names of the relations whose persistent schema the delta changes.
    pub(super) fn schema_names(&self) -> impl Iterator<Item = &str> {
        self.schemas.iter().map(|(relation, _)| relation.as_str())
    }

    /// Whether the delta changes anything that is persisted.
    pub fn is_durable(&self) -> bool {
        !self.rules.is_empty() || !self.schemas.is_empty()
    }

    /// Whether the delta changes anything at all.
    pub fn is_empty(&self) -> bool {
        !self.is_durable() && self.schema_catalog.is_none()
    }

    /// Add the durable changes to `txn` as catalog entries of `kg`.
    pub fn write_to(&self, txn: &mut Transaction, kg: &str) {
        for (name, definition) in &self.rules {
            txn.catalog(
                kg,
                CatalogEntry::Rule {
                    name: name.clone(),
                    definition: definition.clone(),
                },
            );
        }
        for (relation, schema) in &self.schemas {
            txn.catalog(
                kg,
                CatalogEntry::Schema {
                    relation: relation.clone(),
                    schema: schema.clone(),
                },
            );
        }
    }
}

impl KnowledgeGraph {
    /// Install a committed catalog delta. A durable one is saved to the
    /// catalog files at `time`; returns whether that save succeeded (the WAL
    /// keeps the changes until it does). Does not publish.
    pub(super) fn install_catalog(&mut self, delta: CatalogDelta, time: u64) -> bool {
        let durable = delta.is_durable();
        for (name, definition) in delta.rules {
            if let Some(dd) = &self.incremental {
                let compiled = definition.as_ref().map(compile_for_dd);
                let result = dd
                    .remove_rule(&name)
                    .and_then(|()| compiled.map_or(Ok(()), |rule| dd.register_rule(rule)));
                if let Err(e) = result {
                    warn!(rule = %name, error = %e, "incremental_rule_update_failed");
                }
            }
            self.rule_catalog.set(&name, definition);
        }
        if let Some(schemas) = delta.schema_catalog {
            self.schema_catalog = schemas;
        }
        !durable || self.save_catalogs(time)
    }

    /// Save both catalog files as reflecting every change up to `revision`.
    /// Returns whether both saves succeeded.
    fn save_catalogs(&mut self, revision: u64) -> bool {
        self.schema_catalog.advance_revision(revision);
        let results = [
            self.rule_catalog.save_at(revision),
            self.save_schema_catalog(),
        ];
        let mut saved = true;
        for error in results.into_iter().filter_map(Result::err) {
            saved = false;
            warn!(kg = %self.name, revision, error = %error, "catalog_save_failed_wal_keeps_changes");
        }
        self.catalog_unsaved = !saved;
        saved
    }

    /// Apply catalog entries recovered from the WAL that are newer than the
    /// catalog files, and save the files. Returns the newest revision the
    /// files now reflect, or `None` when no entry was for this KG.
    ///
    /// # Errors
    /// Saving the replayed catalogs failed; the WAL still holds the entries.
    pub(super) fn replay_catalog<'r>(
        &mut self,
        records: impl Iterator<Item = &'r CatalogRecord>,
    ) -> Result<Option<u64>, String> {
        let Some(revision) =
            apply_catalog_records(&mut self.rule_catalog, &mut self.schema_catalog, records)
        else {
            return Ok(None);
        };
        self.rule_catalog.save_at(revision)?;
        self.save_schema_catalog()?;
        Ok(Some(revision))
    }
}

/// Apply to `rules` and `schemas`, in memory, the catalog entries recovered
/// from the WAL that are newer than them. Returns the newest entry's revision,
/// which `schemas` now has, or `None` when there was no entry.
pub(super) fn apply_catalog_records<'r>(
    rules: &mut RuleCatalog,
    schemas: &mut SchemaCatalog,
    records: impl Iterator<Item = &'r CatalogRecord>,
) -> Option<u64> {
    let mut newest = None;
    for record in records {
        newest = newest.max(Some(record.revision));
        match &record.entry {
            CatalogEntry::Rule { name, definition } if record.revision > rules.revision() => {
                rules.set(name, definition.clone());
            }
            CatalogEntry::Schema { relation, schema } if record.revision > schemas.revision() => {
                schemas.set_persistent(relation, schema.clone());
            }
            _ => {}
        }
    }
    let revision = newest?;
    schemas.advance_revision(revision);
    Some(revision)
}

/// Compile every clause of a rule into the incremental engine's description
/// of it: its dependencies across all clauses, and whether it is recursive.
fn compile_for_dd(definition: &RuleDefinition) -> CompiledRule {
    let name = definition.name.clone();
    let mut dependencies = HashSet::new();
    let mut is_recursive = false;
    for clause in &definition.rules {
        for pred in &clause.body {
            if let SerializableBodyPred::Atom { relation, .. } = pred {
                if relation == &name {
                    is_recursive = true;
                } else {
                    dependencies.insert(relation.clone());
                }
            }
        }
    }
    let arity = definition.rules.first().map_or(0, |r| r.head_args.len());
    CompiledRule {
        name,
        clauses: vec![], // IR compilation deferred to execution time
        dependencies,
        is_recursive,
        output_schema: (0..arity).map(|i| format!("col{i}")).collect(),
        stratum: 0, // Stratum computed by RuleCatalog
    }
}
