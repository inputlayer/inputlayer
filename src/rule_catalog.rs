//! Rule Catalog for Persistent Rules (Policies)
//!
//! Manages persistent rule definitions per database. Rules are defined with `<-`
//! and are automatically loaded on database startup.
//!
//! ## Storage
//!
//! Rules are stored in JSON format at `{db_dir}/rules/catalog.json`
//!
//! ## Example
//!
//! ```rust,no_run
//! use inputlayer::RuleCatalog;
//! use std::path::PathBuf;
//!
//! let db_dir = PathBuf::from("/tmp/mydb");
//! let mut catalog = RuleCatalog::new(db_dir).unwrap();
//!
//! // Get all rules to prepend to queries
//! let rules = catalog.all_rules();
//!
//! // Drop a rule
//! catalog.drop("path").unwrap();
//! ```

use crate::ast::{AggregateFunc, BodyPredicate, Program, Rule};
use crate::recursion::{build_extended_dependency_graph, find_sccs};
use crate::statement::serialize::SerializableTerm;
use crate::statement::{RuleDef, SerializableRule};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Validate a single rule for safety constraints
///
/// This function checks:
/// 1. Self-negation: a rule cannot negate its own head
/// 2. Head variable safety: all head variables must be bound by positive body atoms
/// 3. Range restriction: variables in negated atoms must be bound by positive atoms
///
/// Note: This does NOT check for mutual negation cycles between rules, which requires
/// analyzing the full rule set. Use `validate_rules_stratification` for that.
///
/// # Arguments
/// * `rule` - The rule to validate
/// * `name` - The name/label for error messages
///
/// # Returns
/// * `Ok(())` if the rule is valid
/// * `Err(String)` with a descriptive error message if validation fails
pub fn validate_rule(rule: &Rule, name: &str) -> Result<(), String> {
    // Check 1: Direct self-negation
    for pred in &rule.body {
        if let BodyPredicate::Negated(atom) = pred {
            if atom.relation == rule.head.relation {
                return Err(format!(
                    "Unstratified negation: Rule '{}' negates itself (!{} in body). \
                     Self-negation is not supported.",
                    name, atom.relation
                ));
            }
        }
    }

    // Check 2: Head variable safety - all head variables must be bound by positive body atoms
    let positive_vars = rule.positive_body_variables();
    let head_vars = rule.head.variables();
    let unbound_head: Vec<_> = head_vars.difference(&positive_vars).cloned().collect();
    if !unbound_head.is_empty() {
        let mut sorted_unbound = unbound_head;
        sorted_unbound.sort();
        return Err(format!(
            "Unsafe rule '{}': Head variable(s) {} not bound by any positive body atom. \
             All head variables must appear in at least one positive body predicate.",
            name,
            sorted_unbound.join(", ")
        ));
    }

    // Check 3: Range restriction for negated atoms
    // Variables in negated atoms must be bound by positive atoms
    for pred in &rule.body {
        if let BodyPredicate::Negated(atom) = pred {
            let neg_vars = atom.variables();
            let unbound: Vec<_> = neg_vars.difference(&positive_vars).cloned().collect();
            if !unbound.is_empty() {
                let mut sorted_unbound = unbound;
                sorted_unbound.sort();
                return Err(format!(
                    "Unsafe negation in rule '{}': Variable(s) {} in negated atom !{}(...) \
                     must be bound by a positive body atom. Range restriction violation.",
                    name,
                    sorted_unbound.join(", "),
                    atom.relation
                ));
            }
        }
    }

    Ok(())
}

/// Validate that a set of rules doesn't have negation cycles (stratification check)
///
/// This checks for mutual negation cycles like:
/// - a(X) <- !b(X)
/// - b(X) <- !a(X)
///
/// # Arguments
/// * `rules` - The rules to validate together
///
/// # Returns
/// * `Ok(())` if the rules are stratifiable
/// * `Err(String)` with a descriptive error message if a negation cycle is found
pub fn validate_rules_stratification(rules: &[Rule]) -> Result<(), String> {
    if rules.is_empty() {
        return Ok(());
    }

    let program = Program {
        rules: rules.to_vec(),
    };

    let extended_graph = build_extended_dependency_graph(&program);
    let simple_graph = extended_graph.to_simple_graph();
    let sccs = find_sccs(&simple_graph);

    // Check for any negative edge within any SCC
    for scc in &sccs {
        if let Some((from, to)) = extended_graph.has_negative_edge_in_scc(scc) {
            let reason = if from == to {
                format!(
                    "Unstratified negation: '{from}' negates itself. Self-negation is not supported."
                )
            } else {
                let mut sorted_scc = scc.clone();
                sorted_scc.sort();
                format!(
                    "Unstratified negation: '{}' negates '{}' within a recursive cycle. \
                     Negation through recursion is not supported. Cycle: [{}]",
                    from,
                    to,
                    sorted_scc.join(", ")
                )
            };
            return Err(reason);
        }
    }

    Ok(())
}

/// Extract the aggregation function from a serializable rule's head arguments, if any.
fn extract_head_aggregate(rule: &SerializableRule) -> Option<&AggregateFunc> {
    rule.head_args.iter().find_map(|t| {
        if let SerializableTerm::Aggregate(func, _) = t {
            Some(func)
        } else {
            None
        }
    })
}

/// Validate that a new rule clause has compatible aggregation with existing clauses.
///
/// Ranking aggregates (top_k, top_k_threshold, within_radius) cannot coexist with
/// different parameters on the same rule head, because DD's reduce operator requires
/// consistent ordering semantics. Attempting to add conflicting ranking aggregates
/// causes a panic in DD's merge batcher.
///
/// Simple aggregates (count, sum, min, max, avg) can coexist as separate clauses
/// on the same head, since each produces a single value per group.
fn validate_aggregation_compatibility(
    existing_rules: &[SerializableRule],
    new_rule: &SerializableRule,
) -> Result<(), String> {
    let new_agg = match extract_head_aggregate(new_rule) {
        Some(agg) => agg,
        None => return Ok(()), // No aggregation in new rule, always compatible
    };

    for existing in existing_rules {
        let existing_agg = match extract_head_aggregate(existing) {
            Some(agg) => agg,
            None => continue, // No aggregation in this existing clause
        };

        // Both have ranking aggregates - check for conflicts
        if new_agg.is_ranking() || existing_agg.is_ranking() {
            // Ranking aggregates must match exactly, because DD's reduce
            // operator requires a single consistent ordering across all clauses.
            if !aggregates_are_compatible(existing_agg, new_agg) {
                return Err(format!(
                    "Conflicting aggregation: cannot add clause with '{}' to rule '{}' \
                     which already has a clause with '{}'. \
                     Drop the rule first with '.rule drop {}' and re-create it.",
                    new_agg, new_rule.head_relation, existing_agg, new_rule.head_relation,
                ));
            }
        }

        // For simple aggregates (count, sum, etc.), different types are allowed
        // as separate clauses - they each produce independent results.
    }

    Ok(())
}

/// Validate that a new session rule clause is compatible with existing session rules
/// for the same head relation. Checks arity and aggregation parameter compatibility.
///
/// This is the public API for session rule validation (session rules bypass the rule catalog).
pub fn validate_session_rule_compatibility(
    existing_rules: &[Rule],
    new_rule: &Rule,
) -> Result<(), String> {
    let head_name = &new_rule.head.relation;

    // Find existing rules with the same head relation
    for existing in existing_rules {
        if existing.head.relation != *head_name {
            continue;
        }

        // Check arity
        let existing_arity = existing.head.effective_arity();
        let new_arity = new_rule.head.effective_arity();
        if existing_arity != new_arity {
            return Err(format!(
                "Arity mismatch: session rule '{head_name}' has {existing_arity} argument(s) but new clause has {new_arity}. \
                 Use '.session drop {head_name}' to remove the existing rule first.",
            ));
        }

        // Check aggregation compatibility
        let existing_aggs = existing.head.aggregates();
        let new_aggs = new_rule.head.aggregates();

        if let (Some((existing_agg, _)), Some((new_agg, _))) =
            (existing_aggs.first(), new_aggs.first())
        {
            if (existing_agg.is_ranking() || new_agg.is_ranking())
                && !aggregates_are_compatible(existing_agg, new_agg)
            {
                return Err(format!(
                        "Conflicting aggregation: cannot add clause with '{new_agg}' to session rule '{head_name}' \
                         which already has a clause with '{existing_agg}'. \
                         Use '.session drop {head_name}' to remove the existing rule first.",
                    ));
            }
        }
    }

    Ok(())
}

/// Check if two aggregate functions are compatible for coexistence on the same rule head.
fn aggregates_are_compatible(a: &AggregateFunc, b: &AggregateFunc) -> bool {
    match (a, b) {
        (
            AggregateFunc::TopK {
                k: k1,
                descending: d1,
                ..
            },
            AggregateFunc::TopK {
                k: k2,
                descending: d2,
                ..
            },
        ) => k1 == k2 && d1 == d2,
        (
            AggregateFunc::TopKThreshold {
                k: k1,
                threshold: t1,
                descending: d1,
                ..
            },
            AggregateFunc::TopKThreshold {
                k: k2,
                threshold: t2,
                descending: d2,
                ..
            },
        ) => k1 == k2 && d1 == d2 && (t1 - t2).abs() < f64::EPSILON,
        (
            AggregateFunc::WithinRadius {
                max_distance: d1, ..
            },
            AggregateFunc::WithinRadius {
                max_distance: d2, ..
            },
        ) => (d1 - d2).abs() < f64::EPSILON,
        // Different ranking aggregate types are always incompatible
        _ if a.is_ranking() || b.is_ranking() => false,
        // Simple aggregates are always compatible with each other
        _ => true,
    }
}

/// Result of registering a rule
#[derive(Debug, Clone, PartialEq)]
pub enum RuleRegisterResult {
    /// New rule definition created (first rule)
    Created,
    /// Rule added to existing definition (returns new rule count)
    RuleAdded(usize),
}

/// Rule definition stored in the catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDefinition {
    /// Rule name (relation name)
    pub name: String,
    /// Rules defining this relation (may have multiple rules for recursion)
    pub rules: Vec<SerializableRule>,
    /// When the rule was created
    pub created_at: String,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
}

impl RuleDefinition {
    /// Create a new rule definition
    pub fn new(name: String, rule: SerializableRule) -> Self {
        RuleDefinition {
            name,
            rules: vec![rule],
            created_at: chrono::Utc::now().to_rfc3339(),
            description: None,
        }
    }

    /// Add another rule to this definition (for recursive rules with multiple clauses)
    /// Checks for duplicates before adding
    pub fn add_rule(&mut self, rule: SerializableRule) {
        // Check if this rule already exists (avoid duplicates)
        let rule_str = format!("{rule:?}");
        for existing in &self.rules {
            if format!("{existing:?}") == rule_str {
                return; // Rule already exists, don't add duplicate
            }
        }
        self.rules.push(rule);
    }

    /// Convert all rules to `crate::ast::Rule`
    pub fn to_rules(&self) -> Vec<Rule> {
        self.rules
            .iter()
            .map(super::statement::serialize::SerializableRule::to_rule)
            .collect()
    }

    /// Get a human-readable description of the rule
    pub fn describe(&self) -> String {
        let mut desc = format!("Rule: {}\n", self.name);
        // Note: Timestamp removed for deterministic output in snapshot testing
        if let Some(d) = &self.description {
            desc.push_str(&format!("Description: {d}\n"));
        }
        desc.push_str("Clauses:\n");
        for (i, rule) in self.rules.iter().enumerate() {
            let r = rule.to_rule();
            // Uses Rule's Display implementation
            desc.push_str(&format!("  {}. {r}\n", i + 1));
        }
        desc
    }
}

/// Catalog file format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogFile {
    version: u32,
    rules: HashMap<String, RuleDefinition>,
}

impl Default for CatalogFile {
    fn default() -> Self {
        CatalogFile {
            version: 1,
            rules: HashMap::new(),
        }
    }
}

/// Rule catalog - manages persistent rules per database
#[derive(Debug)]
pub struct RuleCatalog {
    /// Rules indexed by name
    rules: HashMap<String, RuleDefinition>,
    /// Path to the catalog file
    catalog_path: PathBuf,
    /// Whether the catalog has been modified since last save
    dirty: bool,
}

impl RuleCatalog {
    /// Create an empty rule catalog (for error recovery when loading fails)
    pub fn empty() -> Self {
        RuleCatalog {
            rules: HashMap::new(),
            catalog_path: PathBuf::new(),
            dirty: false,
        }
    }

    /// Create a new rule catalog for a database directory
    pub fn new(db_dir: PathBuf) -> Result<Self, String> {
        let rules_dir = db_dir.join("rules");
        let catalog_path = rules_dir.join("catalog.json");

        let mut catalog = RuleCatalog {
            rules: HashMap::new(),
            catalog_path,
            dirty: false,
        };

        // Load existing catalog if present
        if catalog.catalog_path.exists() {
            catalog.load()?;
        }

        Ok(catalog)
    }

    /// Register a rule from a `RuleDef`
    /// Returns information about whether rule was created or updated
    ///
    /// This function performs stratification checking to reject:
    /// - Self-negation: a(X) <- !a(X)
    /// - Mutual negation cycles: a(X) <- !b(X), b(X) <- !a(X)
    /// - Any recursion through negation
    pub fn register_rule(&mut self, rule_def: &RuleDef) -> Result<RuleRegisterResult, String> {
        let name = &rule_def.name;
        let rule = rule_def.rule.clone();
        let ast_rule = rule.to_rule();

        // Validate single-rule safety constraints (self-negation, head safety, range restriction)
        validate_rule(&ast_rule, name)?;

        // Full stratification check against all existing rules plus the new one
        let mut all_rules: Vec<Rule> = Vec::new();
        for rule_def in self.rules.values() {
            all_rules.extend(rule_def.to_rules());
        }
        all_rules.push(ast_rule.clone());

        validate_rules_stratification(&all_rules)?;

        // Validate arity and aggregation compatibility with existing clauses
        if let Some(existing) = self.rules.get(name) {
            if let Some(first_rule) = existing.rules.first() {
                let existing_arity = first_rule.head_args.len();
                let new_arity = rule.head_args.len();
                if existing_arity != new_arity {
                    return Err(format!(
                        "Arity mismatch: rule '{name}' has {existing_arity} argument(s) but new clause has {new_arity}. \
                         Drop the rule first with '.rule drop {name}' and re-create it.",
                    ));
                }
            }
            validate_aggregation_compatibility(&existing.rules, &rule)?;
        }

        // Stratification passed, proceed with registration
        let result = if let Some(existing) = self.rules.get_mut(name) {
            // Check if this is a new clause for an existing rule (recursive case)
            existing.add_rule(rule);
            RuleRegisterResult::RuleAdded(existing.rules.len())
        } else {
            // Create new rule definition
            let definition = RuleDefinition::new(name.clone(), rule);
            self.rules.insert(name.clone(), definition);
            RuleRegisterResult::Created
        };

        self.dirty = true;
        self.save()?;
        Ok(result)
    }

    /// Register a rule from a Rule directly
    pub fn register(&mut self, name: &str, rule: &Rule) -> Result<(), String> {
        let serializable = SerializableRule::from_rule(rule);

        // Validate arity and aggregation compatibility with existing clauses
        if let Some(existing) = self.rules.get(name) {
            if let Some(first_rule) = existing.rules.first() {
                let existing_arity = first_rule.head_args.len();
                let new_arity = serializable.head_args.len();
                if existing_arity != new_arity {
                    return Err(format!(
                        "Arity mismatch: rule '{name}' has {existing_arity} argument(s) but new clause has {new_arity}. \
                         Drop the rule first with '.rule drop {name}' and re-create it.",
                    ));
                }
            }
            validate_aggregation_compatibility(&existing.rules, &serializable)?;
        }

        if let Some(existing) = self.rules.get_mut(name) {
            existing.add_rule(serializable);
        } else {
            let definition = RuleDefinition::new(name.to_string(), serializable);
            self.rules.insert(name.to_string(), definition);
        }

        self.dirty = true;
        self.save()?;
        Ok(())
    }

    /// Drop a rule
    pub fn drop(&mut self, name: &str) -> Result<(), String> {
        if self.rules.remove(name).is_none() {
            return Err(format!("Rule '{name}' does not exist"));
        }

        self.dirty = true;
        self.save()?;
        Ok(())
    }

    /// Clear all clauses from a rule (for editing/redefining)
    /// The rule remains registered but with no clauses, ready for new registration
    pub fn clear_rules(&mut self, name: &str) -> Result<(), String> {
        if let Some(rule_def) = self.rules.get_mut(name) {
            rule_def.rules.clear();
            self.dirty = true;
            self.save()?;
            Ok(())
        } else {
            Err(format!("Rule '{name}' does not exist"))
        }
    }

    /// Replace a specific clause in a rule by index (0-based)
    pub fn replace_rule(
        &mut self,
        name: &str,
        index: usize,
        new_rule: SerializableRule,
    ) -> Result<(), String> {
        if let Some(rule_def) = self.rules.get_mut(name) {
            if index >= rule_def.rules.len() {
                return Err(format!(
                    "Clause index {} out of bounds. Rule '{}' has {} clause(s).",
                    index + 1,
                    name,
                    rule_def.rules.len()
                ));
            }
            rule_def.rules[index] = new_rule;
            self.dirty = true;
            self.save()?;
            Ok(())
        } else {
            Err(format!("Rule '{name}' does not exist"))
        }
    }

    /// Remove a specific clause from a rule by index (0-based)
    /// If the last clause is removed, the entire rule is deleted
    pub fn remove_rule_clause(&mut self, name: &str, index: usize) -> Result<bool, String> {
        if let Some(rule_def) = self.rules.get_mut(name) {
            if index >= rule_def.rules.len() {
                return Err(format!(
                    "Clause index {} out of bounds. Rule '{}' has {} clause(s).",
                    index + 1,
                    name,
                    rule_def.rules.len()
                ));
            }
            rule_def.rules.remove(index);

            // If no clauses remain, remove the entire rule
            let rule_deleted = if rule_def.rules.is_empty() {
                self.rules.remove(name);
                true
            } else {
                false
            };

            self.dirty = true;
            self.save()?;
            Ok(rule_deleted)
        } else {
            Err(format!("Rule '{name}' does not exist"))
        }
    }

    /// Get the number of clauses in a rule
    pub fn rule_count(&self, name: &str) -> Option<usize> {
        self.rules.get(name).map(|r| r.rules.len())
    }

    /// Get the effective output arity of a rule (accounts for ranking aggregate multi-column output)
    pub fn rule_arity(&self, name: &str) -> Option<usize> {
        self.rules.get(name).and_then(|def| {
            def.rules.first().map(|r| {
                let rule = r.to_rule();
                rule.head.effective_arity()
            })
        })
    }

    /// List all rule names
    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.rules.keys().cloned().collect();
        names.sort();
        names
    }

    /// Get a rule definition by name
    pub fn get(&self, name: &str) -> Option<&RuleDefinition> {
        self.rules.get(name)
    }

    /// Get all rules from all definitions (for prepending to queries)
    /// Rules are returned in dependency order (topologically sorted)
    /// so that a rule only appears after all rules it depends on.
    pub fn all_rules(&self) -> Vec<Rule> {
        let all_rules: Vec<Rule> = self
            .rules
            .values()
            .flat_map(RuleDefinition::to_rules)
            .collect();

        // Topologically sort rules by their dependencies
        self.topological_sort_rules(all_rules)
    }

    /// Topologically sort rules so that each rule appears after all rules it depends on.
    /// A rule R1 depends on rule R2 if R1's body contains a predicate that matches R2's head.
    fn topological_sort_rules(&self, rules: Vec<Rule>) -> Vec<Rule> {
        use std::collections::{HashMap, HashSet, VecDeque};

        if rules.is_empty() {
            return rules;
        }

        // Map from head relation name to rule index
        let mut head_to_rules: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, rule) in rules.iter().enumerate() {
            head_to_rules
                .entry(rule.head.relation.clone())
                .or_default()
                .push(i);
        }

        // Build dependency graph: rule_index -> set of rule indices it depends on
        let mut dependencies: Vec<HashSet<usize>> = vec![HashSet::new(); rules.len()];
        let mut dependents: Vec<HashSet<usize>> = vec![HashSet::new(); rules.len()];

        for (i, rule) in rules.iter().enumerate() {
            for pred in &rule.body {
                if let Some(atom) = pred.atom() {
                    let body_relation = &atom.relation;
                    // If this body relation is defined by another rule, add dependency
                    if let Some(def_rule_indices) = head_to_rules.get(body_relation) {
                        for &def_idx in def_rule_indices {
                            if def_idx != i {
                                dependencies[i].insert(def_idx);
                                dependents[def_idx].insert(i);
                            }
                        }
                    }
                }
            }
        }

        // Topological sort via in-degree reduction
        let mut in_degree: Vec<usize> = dependencies
            .iter()
            .map(std::collections::HashSet::len)
            .collect();
        let mut queue: VecDeque<usize> = VecDeque::new();
        let mut result: Vec<Rule> = Vec::with_capacity(rules.len());

        // Start with rules that have no dependencies
        for (i, &degree) in in_degree.iter().enumerate() {
            if degree == 0 {
                queue.push_back(i);
            }
        }

        // Process rules in order
        let mut processed: Vec<bool> = vec![false; rules.len()];
        while let Some(idx) = queue.pop_front() {
            if processed[idx] {
                continue;
            }
            processed[idx] = true;

            // Add rule to result (we need to clone since we're consuming the queue)
            result.push(rules[idx].clone());

            // Update in-degrees of dependents
            for &dep_idx in &dependents[idx] {
                if in_degree[dep_idx] > 0 {
                    in_degree[dep_idx] -= 1;
                    if in_degree[dep_idx] == 0 {
                        queue.push_back(dep_idx);
                    }
                }
            }
        }

        // If there's a cycle (some rules weren't processed), add remaining rules
        // This handles recursive rules which may have cycles
        for (i, rule) in rules.iter().enumerate() {
            if !processed[i] {
                result.push(rule.clone());
            }
        }

        result
    }

    /// Check if a rule exists
    pub fn exists(&self, name: &str) -> bool {
        self.rules.contains_key(name)
    }

    /// Get the number of rules
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Check if the catalog is empty
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Describe a rule
    pub fn describe(&self, name: &str) -> Option<String> {
        self.rules.get(name).map(RuleDefinition::describe)
    }

    /// Load the catalog from disk
    fn load(&mut self) -> Result<(), String> {
        let content = fs::read_to_string(&self.catalog_path)
            .map_err(|e| format!("Failed to read catalog: {e}"))?;

        let catalog_file: CatalogFile =
            serde_json::from_str(&content).map_err(|e| format!("Failed to parse catalog: {e}"))?;

        self.rules = catalog_file.rules;
        self.dirty = false;
        Ok(())
    }

    /// Save the catalog to disk
    pub fn save(&mut self) -> Result<(), String> {
        if !self.dirty {
            return Ok(());
        }

        // Ensure the rules directory exists
        if let Some(parent) = self.catalog_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create rules directory: {e}"))?;
        }

        let catalog_file = CatalogFile {
            version: 1,
            rules: self.rules.clone(),
        };

        let content = serde_json::to_string_pretty(&catalog_file)
            .map_err(|e| format!("Failed to serialize catalog: {e}"))?;

        fs::write(&self.catalog_path, content)
            .map_err(|e| format!("Failed to write catalog: {e}"))?;

        self.dirty = false;
        Ok(())
    }

    /// Force a reload from disk
    pub fn reload(&mut self) -> Result<(), String> {
        if self.catalog_path.exists() {
            self.load()
        } else {
            self.rules.clear();
            self.dirty = false;
            Ok(())
        }
    }

    /// Clear all rules (does not save automatically)
    pub fn clear(&mut self) {
        self.rules.clear();
        self.dirty = true;
    }
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Atom, BodyPredicate, Term};
    use tempfile::TempDir;

    fn make_test_rule(head_rel: &str, body_rel: &str) -> Rule {
        let head = Atom::new(
            head_rel.to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
            ],
        );
        let body = vec![BodyPredicate::Positive(Atom::new(
            body_rel.to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
            ],
        ))];
        Rule::new(head, body)
    }

    #[test]
    fn test_rule_catalog_new() {
        let tmp_dir = TempDir::new().unwrap();
        let catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();
        assert!(catalog.is_empty());
        assert_eq!(catalog.len(), 0);
    }

    #[test]
    fn test_rule_catalog_register() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        assert!(catalog.exists("path"));
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.list(), vec!["path"]);
    }

    #[test]
    fn test_rule_catalog_drop() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        assert!(catalog.exists("path"));
        catalog.drop("path").unwrap();
        assert!(!catalog.exists("path"));
        assert!(catalog.is_empty());
    }

    #[test]
    fn test_rule_catalog_drop_nonexistent() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let result = catalog.drop("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_rule_catalog_multiple_rules() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First rule: path(X, Y) <- edge(X, Y).
        let rule1 = make_test_rule("path", "edge");
        catalog.register("path", &rule1).unwrap();

        // Second rule: path(X, Z) <- edge(X, Y), path(Y, Z).
        let head = Atom::new(
            "path".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Z".to_string()),
            ],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            )),
            BodyPredicate::Positive(Atom::new(
                "path".to_string(),
                vec![
                    Term::Variable("Y".to_string()),
                    Term::Variable("Z".to_string()),
                ],
            )),
        ];
        let rule2 = Rule::new(head, body);
        catalog.register("path", &rule2).unwrap();

        // Should still be one view with two rules
        assert_eq!(catalog.len(), 1);
        let rules = catalog.all_rules();
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn test_rule_catalog_persistence() {
        let tmp_dir = TempDir::new().unwrap();
        let db_path = tmp_dir.path().to_path_buf();

        // Create and populate catalog
        {
            let mut catalog = RuleCatalog::new(db_path.clone()).unwrap();
            let rule = make_test_rule("path", "edge");
            catalog.register("path", &rule).unwrap();
        }

        // Reload and verify
        {
            let catalog = RuleCatalog::new(db_path).unwrap();
            assert!(catalog.exists("path"));
            assert_eq!(catalog.len(), 1);
            let rules = catalog.all_rules();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0].head.relation, "path");
        }
    }

    #[test]
    fn test_rule_catalog_get() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        let view = catalog.get("path").unwrap();
        assert_eq!(view.name, "path");
        assert_eq!(view.rules.len(), 1);

        assert!(catalog.get("nonexistent").is_none());
    }

    #[test]
    fn test_rule_catalog_describe() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        let desc = catalog.describe("path").unwrap();
        assert!(desc.contains("Rule: path"));
        assert!(desc.contains("Clauses:"));
    }

    #[test]
    fn test_rule_catalog_all_rules() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        catalog
            .register("path", &make_test_rule("path", "edge"))
            .unwrap();
        catalog
            .register("reach", &make_test_rule("reach", "source"))
            .unwrap();

        let rules = catalog.all_rules();
        assert_eq!(rules.len(), 2);

        let relations: Vec<_> = rules.iter().map(|r| r.head.relation.as_str()).collect();
        assert!(relations.contains(&"path"));
        assert!(relations.contains(&"reach"));
    }

    #[test]
    fn test_rule_catalog_clear() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        catalog
            .register("path", &make_test_rule("path", "edge"))
            .unwrap();
        catalog
            .register("reach", &make_test_rule("reach", "source"))
            .unwrap();

        assert_eq!(catalog.len(), 2);

        catalog.clear();
        assert!(catalog.is_empty());
    }

    #[test]
    fn test_rule_catalog_clear_rules() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Register view with 2 rules
        let rule1 = make_test_rule("path", "edge");
        catalog.register("path", &rule1).unwrap();

        let head = Atom::new(
            "path".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Z".to_string()),
            ],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            )),
            BodyPredicate::Positive(Atom::new(
                "path".to_string(),
                vec![
                    Term::Variable("Y".to_string()),
                    Term::Variable("Z".to_string()),
                ],
            )),
        ];
        let rule2 = Rule::new(head, body);
        catalog.register("path", &rule2).unwrap();

        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.all_rules().len(), 2);

        // Clear rules
        catalog.clear_rules("path").unwrap();

        // View still exists but has no rules
        assert!(catalog.exists("path"));
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.all_rules().len(), 0);

        // Re-register with new rule
        let new_rule = make_test_rule("path", "new_edge");
        catalog.register("path", &new_rule).unwrap();

        assert_eq!(catalog.all_rules().len(), 1);
        let rules = catalog.all_rules();
        assert_eq!(rules[0].body.len(), 1);
        if let BodyPredicate::Positive(atom) = &rules[0].body[0] {
            assert_eq!(atom.relation, "new_edge");
        } else {
            panic!("Expected positive body predicate");
        }
    }

    #[test]
    fn test_replace_rule() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Register view with 2 rules
        let rule1 = make_test_rule("path", "edge");
        catalog.register("path", &rule1).unwrap();

        let head = Atom::new(
            "path".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Z".to_string()),
            ],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            )),
            BodyPredicate::Positive(Atom::new(
                "path".to_string(),
                vec![
                    Term::Variable("Y".to_string()),
                    Term::Variable("Z".to_string()),
                ],
            )),
        ];
        let rule2 = Rule::new(head, body);
        catalog.register("path", &rule2).unwrap();

        assert_eq!(catalog.all_rules().len(), 2);

        // Replace second rule (index 1) with a new rule
        let new_rule = make_test_rule("path", "new_connection");
        let new_serializable = SerializableRule::from_rule(&new_rule);
        catalog.replace_rule("path", 1, new_serializable).unwrap();

        // Verify the rule was replaced
        let rules = catalog.all_rules();
        assert_eq!(rules.len(), 2);

        // First rule should be unchanged
        if let BodyPredicate::Positive(atom) = &rules[0].body[0] {
            assert_eq!(atom.relation, "edge");
        }

        // Second rule should be the new one
        if let BodyPredicate::Positive(atom) = &rules[1].body[0] {
            assert_eq!(atom.relation, "new_connection");
        }
    }

    #[test]
    fn test_replace_rule_out_of_bounds() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        // Try to replace rule at index 5 when there's only 1 rule
        let new_rule = make_test_rule("path", "new_edge");
        let result = catalog.replace_rule("path", 5, SerializableRule::from_rule(&new_rule));

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("out of bounds"));
    }

    #[test]
    fn test_clear_rules_nonexistent_view() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let result = catalog.clear_rules("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_rule_definition_new() {
        let rule = make_test_rule("path", "edge");
        let serializable = SerializableRule::from_rule(&rule);
        let def = RuleDefinition::new("path".to_string(), serializable);

        assert_eq!(def.name, "path");
        assert_eq!(def.rules.len(), 1);
        assert!(def.description.is_none());
    }

    #[test]
    fn test_register_rule_multiple_rules() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First rule: connected(X, Y) <- edge(X, Y).
        let rule1 = make_test_rule("connected", "edge");
        let rule_def1 = RuleDef {
            name: "connected".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_rule(&rule_def1).unwrap();

        println!("After first register_rule:");
        println!("  Number of rules: {}", catalog.len());
        if let Some(view) = catalog.get("connected") {
            println!("  Rules in 'connected': {}", view.rules.len());
        }

        // Second rule: connected(X, Z) <- edge(X, Y), connected(Y, Z).
        let head = Atom::new(
            "connected".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Z".to_string()),
            ],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            )),
            BodyPredicate::Positive(Atom::new(
                "connected".to_string(),
                vec![
                    Term::Variable("Y".to_string()),
                    Term::Variable("Z".to_string()),
                ],
            )),
        ];
        let rule2 = Rule::new(head, body);
        let rule_def2 = RuleDef {
            name: "connected".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };
        catalog.register_rule(&rule_def2).unwrap();

        println!("After second register_rule:");
        println!("  Number of views: {}", catalog.len());
        if let Some(view) = catalog.get("connected") {
            println!("  Rules in 'connected': {}", view.rules.len());
        }

        // Verify: should have 1 view with 2 rules
        assert_eq!(catalog.len(), 1, "Should have exactly 1 view");
        let rules = catalog.all_rules();
        assert_eq!(rules.len(), 2, "Should have exactly 2 rules total");

        // Check the view has both rules
        let view = catalog
            .get("connected")
            .expect("View 'connected' should exist");
        assert_eq!(view.rules.len(), 2, "View 'connected' should have 2 rules");
    }

    #[test]
    fn test_register_rule_persists_multiple_rules() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let db_path = tmp_dir.path().to_path_buf();

        // Register two rules for 'connected'
        {
            let mut catalog = RuleCatalog::new(db_path.clone()).unwrap();

            // First rule
            let rule1 = make_test_rule("connected", "edge");
            let rule_def1 = RuleDef {
                name: "connected".to_string(),
                rule: SerializableRule::from_rule(&rule1),
            };
            catalog.register_rule(&rule_def1).unwrap();

            // Second rule
            let head = Atom::new(
                "connected".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Z".to_string()),
                ],
            );
            let body = vec![
                BodyPredicate::Positive(Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("X".to_string()),
                        Term::Variable("Y".to_string()),
                    ],
                )),
                BodyPredicate::Positive(Atom::new(
                    "connected".to_string(),
                    vec![
                        Term::Variable("Y".to_string()),
                        Term::Variable("Z".to_string()),
                    ],
                )),
            ];
            let rule2 = Rule::new(head, body);
            let rule_def2 = RuleDef {
                name: "connected".to_string(),
                rule: SerializableRule::from_rule(&rule2),
            };
            catalog.register_rule(&rule_def2).unwrap();

            println!("Before dropping catalog:");
            println!("  Rules count: {}", catalog.all_rules().len());
        }
        // Catalog is dropped here, file should be persisted

        // Reload and verify
        {
            let catalog = RuleCatalog::new(db_path).unwrap();
            println!("After reloading catalog:");
            println!("  Views count: {}", catalog.len());
            println!("  Rules count: {}", catalog.all_rules().len());

            assert_eq!(catalog.len(), 1, "Should have 1 view after reload");
            assert_eq!(
                catalog.all_rules().len(),
                2,
                "Should have 2 rules after reload"
            );

            let view = catalog.get("connected").expect("View should exist");
            assert_eq!(view.rules.len(), 2, "View should have 2 rules after reload");
        }
    }

    #[test]
    fn test_self_negation_rejected() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Self-negation: a(X) <- base(X), !a(X).
        // This should be rejected as unstratified
        let head = Atom::new("a".to_string(), vec![Term::Variable("X".to_string())]);
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "base".to_string(),
                vec![Term::Variable("X".to_string())],
            )),
            BodyPredicate::Negated(Atom::new(
                "a".to_string(),
                vec![Term::Variable("X".to_string())],
            )),
        ];
        let rule = Rule::new(head, body);
        let rule_def = RuleDef {
            name: "a".to_string(),
            rule: SerializableRule::from_rule(&rule),
        };

        let result = catalog.register_rule(&rule_def);
        assert!(result.is_err(), "Self-negation should be rejected");
        let err = result.unwrap_err();
        assert!(
            err.contains("Unstratified") || err.contains("negates itself"),
            "Error message should mention unstratified negation: {}",
            err
        );
    }

    #[test]
    fn test_unsafe_negation_rejected() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Unsafe negation: unsafe_rule(X) <- !banned(X).
        // X only appears in the negated atom, not in any positive atom
        // This should be rejected as unsafe (range restriction violation)
        let head = Atom::new(
            "unsafe_rule".to_string(),
            vec![Term::Variable("X".to_string())],
        );
        let body = vec![BodyPredicate::Negated(Atom::new(
            "banned".to_string(),
            vec![Term::Variable("X".to_string())],
        ))];
        let rule = Rule::new(head, body);
        let rule_def = RuleDef {
            name: "unsafe_rule".to_string(),
            rule: SerializableRule::from_rule(&rule),
        };

        let result = catalog.register_rule(&rule_def);
        assert!(result.is_err(), "Unsafe negation should be rejected");
        let err = result.unwrap_err();
        // Either "Unsafe negation" (range restriction) or "Unsafe rule" (unbound head var) is correct
        // since X only appears in negation and not in any positive body atom
        assert!(
            err.contains("Unsafe negation")
                || err.contains("Unsafe rule")
                || err.contains("range")
                || err.contains("Range")
                || err.contains("not bound"),
            "Error message should mention unsafe rule or unsafe negation: {}",
            err
        );
    }

    #[test]
    fn test_empty_body_unbound_head_rejected() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Empty body rule: foo(X) <- .
        // X is in head but not bound by any positive body atom (no body atoms at all!)
        let head = Atom::new("foo".to_string(), vec![Term::Variable("X".to_string())]);
        let body = vec![]; // Empty body
        let rule = Rule::new(head, body);
        let rule_def = RuleDef {
            name: "foo".to_string(),
            rule: SerializableRule::from_rule(&rule),
        };

        let result = catalog.register_rule(&rule_def);
        assert!(
            result.is_err(),
            "Rule with empty body and unbound head variable should be rejected"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("Unsafe rule") || err.contains("not bound"),
            "Error message should mention unsafe rule: {}",
            err
        );
    }

    #[test]
    fn test_safe_negation_with_bound_variables() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Safe negation: not_banned(X) <- person(X), !banned(X).
        // X is bound by positive atom person(X), so !banned(X) is safe
        let head = Atom::new(
            "not_banned".to_string(),
            vec![Term::Variable("X".to_string())],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "person".to_string(),
                vec![Term::Variable("X".to_string())],
            )),
            BodyPredicate::Negated(Atom::new(
                "banned".to_string(),
                vec![Term::Variable("X".to_string())],
            )),
        ];
        let rule = Rule::new(head, body);
        let rule_def = RuleDef {
            name: "not_banned".to_string(),
            rule: SerializableRule::from_rule(&rule),
        };

        let result = catalog.register_rule(&rule_def);
        assert!(
            result.is_ok(),
            "Safe negation with bound variables should be accepted: {:?}",
            result
        );
    }

    #[test]
    fn test_conflicting_top_k_ordering_rejected() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First rule: result(X, Y, top_k<2, Score, desc>) <- scores(X, Y, Score).
        let head1 = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 2,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Score".to_string()],
                        descending: true,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        let body1 = vec![BodyPredicate::Positive(Atom::new(
            "scores".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
                Term::Variable("Score".to_string()),
            ],
        ))];
        let rule1 = Rule::new(head1, body1);
        let rule_def1 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_rule(&rule_def1).unwrap();

        // Second rule: same head with asc ordering - should be REJECTED
        let head2 = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 2,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Score".to_string()],
                        descending: false,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        let body2 = vec![BodyPredicate::Positive(Atom::new(
            "scores".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
                Term::Variable("Score".to_string()),
            ],
        ))];
        let rule2 = Rule::new(head2, body2);
        let rule_def2 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };

        let result = catalog.register_rule(&rule_def2);
        assert!(
            result.is_err(),
            "Conflicting top_k ordering should be rejected"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("Conflicting aggregation"),
            "Error should mention conflicting aggregation: {err}"
        );
    }

    #[test]
    fn test_conflicting_top_k_different_k_rejected() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First: top_k<2, Score, desc>
        let head1 = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 2,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Score".to_string()],
                        descending: true,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        let body = vec![BodyPredicate::Positive(Atom::new(
            "data".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Score".to_string()),
            ],
        ))];
        let rule1 = Rule::new(head1, body.clone());
        let def1 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_rule(&def1).unwrap();

        // Second: top_k<5, Score, desc> - different k, should be rejected
        let head2 = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 5,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Score".to_string()],
                        descending: true,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        let rule2 = Rule::new(head2, body);
        let def2 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };

        let result = catalog.register_rule(&def2);
        assert!(result.is_err(), "Different k values should be rejected");
    }

    #[test]
    fn test_compatible_top_k_same_params_accepted() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First: top_k<2, Score, desc> from source1
        let head1 = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 2,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Score".to_string()],
                        descending: true,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        let body1 = vec![BodyPredicate::Positive(Atom::new(
            "source1".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Score".to_string()),
            ],
        ))];
        let rule1 = Rule::new(head1, body1);
        let def1 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_rule(&def1).unwrap();

        // Second: top_k<2, Score, desc> from source2 - same params, different body
        let head2 = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 2,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Score".to_string()],
                        descending: true,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        let body2 = vec![BodyPredicate::Positive(Atom::new(
            "source2".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Score".to_string()),
            ],
        ))];
        let rule2 = Rule::new(head2, body2);
        let def2 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };

        let result = catalog.register_rule(&def2);
        assert!(
            result.is_ok(),
            "Compatible top_k with same params should be accepted: {:?}",
            result
        );
    }

    #[test]
    fn test_mixing_ranking_and_simple_agg_rejected() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First: top_k<2, Score, desc>
        let head1 = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Aggregate(
                    AggregateFunc::TopK {
                        k: 2,
                        order_var: "Score".to_string(),
                        output_vars: vec!["Score".to_string()],
                        descending: true,
                    },
                    "Score".to_string(),
                ),
            ],
        );
        let body1 = vec![BodyPredicate::Positive(Atom::new(
            "data".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Score".to_string()),
            ],
        ))];
        let rule1 = Rule::new(head1, body1);
        let def1 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_rule(&def1).unwrap();

        // Second: sum<Score> - mixing ranking with simple agg should be rejected
        let head2 = Atom::new(
            "result".to_string(),
            vec![Term::Aggregate(AggregateFunc::Sum, "Score".to_string())],
        );
        let body2 = vec![BodyPredicate::Positive(Atom::new(
            "data".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Score".to_string()),
            ],
        ))];
        let rule2 = Rule::new(head2, body2);
        let def2 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };

        let result = catalog.register_rule(&def2);
        assert!(
            result.is_err(),
            "Mixing ranking aggregate with simple aggregate should be rejected"
        );
    }

    #[test]
    fn test_different_simple_aggs_accepted() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First: sum<Score>
        let head1 = Atom::new(
            "result".to_string(),
            vec![Term::Aggregate(AggregateFunc::Sum, "V".to_string())],
        );
        let body1 = vec![BodyPredicate::Positive(Atom::new(
            "data".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("V".to_string()),
            ],
        ))];
        let rule1 = Rule::new(head1, body1);
        let def1 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_rule(&def1).unwrap();

        // Second: max<Score> - different simple agg should be accepted
        let head2 = Atom::new(
            "result".to_string(),
            vec![Term::Aggregate(AggregateFunc::Max, "V".to_string())],
        );
        let body2 = vec![BodyPredicate::Positive(Atom::new(
            "data".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("V".to_string()),
            ],
        ))];
        let rule2 = Rule::new(head2, body2);
        let def2 = RuleDef {
            name: "result".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };

        let result = catalog.register_rule(&def2);
        assert!(
            result.is_ok(),
            "Different simple aggregates should be accepted: {:?}",
            result
        );
    }

    #[test]
    fn test_arity_mismatch_rejected() {
        use crate::statement::RuleDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = RuleCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First: path(X, Y) <- edge(X, Y). (arity 2)
        let rule1 = make_test_rule("path", "edge");
        let def1 = RuleDef {
            name: "path".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_rule(&def1).unwrap();

        // Second: path(X, Y, Z) <- triple(X, Y, Z). (arity 3 - mismatch!)
        let head2 = Atom::new(
            "path".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
                Term::Variable("Z".to_string()),
            ],
        );
        let body2 = vec![BodyPredicate::Positive(Atom::new(
            "triple".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
                Term::Variable("Z".to_string()),
            ],
        ))];
        let rule2 = Rule::new(head2, body2);
        let def2 = RuleDef {
            name: "path".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };

        let result = catalog.register_rule(&def2);
        assert!(result.is_err(), "Arity mismatch should be rejected");
        let err = result.unwrap_err();
        assert!(
            err.contains("Arity mismatch"),
            "Error should mention arity mismatch: {err}"
        );
    }
}
