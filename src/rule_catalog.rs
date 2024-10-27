//! Rule Catalog for Persistent Rules (Policies)
//!
//! Manages persistent rule definitions per database. Rules are defined with `:-`
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

use crate::ast::{BodyPredicate, Program, Rule};
use crate::recursion::{build_extended_dependency_graph, find_sccs};
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
/// - a(X) :- !b(X)
/// - b(X) :- !a(X)
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
    /// - Self-negation: a(X) :- !a(X)
    /// - Mutual negation cycles: a(X) :- !b(X), b(X) :- !a(X)
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

    /// Get the arity (number of head arguments) of a rule
    pub fn rule_arity(&self, name: &str) -> Option<usize> {
        self.rules.get(name).and_then(|def| {
            def.rules.first().map(|r| {
                let rule = r.to_rule();
                rule.head.args.len()
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

