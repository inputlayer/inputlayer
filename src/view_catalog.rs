//! View Catalog for Persistent Views
//!
//! Manages persistent view definitions per database. Views are defined with `:=`
//! and are automatically loaded on database startup.
//!
//! ## Storage
//!
//! Views are stored in JSON format at `{db_dir}/views/catalog.json`
//!
//! ## Example
//!
//! ```ignore
//! let mut catalog = ViewCatalog::new(db_dir)?;
//!
//! // Register a view
//! catalog.register("path", rule)?;
//!
//! // Get all view rules to prepend to queries
//! let rules = catalog.all_rules();
//!
//! // Drop a view
//! catalog.drop("path")?;
//! ```

use crate::statement::{ViewDef, SerializableRule};
use datalog_ast::Rule;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Result of registering a view
#[derive(Debug, Clone, PartialEq)]
pub enum ViewRegisterResult {
    /// New view created (first rule)
    Created,
    /// Rule added to existing view (returns new rule count)
    RuleAdded(usize),
}

/// View definition stored in the catalog
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// View name (relation name)
    pub name: String,
    /// Rules defining this view (may have multiple rules for the same view)
    pub rules: Vec<SerializableRule>,
    /// When the view was created
    pub created_at: String,
    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
}

impl ViewDefinition {
    /// Create a new view definition
    pub fn new(name: String, rule: SerializableRule) -> Self {
        ViewDefinition {
            name,
            rules: vec![rule],
            created_at: chrono::Utc::now().to_rfc3339(),
            description: None,
        }
    }

    /// Add another rule to this view (for recursive views with multiple rules)
    /// Checks for duplicates before adding
    pub fn add_rule(&mut self, rule: SerializableRule) {
        // Check if this rule already exists (avoid duplicates)
        let rule_str = format!("{:?}", rule);
        for existing in &self.rules {
            if format!("{:?}", existing) == rule_str {
                return; // Rule already exists, don't add duplicate
            }
        }
        self.rules.push(rule);
    }

    /// Convert all rules to datalog_ast::Rule
    pub fn to_rules(&self) -> Vec<Rule> {
        self.rules.iter().map(|r| r.to_rule()).collect()
    }

    /// Get a human-readable description of the view
    pub fn describe(&self) -> String {
        let mut desc = format!("View: {}\n", self.name);
        // Note: Timestamp removed for deterministic output in snapshot testing
        if let Some(d) = &self.description {
            desc.push_str(&format!("Description: {}\n", d));
        }
        desc.push_str("Rules:\n");
        for (i, rule) in self.rules.iter().enumerate() {
            let r = rule.to_rule();
            desc.push_str(&format!("  {}. {}\n", i + 1, format_rule(&r)));
        }
        desc
    }
}

/// Format a Rule as a Datalog string
fn format_rule(rule: &Rule) -> String {
    let head = format_atom(&rule.head);

    if rule.body.is_empty() && rule.constraints.is_empty() {
        return format!("{}.", head);
    }

    let mut body_parts = Vec::new();

    for pred in &rule.body {
        match pred {
            datalog_ast::BodyPredicate::Positive(atom) => {
                body_parts.push(format_atom(atom));
            }
            datalog_ast::BodyPredicate::Negated(atom) => {
                body_parts.push(format!("!{}", format_atom(atom)));
            }
        }
    }

    for constraint in &rule.constraints {
        body_parts.push(format_constraint(constraint));
    }

    format!("{} :- {}.", head, body_parts.join(", "))
}

/// Format an Atom as a Datalog string
fn format_atom(atom: &datalog_ast::Atom) -> String {
    let args: Vec<String> = atom.args.iter().map(format_term).collect();
    format!("{}({})", atom.relation, args.join(", "))
}

/// Format a Term as a Datalog string
fn format_term(term: &datalog_ast::Term) -> String {
    match term {
        datalog_ast::Term::Variable(name) => name.clone(),
        datalog_ast::Term::Constant(val) => val.to_string(),
        datalog_ast::Term::StringConstant(s) => format!("\"{}\"", s),
        datalog_ast::Term::FloatConstant(f) => f.to_string(),
        datalog_ast::Term::Placeholder => "_".to_string(),
        datalog_ast::Term::Aggregate(func, var) => format_aggregate(func, var),
        _ => "_".to_string(),
    }
}

/// Format an AggregateFunc as a Datalog string (e.g., count<X>, sum<Amount>)
fn format_aggregate(func: &datalog_ast::AggregateFunc, var: &str) -> String {
    use datalog_ast::AggregateFunc;
    match func {
        AggregateFunc::Count => format!("count<{}>", var),
        AggregateFunc::Sum => format!("sum<{}>", var),
        AggregateFunc::Min => format!("min<{}>", var),
        AggregateFunc::Max => format!("max<{}>", var),
        AggregateFunc::Avg => format!("avg<{}>", var),
        AggregateFunc::TopK { k, descending, .. } => {
            if *descending {
                format!("top_k<{}, {}, desc>", k, var)
            } else {
                format!("top_k<{}, {}>", k, var)
            }
        }
        AggregateFunc::TopKThreshold { k, threshold, descending, .. } => {
            if *descending {
                format!("top_k_threshold<{}, {}, {}, desc>", k, var, threshold)
            } else {
                format!("top_k_threshold<{}, {}, {}>", k, var, threshold)
            }
        }
        AggregateFunc::WithinRadius { max_distance, .. } => {
            format!("within_radius<{}, {}>", var, max_distance)
        }
    }
}

/// Format a Constraint as a Datalog string
fn format_constraint(constraint: &datalog_ast::Constraint) -> String {
    match constraint {
        datalog_ast::Constraint::Equal(l, r) => format!("{} = {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::NotEqual(l, r) => format!("{} != {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::LessThan(l, r) => format!("{} < {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::LessOrEqual(l, r) => format!("{} <= {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::GreaterThan(l, r) => format!("{} > {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::GreaterOrEqual(l, r) => format!("{} >= {}", format_term(l), format_term(r)),
    }
}

/// Catalog file format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogFile {
    version: u32,
    views: HashMap<String, ViewDefinition>,
}

impl Default for CatalogFile {
    fn default() -> Self {
        CatalogFile {
            version: 1,
            views: HashMap::new(),
        }
    }
}

/// View catalog - manages persistent views per database
#[derive(Debug)]
pub struct ViewCatalog {
    /// Views indexed by name
    views: HashMap<String, ViewDefinition>,
    /// Path to the catalog file
    catalog_path: PathBuf,
    /// Whether the catalog has been modified since last save
    dirty: bool,
}

impl ViewCatalog {
    /// Create a new view catalog for a database directory
    pub fn new(db_dir: PathBuf) -> Result<Self, String> {
        let views_dir = db_dir.join("views");
        let catalog_path = views_dir.join("catalog.json");

        let mut catalog = ViewCatalog {
            views: HashMap::new(),
            catalog_path,
            dirty: false,
        };

        // Load existing catalog if present
        if catalog.catalog_path.exists() {
            catalog.load()?;
        }

        Ok(catalog)
    }

    /// Register a view from a ViewDef
    /// Returns information about whether view was created or updated
    pub fn register_view(&mut self, view_def: &ViewDef) -> Result<ViewRegisterResult, String> {
        let name = &view_def.name;
        let rule = view_def.rule.clone();

        let result = if let Some(existing) = self.views.get_mut(name) {
            // Check if this is a new rule for an existing view (recursive case)
            existing.add_rule(rule);
            ViewRegisterResult::RuleAdded(existing.rules.len())
        } else {
            // Create new view
            let definition = ViewDefinition::new(name.clone(), rule);
            self.views.insert(name.clone(), definition);
            ViewRegisterResult::Created
        };

        self.dirty = true;
        self.save()?;
        Ok(result)
    }

    /// Register a view from a Rule directly
    pub fn register(&mut self, name: &str, rule: &Rule) -> Result<(), String> {
        let serializable = SerializableRule::from_rule(rule);

        if let Some(existing) = self.views.get_mut(name) {
            existing.add_rule(serializable);
        } else {
            let definition = ViewDefinition::new(name.to_string(), serializable);
            self.views.insert(name.to_string(), definition);
        }

        self.dirty = true;
        self.save()?;
        Ok(())
    }

    /// Drop a view
    pub fn drop(&mut self, name: &str) -> Result<(), String> {
        if self.views.remove(name).is_none() {
            return Err(format!("View '{}' does not exist", name));
        }

        self.dirty = true;
        self.save()?;
        Ok(())
    }

    /// Clear all rules from a view (for editing/redefining)
    /// The view remains registered but with no rules, ready for new rule registration
    pub fn clear_rules(&mut self, name: &str) -> Result<(), String> {
        if let Some(view) = self.views.get_mut(name) {
            view.rules.clear();
            self.dirty = true;
            self.save()?;
            Ok(())
        } else {
            Err(format!("View '{}' does not exist", name))
        }
    }

    /// Replace a specific rule in a view by index (0-based)
    pub fn replace_rule(&mut self, name: &str, index: usize, new_rule: SerializableRule) -> Result<(), String> {
        if let Some(view) = self.views.get_mut(name) {
            if index >= view.rules.len() {
                return Err(format!(
                    "Rule index {} out of bounds. View '{}' has {} rule(s).",
                    index + 1, name, view.rules.len()
                ));
            }
            view.rules[index] = new_rule;
            self.dirty = true;
            self.save()?;
            Ok(())
        } else {
            Err(format!("View '{}' does not exist", name))
        }
    }

    /// Get the number of rules in a view
    pub fn rule_count(&self, name: &str) -> Option<usize> {
        self.views.get(name).map(|v| v.rules.len())
    }

    /// List all view names
    pub fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self.views.keys().cloned().collect();
        names.sort();
        names
    }

    /// Get a view definition by name
    pub fn get(&self, name: &str) -> Option<&ViewDefinition> {
        self.views.get(name)
    }

    /// Get all rules from all views (for prepending to queries)
    /// Rules are returned in dependency order (topologically sorted)
    /// so that a rule only appears after all rules it depends on.
    pub fn all_rules(&self) -> Vec<Rule> {
        let all_rules: Vec<Rule> = self.views
            .values()
            .flat_map(|def| def.to_rules())
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
                let body_relation = &pred.atom().relation;
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

        // Kahn's algorithm for topological sort
        let mut in_degree: Vec<usize> = dependencies.iter().map(|deps| deps.len()).collect();
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

    /// Check if a view exists
    pub fn exists(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    /// Get the number of views
    pub fn len(&self) -> usize {
        self.views.len()
    }

    /// Check if the catalog is empty
    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// Describe a view
    pub fn describe(&self, name: &str) -> Option<String> {
        self.views.get(name).map(|v| v.describe())
    }

    /// Load the catalog from disk
    fn load(&mut self) -> Result<(), String> {
        let content = fs::read_to_string(&self.catalog_path)
            .map_err(|e| format!("Failed to read catalog: {}", e))?;

        let catalog_file: CatalogFile = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse catalog: {}", e))?;

        self.views = catalog_file.views;
        self.dirty = false;
        Ok(())
    }

    /// Save the catalog to disk
    pub fn save(&mut self) -> Result<(), String> {
        if !self.dirty {
            return Ok(());
        }

        // Ensure the views directory exists
        if let Some(parent) = self.catalog_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create views directory: {}", e))?;
        }

        let catalog_file = CatalogFile {
            version: 1,
            views: self.views.clone(),
        };

        let content = serde_json::to_string_pretty(&catalog_file)
            .map_err(|e| format!("Failed to serialize catalog: {}", e))?;

        fs::write(&self.catalog_path, content)
            .map_err(|e| format!("Failed to write catalog: {}", e))?;

        self.dirty = false;
        Ok(())
    }

    /// Force a reload from disk
    pub fn reload(&mut self) -> Result<(), String> {
        if self.catalog_path.exists() {
            self.load()
        } else {
            self.views.clear();
            self.dirty = false;
            Ok(())
        }
    }

    /// Clear all views (does not save automatically)
    pub fn clear(&mut self) {
        self.views.clear();
        self.dirty = true;
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use datalog_ast::{Atom, Term, BodyPredicate};

    fn make_test_rule(head_rel: &str, body_rel: &str) -> Rule {
        let head = Atom::new(
            head_rel.to_string(),
            vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
        );
        let body = vec![BodyPredicate::Positive(Atom::new(
            body_rel.to_string(),
            vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
        ))];
        Rule::new(head, body, vec![])
    }

    #[test]
    fn test_view_catalog_new() {
        let tmp_dir = TempDir::new().unwrap();
        let catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();
        assert!(catalog.is_empty());
        assert_eq!(catalog.len(), 0);
    }

    #[test]
    fn test_view_catalog_register() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        assert!(catalog.exists("path"));
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.list(), vec!["path"]);
    }

    #[test]
    fn test_view_catalog_drop() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        assert!(catalog.exists("path"));
        catalog.drop("path").unwrap();
        assert!(!catalog.exists("path"));
        assert!(catalog.is_empty());
    }

    #[test]
    fn test_view_catalog_drop_nonexistent() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let result = catalog.drop("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_view_catalog_multiple_rules() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First rule: path(X, Y) := edge(X, Y).
        let rule1 = make_test_rule("path", "edge");
        catalog.register("path", &rule1).unwrap();

        // Second rule: path(X, Z) := edge(X, Y), path(Y, Z).
        let head = Atom::new(
            "path".to_string(),
            vec![Term::Variable("X".to_string()), Term::Variable("Z".to_string())],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
            )),
            BodyPredicate::Positive(Atom::new(
                "path".to_string(),
                vec![Term::Variable("Y".to_string()), Term::Variable("Z".to_string())],
            )),
        ];
        let rule2 = Rule::new(head, body, vec![]);
        catalog.register("path", &rule2).unwrap();

        // Should still be one view with two rules
        assert_eq!(catalog.len(), 1);
        let rules = catalog.all_rules();
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn test_view_catalog_persistence() {
        let tmp_dir = TempDir::new().unwrap();
        let db_path = tmp_dir.path().to_path_buf();

        // Create and populate catalog
        {
            let mut catalog = ViewCatalog::new(db_path.clone()).unwrap();
            let rule = make_test_rule("path", "edge");
            catalog.register("path", &rule).unwrap();
        }

        // Reload and verify
        {
            let catalog = ViewCatalog::new(db_path).unwrap();
            assert!(catalog.exists("path"));
            assert_eq!(catalog.len(), 1);
            let rules = catalog.all_rules();
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0].head.relation, "path");
        }
    }

    #[test]
    fn test_view_catalog_get() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        let view = catalog.get("path").unwrap();
        assert_eq!(view.name, "path");
        assert_eq!(view.rules.len(), 1);

        assert!(catalog.get("nonexistent").is_none());
    }

    #[test]
    fn test_view_catalog_describe() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let rule = make_test_rule("path", "edge");
        catalog.register("path", &rule).unwrap();

        let desc = catalog.describe("path").unwrap();
        assert!(desc.contains("View: path"));
        assert!(desc.contains("Rules:"));
    }

    #[test]
    fn test_view_catalog_all_rules() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        catalog.register("path", &make_test_rule("path", "edge")).unwrap();
        catalog.register("reach", &make_test_rule("reach", "source")).unwrap();

        let rules = catalog.all_rules();
        assert_eq!(rules.len(), 2);

        let relations: Vec<_> = rules.iter().map(|r| r.head.relation.as_str()).collect();
        assert!(relations.contains(&"path"));
        assert!(relations.contains(&"reach"));
    }

    #[test]
    fn test_view_catalog_clear() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        catalog.register("path", &make_test_rule("path", "edge")).unwrap();
        catalog.register("reach", &make_test_rule("reach", "source")).unwrap();

        assert_eq!(catalog.len(), 2);

        catalog.clear();
        assert!(catalog.is_empty());
    }

    #[test]
    fn test_view_catalog_clear_rules() {
        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Register view with 2 rules
        let rule1 = make_test_rule("path", "edge");
        catalog.register("path", &rule1).unwrap();

        let head = Atom::new(
            "path".to_string(),
            vec![Term::Variable("X".to_string()), Term::Variable("Z".to_string())],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
            )),
            BodyPredicate::Positive(Atom::new(
                "path".to_string(),
                vec![Term::Variable("Y".to_string()), Term::Variable("Z".to_string())],
            )),
        ];
        let rule2 = Rule::new(head, body, vec![]);
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
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // Register view with 2 rules
        let rule1 = make_test_rule("path", "edge");
        catalog.register("path", &rule1).unwrap();

        let head = Atom::new(
            "path".to_string(),
            vec![Term::Variable("X".to_string()), Term::Variable("Z".to_string())],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
            )),
            BodyPredicate::Positive(Atom::new(
                "path".to_string(),
                vec![Term::Variable("Y".to_string()), Term::Variable("Z".to_string())],
            )),
        ];
        let rule2 = Rule::new(head, body, vec![]);
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
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

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
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        let result = catalog.clear_rules("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("does not exist"));
    }

    #[test]
    fn test_view_definition_new() {
        let rule = make_test_rule("path", "edge");
        let serializable = SerializableRule::from_rule(&rule);
        let def = ViewDefinition::new("path".to_string(), serializable);

        assert_eq!(def.name, "path");
        assert_eq!(def.rules.len(), 1);
        assert!(def.description.is_none());
    }

    #[test]
    fn test_register_view_multiple_rules() {
        use crate::statement::ViewDef;

        let tmp_dir = TempDir::new().unwrap();
        let mut catalog = ViewCatalog::new(tmp_dir.path().to_path_buf()).unwrap();

        // First rule: connected(X, Y) := edge(X, Y).
        let rule1 = make_test_rule("connected", "edge");
        let view_def1 = ViewDef {
            name: "connected".to_string(),
            rule: SerializableRule::from_rule(&rule1),
        };
        catalog.register_view(&view_def1).unwrap();

        println!("After first register_view:");
        println!("  Number of views: {}", catalog.len());
        if let Some(view) = catalog.get("connected") {
            println!("  Rules in 'connected': {}", view.rules.len());
        }

        // Second rule: connected(X, Z) := edge(X, Y), connected(Y, Z).
        let head = Atom::new(
            "connected".to_string(),
            vec![Term::Variable("X".to_string()), Term::Variable("Z".to_string())],
        );
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
            )),
            BodyPredicate::Positive(Atom::new(
                "connected".to_string(),
                vec![Term::Variable("Y".to_string()), Term::Variable("Z".to_string())],
            )),
        ];
        let rule2 = Rule::new(head, body, vec![]);
        let view_def2 = ViewDef {
            name: "connected".to_string(),
            rule: SerializableRule::from_rule(&rule2),
        };
        catalog.register_view(&view_def2).unwrap();

        println!("After second register_view:");
        println!("  Number of views: {}", catalog.len());
        if let Some(view) = catalog.get("connected") {
            println!("  Rules in 'connected': {}", view.rules.len());
        }

        // Verify: should have 1 view with 2 rules
        assert_eq!(catalog.len(), 1, "Should have exactly 1 view");
        let rules = catalog.all_rules();
        assert_eq!(rules.len(), 2, "Should have exactly 2 rules total");

        // Check the view has both rules
        let view = catalog.get("connected").expect("View 'connected' should exist");
        assert_eq!(view.rules.len(), 2, "View 'connected' should have 2 rules");
    }

    #[test]
    fn test_register_view_persists_multiple_rules() {
        use crate::statement::ViewDef;

        let tmp_dir = TempDir::new().unwrap();
        let db_path = tmp_dir.path().to_path_buf();

        // Register two rules for 'connected'
        {
            let mut catalog = ViewCatalog::new(db_path.clone()).unwrap();

            // First rule
            let rule1 = make_test_rule("connected", "edge");
            let view_def1 = ViewDef {
                name: "connected".to_string(),
                rule: SerializableRule::from_rule(&rule1),
            };
            catalog.register_view(&view_def1).unwrap();

            // Second rule
            let head = Atom::new(
                "connected".to_string(),
                vec![Term::Variable("X".to_string()), Term::Variable("Z".to_string())],
            );
            let body = vec![
                BodyPredicate::Positive(Atom::new(
                    "edge".to_string(),
                    vec![Term::Variable("X".to_string()), Term::Variable("Y".to_string())],
                )),
                BodyPredicate::Positive(Atom::new(
                    "connected".to_string(),
                    vec![Term::Variable("Y".to_string()), Term::Variable("Z".to_string())],
                )),
            ];
            let rule2 = Rule::new(head, body, vec![]);
            let view_def2 = ViewDef {
                name: "connected".to_string(),
                rule: SerializableRule::from_rule(&rule2),
            };
            catalog.register_view(&view_def2).unwrap();

            println!("Before dropping catalog:");
            println!("  Rules count: {}", catalog.all_rules().len());
        }
        // Catalog is dropped here, file should be persisted

        // Reload and verify
        {
            let catalog = ViewCatalog::new(db_path).unwrap();
            println!("After reloading catalog:");
            println!("  Views count: {}", catalog.len());
            println!("  Rules count: {}", catalog.all_rules().len());

            assert_eq!(catalog.len(), 1, "Should have 1 view after reload");
            assert_eq!(catalog.all_rules().len(), 2, "Should have 2 rules after reload");

            let view = catalog.get("connected").expect("View should exist");
            assert_eq!(view.rules.len(), 2, "View should have 2 rules after reload");
        }
    }
}
