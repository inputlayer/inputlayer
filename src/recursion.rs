//! # Recursion Support
//!
//! This module provides utilities for detecting and handling recursive Datalog programs.
//!
//! ## Recursion in Datalog
//!
//! A rule is **recursive** if its head relation appears in its body:
//! ```datalog
//! tc(x, z) :- tc(x, y), edge(y, z).
//! ```
//!
//! ## Implementation Requirements
//!
//! To support recursion, we need:
//!
//! ### 1. Detection
//! - Identify which rules are recursive
//! - Build dependency graph between relations
//! - Find strongly connected components (SCCs)
//!
//! ### 2. Stratification
//! - Group rules into strata (layers)
//! - Rules in same SCC go in same stratum
//! - Execute strata in order
//! - **Negation requires stratification**: Relations used in negated atoms must
//!   be fully computed before the rules using them can execute
//!
//! ### 3. Code Generation with .iterative()
//! - Non-recursive strata: direct DD code
//! - Recursive strata: wrap in `.iterative()` scope
//! - Pattern from Module 11:
//!   ```text
//!   scope.iterative::<u32, _, _>(|inner| {
//!       let (handle, stream) = inner.new_collection();
//!       let base = base_case.enter(inner);
//!       let recursive = stream.join(...);
//!       let next = base.concat(&recursive).distinct();
//!       next.connect_loop(handle);
//!       next.leave()
//!   })
//!   ```
//!
//! ### 4. Semi-Naive Evaluation (Optimization)
//! - Track only "delta" (new tuples) each iteration
//! - Significantly faster than naive evaluation
//! - Implementation detail for advanced students
//!
//! ## Current Implementation
//!
//! This module provides complete recursion support:
//! - **Recursion detection**: Identify recursive rules and programs
//! - **Dependency graph**: Build relation dependencies from rules (positive and negative)
//! - **SCC detection**: Tarjan's algorithm for finding strongly connected components
//! - **Stratification**: Group rules into evaluation strata based on dependencies
//! - **Negation stratification**: Ensure negated atoms are computed before use
//!
//! ## What Students Learn
//!
//! By studying this implementation, students learn:
//! - How to detect recursion in Datalog programs
//! - Tarjan's algorithm for finding strongly connected components
//! - Stratification algorithms for query evaluation order
//! - How dependency graphs guide recursive query execution
//! - How negation affects stratification (requires lower-stratum computation)
//!
//! ## Student Exercises (Separate from Final Project)
//!
//! In course modules, students will:
//! - Module 11: Trace SCC detection manually on example graphs
//! - Module 11: Generate code with `.iterative()` scopes for recursive strata
//! - Module 11: Test with recursive queries (transitive closure, etc.)
//! - Module 08: Extend with semi-naive evaluation (delta rules)

use crate::ast::{BodyPredicate, Program, Rule};
use std::collections::{HashMap, HashSet};

// ============================================================================
// Dependency Types for Stratification
// ============================================================================

/// Type of dependency between relations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DependencyType {
    /// Positive dependency: head depends on relation via positive atom
    /// Can be in same stratum or higher
    Positive,
    /// Negative dependency: head depends on relation via negated atom
    /// Negated relation MUST be in strictly lower stratum
    Negative,
}

/// Extended dependency graph with positive/negative edges
///
/// This is essential for stratified negation:
/// - Positive edges: A → B means A depends on B (can be same or higher stratum)
/// - Negative edges: A -/→ B means A negates B (B must be in lower stratum)
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    /// Map from relation to its dependencies with types
    pub edges: HashMap<String, Vec<(String, DependencyType)>>,
    /// All relations in the graph
    pub relations: HashSet<String>,
}

impl DependencyGraph {
    /// Create a new empty dependency graph
    pub fn new() -> Self {
        DependencyGraph {
            edges: HashMap::new(),
            relations: HashSet::new(),
        }
    }

    /// Add a dependency edge
    pub fn add_edge(&mut self, from: &str, to: &str, dep_type: DependencyType) {
        self.relations.insert(from.to_string());
        self.relations.insert(to.to_string());
        self.edges
            .entry(from.to_string())
            .or_insert_with(Vec::new)
            .push((to.to_string(), dep_type));
    }

    /// Get positive dependencies for a relation
    pub fn positive_deps(&self, relation: &str) -> Vec<&str> {
        self.edges
            .get(relation)
            .map(|deps| {
                deps.iter()
                    .filter(|(_, t)| *t == DependencyType::Positive)
                    .map(|(r, _)| r.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get negative dependencies for a relation
    pub fn negative_deps(&self, relation: &str) -> Vec<&str> {
        self.edges
            .get(relation)
            .map(|deps| {
                deps.iter()
                    .filter(|(_, t)| *t == DependencyType::Negative)
                    .map(|(r, _)| r.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if a relation has any negative dependencies
    pub fn has_negative_deps(&self, relation: &str) -> bool {
        !self.negative_deps(relation).is_empty()
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Recursion Detection
// ============================================================================

/// Check if a single rule is recursive
///
/// A rule is recursive if its head relation appears in any positive body atom.
pub fn is_recursive_rule(rule: &Rule) -> bool {
    let head_relation = &rule.head.relation;

    for pred in &rule.body {
        if let BodyPredicate::Positive(atom) = pred {
            if &atom.relation == head_relation {
                return true;
            }
        }
    }

    false
}

/// Check if a program contains any recursive rules
pub fn has_recursion(program: &Program) -> bool {
    program.rules.iter().any(|rule| is_recursive_rule(rule))
}

/// Build extended relation dependency graph with positive/negative edges
///
/// Returns: DependencyGraph with typed edges for stratification
pub fn build_extended_dependency_graph(program: &Program) -> DependencyGraph {
    let mut graph = DependencyGraph::new();

    for rule in &program.rules {
        let head_relation = &rule.head.relation;
        graph.relations.insert(head_relation.clone());

        for pred in &rule.body {
            match pred {
                BodyPredicate::Positive(atom) => {
                    graph.add_edge(head_relation, &atom.relation, DependencyType::Positive);
                }
                BodyPredicate::Negated(atom) => {
                    graph.add_edge(head_relation, &atom.relation, DependencyType::Negative);
                }
            }
        }
    }

    graph
}

/// Build relation dependency graph (positive dependencies only)
///
/// Returns: Map from relation name to set of relations it depends on
///
/// Note: For stratified negation, use `build_extended_dependency_graph` instead.
pub fn build_dependency_graph(program: &Program) -> HashMap<String, HashSet<String>> {
    let mut graph = HashMap::new();

    for rule in &program.rules {
        let head_relation = rule.head.relation.clone();
        let mut dependencies = HashSet::new();

        for pred in &rule.body {
            if let BodyPredicate::Positive(atom) = pred {
                dependencies.insert(atom.relation.clone());
            }
        }

        graph
            .entry(head_relation)
            .or_insert_with(HashSet::new)
            .extend(dependencies);
    }

    graph
}

/// Find strongly connected components (SCCs) in dependency graph
///
/// This is needed for stratification. Relations in the same SCC must be
/// computed together in an iterative scope.
///
/// ## Implementation
///
/// Uses Tarjan's algorithm to find SCCs:
/// 1. Depth-first search with discovery times
/// 2. Track low-link values for each node
/// 3. Identify back edges that form cycles
/// 4. Group nodes in same cycle into SCCs
///
/// ## What Students Learn
///
/// By studying this implementation, students learn:
/// - Tarjan's algorithm for SCC detection
/// - Cycle detection in directed graphs
/// - Stack-based DFS for graph algorithms
/// - Application of SCCs to query planning
pub fn find_sccs(graph: &HashMap<String, HashSet<String>>) -> Vec<Vec<String>> {
    let mut index = 0;
    let mut stack = Vec::new();
    let mut indices: HashMap<String, usize> = HashMap::new();
    let mut lowlinks: HashMap<String, usize> = HashMap::new();
    let mut on_stack: HashSet<String> = HashSet::new();
    let mut sccs = Vec::new();

    // Get all nodes from the graph
    let mut nodes: HashSet<String> = HashSet::new();
    for (node, neighbors) in graph {
        nodes.insert(node.clone());
        for neighbor in neighbors {
            nodes.insert(neighbor.clone());
        }
    }

    // Tarjan's algorithm - visit each unvisited node
    for node in nodes {
        if !indices.contains_key(&node) {
            strongconnect(
                &node,
                graph,
                &mut index,
                &mut stack,
                &mut indices,
                &mut lowlinks,
                &mut on_stack,
                &mut sccs,
            );
        }
    }

    sccs
}

/// Helper function for Tarjan's algorithm
fn strongconnect(
    v: &str,
    graph: &HashMap<String, HashSet<String>>,
    index: &mut usize,
    stack: &mut Vec<String>,
    indices: &mut HashMap<String, usize>,
    lowlinks: &mut HashMap<String, usize>,
    on_stack: &mut HashSet<String>,
    sccs: &mut Vec<Vec<String>>,
) {
    // Set the depth index for v
    indices.insert(v.to_string(), *index);
    lowlinks.insert(v.to_string(), *index);
    *index += 1;
    stack.push(v.to_string());
    on_stack.insert(v.to_string());

    // Consider successors of v
    if let Some(neighbors) = graph.get(v) {
        for w in neighbors {
            if !indices.contains_key(w) {
                // Successor w has not been visited; recurse
                strongconnect(w, graph, index, stack, indices, lowlinks, on_stack, sccs);
                let w_lowlink = lowlinks[w];
                let v_lowlink = lowlinks[v];
                lowlinks.insert(v.to_string(), v_lowlink.min(w_lowlink));
            } else if on_stack.contains(w) {
                // Successor w is on stack and hence in current SCC
                let w_index = indices[w];
                let v_lowlink = lowlinks[v];
                lowlinks.insert(v.to_string(), v_lowlink.min(w_index));
            }
        }
    }

    // If v is a root node, pop the stack to form an SCC
    if lowlinks[v] == indices[v] {
        let mut scc = Vec::new();
        loop {
            let w = stack.pop().unwrap();
            on_stack.remove(&w);
            scc.push(w.clone());
            if w == v {
                break;
            }
        }
        sccs.push(scc);
    }
}

/// Stratification result with potential error
#[derive(Debug, Clone)]
pub enum StratificationResult {
    /// Successfully stratified into strata
    Success(Vec<Vec<usize>>),
    /// Program is not stratifiable (negation through recursion)
    NotStratifiable {
        /// The relation that causes the problem
        relation: String,
        /// Why it's not stratifiable
        reason: String,
    },
}

impl StratificationResult {
    /// Get the strata if stratification succeeded
    pub fn strata(&self) -> Option<&Vec<Vec<usize>>> {
        match self {
            StratificationResult::Success(strata) => Some(strata),
            StratificationResult::NotStratifiable { .. } => None,
        }
    }

    /// Check if stratification succeeded
    pub fn is_success(&self) -> bool {
        matches!(self, StratificationResult::Success(_))
    }

    /// Unwrap the strata, panicking if not stratifiable
    pub fn unwrap(self) -> Vec<Vec<usize>> {
        match self {
            StratificationResult::Success(strata) => strata,
            StratificationResult::NotStratifiable { relation, reason } => {
                panic!("Not stratifiable: {} - {}", relation, reason)
            }
        }
    }
}

/// Stratify a program into layers with negation support
///
/// Returns: StratificationResult containing strata or error
///
/// ## Implementation
///
/// Stratification algorithm with negation:
/// 1. Build extended dependency graph (positive + negative edges)
/// 2. Check for negation through recursion (not stratifiable)
/// 3. Compute stratum for each relation using fixpoint iteration
/// 4. Assign rules to strata based on their head relation
///
/// ## Stratification Rules
///
/// - Positive dependency: stratum(A) >= stratum(B) when A depends on B
/// - Negative dependency: stratum(A) > stratum(B) when A negates B
/// - Relations in same SCC must have same stratum
/// - If SCC contains a negative edge to itself → NOT STRATIFIABLE
///
/// ## What Students Learn
///
/// By studying this implementation, students learn:
/// - Stratification for recursive query evaluation
/// - How negation affects stratification constraints
/// - Why negation through recursion is problematic
/// - Fixpoint algorithms for stratum assignment
///
/// ## Example
///
/// For unreachable nodes:
/// ```datalog
/// reach(x) :- source(x).             // Stratum 0
/// reach(y) :- reach(x), edge(x, y).  // Stratum 0 (recursive)
/// unreachable(x) :- node(x), !reach(x). // Stratum 1 (negates reach)
/// ```
pub fn stratify_with_negation(program: &Program) -> StratificationResult {
    if program.rules.is_empty() {
        return StratificationResult::Success(vec![]);
    }

    // Build positive-only graph for SCC detection
    let positive_graph = build_dependency_graph(program);

    // Find SCCs in the positive dependency graph
    let sccs = find_sccs(&positive_graph);

    // Create mapping from relation to SCC
    let mut relation_to_scc: HashMap<String, usize> = HashMap::new();
    let mut scc_to_relations: HashMap<usize, Vec<String>> = HashMap::new();
    for (scc_idx, scc) in sccs.iter().enumerate() {
        for relation in scc {
            relation_to_scc.insert(relation.clone(), scc_idx);
            scc_to_relations
                .entry(scc_idx)
                .or_default()
                .push(relation.clone());
        }
    }

    // Check for negation within SCCs (not stratifiable)
    for rule in &program.rules {
        let head_relation = &rule.head.relation;
        let head_scc = relation_to_scc.get(head_relation);

        for negated_atom in rule.negated_body_atoms() {
            let negated_relation = &negated_atom.relation;
            let negated_scc = relation_to_scc.get(negated_relation);

            // If head and negated relation are in the same SCC, not stratifiable
            if head_scc == negated_scc && head_scc.is_some() {
                return StratificationResult::NotStratifiable {
                    relation: head_relation.clone(),
                    reason: format!(
                        "Negation of '{}' within same SCC as '{}' (negation through recursion)",
                        negated_relation, head_relation
                    ),
                };
            }
        }
    }

    // Compute stratum for each SCC using fixpoint iteration
    let num_sccs = sccs.len();
    let mut scc_stratum: Vec<usize> = vec![0; num_sccs];

    // Iterate until fixpoint
    let mut changed = true;
    let max_iterations = num_sccs + 1; // Guard against infinite loops
    let mut iterations = 0;

    while changed && iterations < max_iterations {
        changed = false;
        iterations += 1;

        for rule in &program.rules {
            let head_relation = &rule.head.relation;
            let head_scc_opt = relation_to_scc.get(head_relation);
            if head_scc_opt.is_none() {
                continue;
            }
            let head_scc = *head_scc_opt.unwrap();

            // Process positive dependencies
            for pos_atom in rule.positive_body_atoms() {
                if let Some(&dep_scc) = relation_to_scc.get(&pos_atom.relation) {
                    // Positive: stratum(head) >= stratum(dep)
                    if scc_stratum[head_scc] < scc_stratum[dep_scc] {
                        scc_stratum[head_scc] = scc_stratum[dep_scc];
                        changed = true;
                    }
                }
            }

            // Process negative dependencies
            for neg_atom in rule.negated_body_atoms() {
                if let Some(&dep_scc) = relation_to_scc.get(&neg_atom.relation) {
                    // Negative: stratum(head) > stratum(dep)
                    let required_stratum = scc_stratum[dep_scc] + 1;
                    if scc_stratum[head_scc] < required_stratum {
                        scc_stratum[head_scc] = required_stratum;
                        changed = true;
                    }
                }
            }
        }
    }

    // Assign each rule to a stratum based on its head relation's SCC
    let mut rule_to_stratum: Vec<usize> = Vec::new();
    for rule in &program.rules {
        let head_relation = &rule.head.relation;
        let stratum = relation_to_scc
            .get(head_relation)
            .map(|&scc| scc_stratum[scc])
            .unwrap_or(0);
        rule_to_stratum.push(stratum);
    }

    // Group rules by stratum
    let max_stratum = rule_to_stratum.iter().max().copied().unwrap_or(0);
    let mut strata: Vec<Vec<usize>> = vec![Vec::new(); max_stratum + 1];

    for (rule_idx, &stratum) in rule_to_stratum.iter().enumerate() {
        strata[stratum].push(rule_idx);
    }

    // Remove empty strata
    strata.retain(|s| !s.is_empty());

    StratificationResult::Success(strata)
}

/// Stratify a program into layers
///
/// Returns: Vec of strata, where each stratum is a Vec of rule indices
///
/// ## Implementation
///
/// Stratification algorithm:
/// 1. Build dependency graph from rules
/// 2. Find SCCs in the graph
/// 3. Topologically sort SCCs
/// 4. Assign rules to strata based on their head relation's SCC
/// 5. Order strata to respect dependencies
///
/// ## What Students Learn
///
/// By studying this implementation, students learn:
/// - Stratification for recursive query evaluation
/// - Topological sorting of dependency graphs
/// - Grouping rules into evaluation strata
/// - Handling recursive and non-recursive rules differently
///
/// ## Example
///
/// For transitive closure:
/// ```datalog
/// tc(x, y) :- edge(x, y).           // Stratum 0 (base case)
/// tc(x, z) :- tc(x, y), edge(y, z). // Stratum 0 (recursive - same SCC)
/// result(x, y) :- tc(x, y).         // Stratum 1 (depends on tc)
/// ```
///
/// Note: For programs with negation, use `stratify_with_negation` instead.
pub fn stratify(program: &Program) -> Vec<Vec<usize>> {
    // Delegate to the negation-aware version
    match stratify_with_negation(program) {
        StratificationResult::Success(strata) => strata,
        StratificationResult::NotStratifiable { relation, reason } => {
            // Fall back to basic stratification (ignoring negation constraints)
            // This maintains backward compatibility
            eprintln!(
                "Warning: Program not stratifiable: {} - {}",
                relation, reason
            );
            basic_stratify(program)
        }
    }
}

/// Basic stratification without negation support (for backward compatibility)
fn basic_stratify(program: &Program) -> Vec<Vec<usize>> {
    if program.rules.is_empty() {
        return vec![];
    }

    // Build dependency graph
    let graph = build_dependency_graph(program);

    // Find SCCs in the dependency graph
    let sccs = find_sccs(&graph);

    // Create mapping from relation to SCC index
    let mut relation_to_scc: HashMap<String, usize> = HashMap::new();
    for (scc_idx, scc) in sccs.iter().enumerate() {
        for relation in scc {
            relation_to_scc.insert(relation.clone(), scc_idx);
        }
    }

    // Assign each rule to a stratum based on its head relation's SCC
    let mut rule_to_stratum: Vec<usize> = Vec::new();
    for rule in &program.rules {
        let head_relation = &rule.head.relation;
        let stratum = relation_to_scc.get(head_relation).copied().unwrap_or(0);
        rule_to_stratum.push(stratum);
    }

    // Group rules by stratum
    let max_stratum = rule_to_stratum.iter().max().copied().unwrap_or(0);
    let mut strata: Vec<Vec<usize>> = vec![Vec::new(); max_stratum + 1];

    for (rule_idx, &stratum) in rule_to_stratum.iter().enumerate() {
        strata[stratum].push(rule_idx);
    }

    // Remove empty strata
    strata.retain(|s| !s.is_empty());

    strata
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Atom, Term};

    #[test]
    fn test_is_recursive_rule() {
        // tc(x, z) :- tc(x, y), edge(y, z).  -> RECURSIVE
        let rule = Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("z".to_string()),
                ],
            ),
            vec![
                Atom::new(
                    "tc".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("y".to_string()),
                        Term::Variable("z".to_string()),
                    ],
                ),
            ],
            vec![],
        );

        assert!(is_recursive_rule(&rule));
    }

    #[test]
    fn test_non_recursive_rule() {
        // result(x, y) :- edge(x, y).  -> NOT RECURSIVE
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![],
        );

        assert!(!is_recursive_rule(&rule));
    }

    #[test]
    fn test_has_recursion() {
        let mut program = Program::new();

        // Non-recursive rule
        program.add_rule(Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![],
        ));

        assert!(!has_recursion(&program));

        // Add recursive rule
        program.add_rule(Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("z".to_string()),
                ],
            ),
            vec![
                Atom::new(
                    "tc".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("y".to_string()),
                        Term::Variable("z".to_string()),
                    ],
                ),
            ],
            vec![],
        ));

        assert!(has_recursion(&program));
    }

    #[test]
    fn test_build_dependency_graph() {
        let mut program = Program::new();

        // tc(x, y) :- edge(x, y).
        program.add_rule(Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![],
        ));

        // tc(x, z) :- tc(x, y), edge(y, z).
        program.add_rule(Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("z".to_string()),
                ],
            ),
            vec![
                Atom::new(
                    "tc".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("y".to_string()),
                        Term::Variable("z".to_string()),
                    ],
                ),
            ],
            vec![],
        ));

        let graph = build_dependency_graph(&program);

        assert!(graph.contains_key("tc"));
        let tc_deps = &graph["tc"];
        assert!(tc_deps.contains("edge"));
        assert!(tc_deps.contains("tc")); // Self-dependency!
    }

    #[test]
    fn test_find_sccs_simple() {
        // Simple graph: a -> b -> c (no cycles)
        let mut graph = HashMap::new();
        graph.insert("a".to_string(), ["b".to_string()].iter().cloned().collect());
        graph.insert("b".to_string(), ["c".to_string()].iter().cloned().collect());
        graph.insert("c".to_string(), HashSet::new());

        let sccs = find_sccs(&graph);

        // Each node should be in its own SCC (no cycles)
        assert_eq!(sccs.len(), 3);
    }

    #[test]
    fn test_find_sccs_cycle() {
        // Graph with self-loop: tc -> tc, tc -> edge
        let mut graph = HashMap::new();
        let mut tc_deps = HashSet::new();
        tc_deps.insert("tc".to_string());
        tc_deps.insert("edge".to_string());
        graph.insert("tc".to_string(), tc_deps);

        let sccs = find_sccs(&graph);

        // tc forms a SCC with itself (self-loop)
        // edge is in its own SCC
        assert!(sccs.len() >= 1);

        // Find the SCC containing "tc"
        let tc_scc = sccs.iter().find(|scc| scc.contains(&"tc".to_string()));
        assert!(tc_scc.is_some());
    }

    #[test]
    fn test_stratify_non_recursive() {
        let mut program = Program::new();

        // result(x, y) :- edge(x, y).
        program.add_rule(Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![],
        ));

        let strata = stratify(&program);

        // Should have at least one stratum
        assert!(!strata.is_empty());
        // First stratum should contain rule 0
        assert!(strata.iter().any(|s| s.contains(&0)));
    }

    #[test]
    fn test_stratify_recursive() {
        let mut program = Program::new();

        // tc(x, y) :- edge(x, y).
        program.add_rule(Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![],
        ));

        // tc(x, z) :- tc(x, y), edge(y, z). [RECURSIVE]
        program.add_rule(Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("z".to_string()),
                ],
            ),
            vec![
                Atom::new(
                    "tc".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("y".to_string()),
                        Term::Variable("z".to_string()),
                    ],
                ),
            ],
            vec![],
        ));

        let strata = stratify(&program);

        // Should have at least one stratum
        assert!(!strata.is_empty());

        // Both rules should be in the same stratum (they define same relation "tc")
        let found_stratum = strata.iter().find(|s| s.contains(&0) || s.contains(&1));
        assert!(found_stratum.is_some());

        if let Some(stratum) = found_stratum {
            // Both rules for "tc" should be in same stratum
            assert!(stratum.contains(&0) && stratum.contains(&1));
        }
    }

    // ========================================================================
    // Tests for stratification with negation
    // ========================================================================

    #[test]
    fn test_stratify_with_negation_simple() {
        // unreachable(x) :- node(x), !reach(x).
        // This should be stratifiable: reach must be computed before unreachable
        let mut program = Program::new();

        // reach(x) :- source(x).
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        // unreachable(x) :- node(x), !reach(x).
        program.add_rule(Rule::new(
            Atom::new(
                "unreachable".to_string(),
                vec![Term::Variable("x".to_string())],
            ),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "node".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "reach".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
            ],
            vec![],
        ));

        let result = stratify_with_negation(&program);
        assert!(result.is_success());

        let strata = result.unwrap();
        // Should have 2 strata: reach in stratum 0, unreachable in stratum 1
        assert_eq!(strata.len(), 2);

        // Rule 0 (reach) should be in stratum 0
        assert!(strata[0].contains(&0));
        // Rule 1 (unreachable) should be in stratum 1
        assert!(strata[1].contains(&1));
    }

    #[test]
    fn test_stratify_with_negation_not_stratifiable() {
        // This is NOT stratifiable: negation through recursion
        // p(x) :- q(x), !p(x).  -- p depends negatively on itself
        let mut program = Program::new();

        program.add_rule(Rule::new(
            Atom::new("p".to_string(), vec![Term::Variable("x".to_string())]),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "q".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "p".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
            ],
            vec![],
        ));

        let result = stratify_with_negation(&program);
        assert!(!result.is_success());

        match result {
            StratificationResult::NotStratifiable { relation, .. } => {
                assert_eq!(relation, "p");
            }
            _ => panic!("Expected NotStratifiable"),
        }
    }

    #[test]
    fn test_stratify_with_negation_chain() {
        // a(x) :- base(x).
        // b(x) :- a(x), !c(x).   -- b depends negatively on c
        // c(x) :- base(x).
        // This is stratifiable: c computed first, then b
        let mut program = Program::new();

        // a(x) :- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        // b(x) :- a(x), !c(x).
        program.add_rule(Rule::new(
            Atom::new("b".to_string(), vec![Term::Variable("x".to_string())]),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "a".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "c".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
            ],
            vec![],
        ));

        // c(x) :- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("c".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        let result = stratify_with_negation(&program);
        assert!(result.is_success());

        let strata = result.unwrap();
        // b must be in a higher stratum than c
        // Find which stratum b is in
        let b_stratum = strata.iter().position(|s| s.contains(&1)).unwrap();
        let c_stratum = strata.iter().position(|s| s.contains(&2)).unwrap();
        assert!(b_stratum > c_stratum, "b must be in higher stratum than c");
    }

    #[test]
    fn test_stratify_with_negation_recursive_and_negation() {
        // Transitive closure with negation (stratifiable)
        // tc(x, y) :- edge(x, y).
        // tc(x, z) :- tc(x, y), edge(y, z).
        // not_connected(x, y) :- node(x), node(y), !tc(x, y).
        let mut program = Program::new();

        // tc(x, y) :- edge(x, y).
        program.add_rule(Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![],
        ));

        // tc(x, z) :- tc(x, y), edge(y, z).
        program.add_rule(Rule::new_simple(
            Atom::new(
                "tc".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("z".to_string()),
                ],
            ),
            vec![
                Atom::new(
                    "tc".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("y".to_string()),
                        Term::Variable("z".to_string()),
                    ],
                ),
            ],
            vec![],
        ));

        // not_connected(x, y) :- node(x), node(y), !tc(x, y).
        program.add_rule(Rule::new(
            Atom::new(
                "not_connected".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "node".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Positive(Atom::new(
                    "node".to_string(),
                    vec![Term::Variable("y".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "tc".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                )),
            ],
            vec![],
        ));

        let result = stratify_with_negation(&program);
        assert!(result.is_success());

        let strata = result.unwrap();
        // tc (rules 0, 1) should be in stratum 0
        // not_connected (rule 2) should be in stratum 1
        assert!(strata.len() >= 2);

        let tc_stratum = strata.iter().position(|s| s.contains(&0)).unwrap();
        let not_connected_stratum = strata.iter().position(|s| s.contains(&2)).unwrap();
        assert!(
            not_connected_stratum > tc_stratum,
            "not_connected must be after tc"
        );
    }

    #[test]
    fn test_extended_dependency_graph() {
        // Test building extended dependency graph with positive/negative edges
        let mut program = Program::new();

        // a(x) :- b(x), !c(x).
        program.add_rule(Rule::new(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "b".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "c".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
            ],
            vec![],
        ));

        let graph = build_extended_dependency_graph(&program);

        // Check that 'a' has positive dep on 'b'
        let pos_deps = graph.positive_deps("a");
        assert!(pos_deps.contains(&"b"));

        // Check that 'a' has negative dep on 'c'
        let neg_deps = graph.negative_deps("a");
        assert!(neg_deps.contains(&"c"));

        // Check has_negative_deps
        assert!(graph.has_negative_deps("a"));
    }

    #[test]
    fn test_stratify_multiple_negations() {
        // d(x) :- a(x), !b(x), !c(x).
        // All of a, b, c must be computed before d
        let mut program = Program::new();

        // a(x) :- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        // b(x) :- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("b".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        // c(x) :- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("c".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        // d(x) :- a(x), !b(x), !c(x).
        program.add_rule(Rule::new(
            Atom::new("d".to_string(), vec![Term::Variable("x".to_string())]),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "a".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "b".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "c".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
            ],
            vec![],
        ));

        let result = stratify_with_negation(&program);
        assert!(result.is_success());

        let strata = result.unwrap();
        // d must be in a higher stratum than a, b, and c
        let d_stratum = strata.iter().position(|s| s.contains(&3)).unwrap();
        let b_stratum = strata.iter().position(|s| s.contains(&1)).unwrap();
        let c_stratum = strata.iter().position(|s| s.contains(&2)).unwrap();

        assert!(d_stratum > b_stratum, "d must be after b");
        assert!(d_stratum > c_stratum, "d must be after c");
    }
}
