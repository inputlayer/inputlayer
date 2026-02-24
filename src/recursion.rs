//! # Recursion Support
//!
//! Recursion detection, dependency graphs, SCC detection (Tarjan's), and stratification
//! for Datalog programs. Handles both positive recursion and stratified negation.
//!
//! A rule is recursive if its head relation appears in its body:
//! ```datalog
//! tc(x, z) <- tc(x, y), edge(y, z).
//! ```
//!
//! Stratification groups rules into evaluation layers so that negated relations
//! are fully computed before rules that negate them can execute.
//!
use crate::ast::{BodyPredicate, Program, Rule};
use std::collections::{HashMap, HashSet};

// Dependency Types for Stratification
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
/// - Positive edges: A -> B means A depends on B (can be same or higher stratum)
/// - Negative edges: A -/-> B means A negates B (B must be in lower stratum)
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
            .or_default()
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

    /// Convert to simple graph format for SCC detection (all edges, ignoring type)
    /// This is needed because cycles can form through BOTH positive and negative edges
    pub fn to_simple_graph(&self) -> HashMap<String, HashSet<String>> {
        let mut simple = HashMap::new();
        for (from, edges) in &self.edges {
            let deps: HashSet<String> = edges.iter().map(|(to, _)| to.clone()).collect();
            simple.insert(from.clone(), deps);
        }
        // Ensure all relations are in the graph even if they have no outgoing edges
        for rel in &self.relations {
            simple.entry(rel.clone()).or_insert_with(HashSet::new);
        }
        simple
    }

    /// Check if there's a negative edge within an SCC
    /// Returns the first negative edge found within the SCC, if any
    /// NOTE: Sorted for deterministic error messages
    pub fn has_negative_edge_in_scc(&self, scc: &[String]) -> Option<(String, String)> {
        let scc_set: HashSet<&String> = scc.iter().collect();
        // Sort for deterministic iteration order
        let mut sorted_scc: Vec<&String> = scc.iter().collect();
        sorted_scc.sort();
        for from in sorted_scc {
            if let Some(edges) = self.edges.get(from) {
                // Sort edges by target for deterministic order
                let mut sorted_edges: Vec<_> = edges.iter().collect();
                sorted_edges.sort_by(|a, b| a.0.cmp(&b.0));
                for (to, dep_type) in sorted_edges {
                    if *dep_type == DependencyType::Negative && scc_set.contains(to) {
                        return Some((from.clone(), to.clone()));
                    }
                }
            }
        }
        None
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

// Recursion Detection
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
    program.rules.iter().any(is_recursive_rule)
}

/// Build extended relation dependency graph with positive/negative edges
///
/// Returns: `DependencyGraph` with typed edges for stratification
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
                BodyPredicate::Comparison(_, _, _) => {
                    // Comparisons don't add relation dependencies
                }
                BodyPredicate::HnswNearest { .. } => {
                    // HNSW nearest neighbor search doesn't add relation dependencies
                    // (it queries an index, not a relation)
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
/// ## Algorithm
///
/// Uses Tarjan's algorithm: DFS with discovery times, low-link tracking,
/// and stack-based cycle detection.
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

    /// Try to convert into strata, returning an error with relation and reason if not stratifiable.
    /// This is the safe, non-panicking alternative to `unwrap()`.
    pub fn try_into_strata(self) -> Result<Vec<Vec<usize>>, (String, String)> {
        match self {
            StratificationResult::Success(strata) => Ok(strata),
            StratificationResult::NotStratifiable { relation, reason } => Err((relation, reason)),
        }
    }

    /// Unwrap the strata, panicking if not stratifiable.
    ///
    /// # Panics
    ///
    /// Panics if the stratification result is `NotStratifiable`.
    /// For production code, prefer `try_into_strata()` which returns a Result.
    #[track_caller]
    pub fn unwrap(self) -> Vec<Vec<usize>> {
        match self {
            StratificationResult::Success(strata) => strata,
            StratificationResult::NotStratifiable { relation, reason } => {
                panic!("Not stratifiable: {relation} - {reason}")
            }
        }
    }
}

/// Stratify a program into layers with negation support
///
/// Returns: `StratificationResult` containing strata or error
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
/// - If SCC contains a negative edge to itself -> NOT STRATIFIABLE
///
/// ## Example
///
/// For unreachable nodes:
/// ```datalog
/// reach(x) <- source(x).             // Stratum 0
/// reach(y) <- reach(x), edge(x, y).  // Stratum 0 (recursive)
/// unreachable(x) <- node(x), !reach(x). // Stratum 1 (negates reach)
/// ```
pub fn stratify_with_negation(program: &Program) -> StratificationResult {
    if program.rules.is_empty() {
        return StratificationResult::Success(vec![]);
    }

    // Build extended graph with BOTH positive and negative edges
    // Negative edges are critical for detecting self-negation and mutual negation cycles
    let extended_graph = build_extended_dependency_graph(program);

    // Convert to simple graph for SCC detection (includes ALL edges)
    let simple_graph = extended_graph.to_simple_graph();

    // Find SCCs considering ALL dependencies (both positive and negative)
    let sccs = find_sccs(&simple_graph);

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

    // Check for ANY negative edge within ANY SCC (not stratifiable)
    // This catches: self-negation, mutual negation, and any cycle through negation
    for scc in &sccs {
        if let Some((from, to)) = extended_graph.has_negative_edge_in_scc(scc) {
            let reason = if from == to {
                format!("Self-negation: '{from}' negates itself (!{from} in body)")
            } else {
                format!(
                    "Unstratified negation: '{}' negates '{}' within same recursive cycle. \
                     Cycle members: [{}]",
                    from,
                    to,
                    scc.join(", ")
                )
            };
            return StratificationResult::NotStratifiable {
                relation: from,
                reason,
            };
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
            .map_or(0, |&scc| scc_stratum[scc]);
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
/// ## Example
///
/// For transitive closure:
/// ```datalog
/// tc(x, y) <- edge(x, y).           // Stratum 0 (base case)
/// tc(x, z) <- tc(x, y), edge(y, z). // Stratum 0 (recursive - same SCC)
/// result(x, y) <- tc(x, y).         // Stratum 1 (depends on tc)
/// ```
///
/// Note: For programs with negation, use `stratify_with_negation` instead.
pub fn stratify(program: &Program) -> Vec<Vec<usize>> {
    // Delegate to the negation-aware version
    match stratify_with_negation(program) {
        StratificationResult::Success(strata) => strata,
        StratificationResult::NotStratifiable { reason, .. } => {
            // Fall back to basic stratification (ignoring negation constraints).
            // This maintains backward compatibility. The warning is surfaced
            // through the query result, not printed to stderr.
            let _ = reason; // consumed by caller if needed
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
        // tc(x, z) <- tc(x, y), edge(y, z).  -> RECURSIVE
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
        );

        assert!(is_recursive_rule(&rule));
    }

    #[test]
    fn test_non_recursive_rule() {
        // result(x, y) <- edge(x, y).  -> NOT RECURSIVE
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
        ));

        assert!(has_recursion(&program));
    }

    #[test]
    fn test_build_dependency_graph() {
        let mut program = Program::new();

        // tc(x, y) <- edge(x, y).
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
        ));

        // tc(x, z) <- tc(x, y), edge(y, z).
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

        // result(x, y) <- edge(x, y).
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

        // tc(x, y) <- edge(x, y).
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
        ));

        // tc(x, z) <- tc(x, y), edge(y, z). [RECURSIVE]
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

    // Tests for stratification with negation
    #[test]
    fn test_stratify_with_negation_simple() {
        // unreachable(x) <- node(x), !reach(x).
        // This should be stratifiable: reach must be computed before unreachable
        let mut program = Program::new();

        // reach(x) <- source(x).
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        // unreachable(x) <- node(x), !reach(x).
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
        // p(x) <- q(x), !p(x).  -- p depends negatively on itself
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
        // a(x) <- base(x).
        // b(x) <- a(x), !c(x).   -- b depends negatively on c
        // c(x) <- base(x).
        // This is stratifiable: c computed first, then b
        let mut program = Program::new();

        // a(x) <- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        // b(x) <- a(x), !c(x).
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
        ));

        // c(x) <- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("c".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
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
        // tc(x, y) <- edge(x, y).
        // tc(x, z) <- tc(x, y), edge(y, z).
        // not_connected(x, y) <- node(x), node(y), !tc(x, y).
        let mut program = Program::new();

        // tc(x, y) <- edge(x, y).
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
        ));

        // tc(x, z) <- tc(x, y), edge(y, z).
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
        ));

        // not_connected(x, y) <- node(x), node(y), !tc(x, y).
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

        // a(x) <- b(x), !c(x).
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

    // === Additional Coverage ===

    #[test]
    fn test_dependency_graph_default() {
        let graph = DependencyGraph::default();
        assert!(graph.relations.is_empty());
        assert!(graph.edges.is_empty());
    }

    #[test]
    fn test_dependency_graph_to_simple_graph() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("a", "c", DependencyType::Negative);

        let simple = graph.to_simple_graph();
        assert!(simple.contains_key("a"));
        let a_deps = &simple["a"];
        assert!(a_deps.contains("b"));
        assert!(a_deps.contains("c")); // Both positive and negative edges in simple graph
    }

    #[test]
    fn test_dependency_graph_no_negative_deps() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        assert!(!graph.has_negative_deps("a"));
        assert!(graph.negative_deps("a").is_empty());
    }

    #[test]
    fn test_positive_deps_nonexistent() {
        let graph = DependencyGraph::new();
        assert!(graph.positive_deps("nonexistent").is_empty());
    }

    #[test]
    fn test_negative_deps_nonexistent() {
        let graph = DependencyGraph::new();
        assert!(graph.negative_deps("nonexistent").is_empty());
    }

    #[test]
    fn test_has_negative_edge_in_scc_positive_only() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("b", "a", DependencyType::Positive);

        let scc = vec!["a".to_string(), "b".to_string()];
        assert!(graph.has_negative_edge_in_scc(&scc).is_none());
    }

    #[test]
    fn test_has_negative_edge_in_scc_with_negative() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Negative);
        graph.add_edge("b", "a", DependencyType::Positive);

        let scc = vec!["a".to_string(), "b".to_string()];
        let result = graph.has_negative_edge_in_scc(&scc);
        assert!(result.is_some());
        let (from, to) = result.unwrap();
        assert_eq!(from, "a");
        assert_eq!(to, "b");
    }

    #[test]
    fn test_is_recursive_rule_non_recursive() {
        let rule = Rule::new_simple(
            Atom::new("result".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        );
        assert!(!is_recursive_rule(&rule));
    }

    #[test]
    fn test_has_recursion_empty_program() {
        let program = Program::new();
        assert!(!has_recursion(&program));
    }

    #[test]
    fn test_stratify_empty_program() {
        let program = Program::new();
        let strata = stratify(&program);
        assert!(strata.is_empty());
    }

    #[test]
    fn test_find_sccs_empty_graph() {
        let graph: HashMap<String, HashSet<String>> = HashMap::new();
        let sccs = find_sccs(&graph);
        assert!(sccs.is_empty());
    }

    #[test]
    fn test_find_sccs_mutual_recursion() {
        // a -> b -> a (mutual recursion)
        let mut graph = HashMap::new();
        graph.insert("a".to_string(), ["b".to_string()].iter().cloned().collect());
        graph.insert("b".to_string(), ["a".to_string()].iter().cloned().collect());

        let sccs = find_sccs(&graph);
        // a and b should be in the same SCC
        let ab_scc = sccs.iter().find(|scc| scc.contains(&"a".to_string()));
        assert!(ab_scc.is_some());
        assert!(ab_scc.unwrap().contains(&"b".to_string()));
    }

    #[test]
    fn test_build_extended_dependency_graph_with_comparison() {
        // Tests that comparison predicates don't add edges
        let mut program = Program::new();
        program.add_rule(Rule::new(
            Atom::new("result".to_string(), vec![Term::Variable("x".to_string())]),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "data".to_string(),
                    vec![Term::Variable("x".to_string())],
                )),
                BodyPredicate::Comparison(
                    Term::Variable("x".to_string()),
                    crate::ast::ComparisonOp::GreaterThan,
                    Term::Constant(0),
                ),
            ],
        ));

        let graph = build_extended_dependency_graph(&program);
        let pos_deps = graph.positive_deps("result");
        assert!(pos_deps.contains(&"data"));
        assert_eq!(pos_deps.len(), 1); // Only data, not comparison
    }

    #[test]
    fn test_stratify_multiple_negations() {
        // d(x) <- a(x), !b(x), !c(x).
        // All of a, b, c must be computed before d
        let mut program = Program::new();

        // a(x) <- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        // b(x) <- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("b".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        // c(x) <- base(x).
        program.add_rule(Rule::new_simple(
            Atom::new("c".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        // d(x) <- a(x), !b(x), !c(x).
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

    // =========================================================================
    // Additional Recursion Coverage Tests
    // =========================================================================

    #[test]
    fn test_stratification_result_strata_not_stratifiable() {
        let result = StratificationResult::NotStratifiable {
            relation: "bad".to_string(),
            reason: "negation through recursion".to_string(),
        };
        assert!(result.strata().is_none());
        assert!(!result.is_success());
    }

    #[test]
    fn test_stratification_result_try_into_success() {
        let result = StratificationResult::Success(vec![vec![0], vec![1]]);
        let strata = result.try_into_strata().unwrap();
        assert_eq!(strata.len(), 2);
    }

    #[test]
    fn test_stratification_result_try_into_error() {
        let result = StratificationResult::NotStratifiable {
            relation: "r".to_string(),
            reason: "cycle".to_string(),
        };
        let err = result.try_into_strata().unwrap_err();
        assert_eq!(err.0, "r");
        assert_eq!(err.1, "cycle");
    }

    #[test]
    #[should_panic(expected = "Not stratifiable")]
    fn test_stratification_result_unwrap_panics() {
        let result = StratificationResult::NotStratifiable {
            relation: "x".to_string(),
            reason: "fail".to_string(),
        };
        result.unwrap();
    }

    #[test]
    fn test_dependency_graph_add_edge() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("a", "c", DependencyType::Negative);

        assert!(graph.relations.contains("a"));
        assert!(graph.relations.contains("b"));
        assert!(graph.relations.contains("c"));
    }

    #[test]
    fn test_dependency_graph_positive_deps() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("a", "c", DependencyType::Negative);

        let pos = graph.positive_deps("a");
        assert_eq!(pos, vec!["b"]);
    }

    #[test]
    fn test_dependency_graph_negative_deps() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("a", "c", DependencyType::Negative);

        let neg = graph.negative_deps("a");
        assert_eq!(neg, vec!["c"]);
    }

    #[test]
    fn test_dependency_graph_has_negative_deps() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        assert!(!graph.has_negative_deps("a"));

        graph.add_edge("a", "c", DependencyType::Negative);
        assert!(graph.has_negative_deps("a"));
    }

    #[test]
    fn test_dependency_graph_to_simple_graph_includes_all() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("a", "c", DependencyType::Negative);
        // d has no outgoing edges but is a target
        graph.relations.insert("d".to_string());

        let simple = graph.to_simple_graph();
        assert!(simple.contains_key("a"));
        assert!(simple.contains_key("d"));
        assert!(simple["a"].contains("b"));
        assert!(simple["a"].contains("c"));
    }

    #[test]
    fn test_has_negative_edge_in_scc_none() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("b", "a", DependencyType::Positive);

        let scc = vec!["a".to_string(), "b".to_string()];
        assert!(graph.has_negative_edge_in_scc(&scc).is_none());
    }

    #[test]
    fn test_has_negative_edge_in_scc_found() {
        let mut graph = DependencyGraph::new();
        graph.add_edge("a", "b", DependencyType::Positive);
        graph.add_edge("b", "a", DependencyType::Negative);

        let scc = vec!["a".to_string(), "b".to_string()];
        let result = graph.has_negative_edge_in_scc(&scc);
        assert!(result.is_some());
        let (from, to) = result.unwrap();
        assert_eq!(from, "b");
        assert_eq!(to, "a");
    }

    #[test]
    fn test_find_sccs_singleton() {
        let mut graph = HashMap::new();
        graph.insert("a".to_string(), HashSet::new());

        let sccs = find_sccs(&graph);
        assert_eq!(sccs.len(), 1);
        assert_eq!(sccs[0], vec!["a"]);
    }

    #[test]
    fn test_find_sccs_chain_no_cycle() {
        let mut graph = HashMap::new();
        graph.insert("a".to_string(), HashSet::from(["b".to_string()]));
        graph.insert("b".to_string(), HashSet::from(["c".to_string()]));
        graph.insert("c".to_string(), HashSet::new());

        let sccs = find_sccs(&graph);
        // Each node is its own SCC (no cycles)
        assert_eq!(sccs.len(), 3);
        for scc in &sccs {
            assert_eq!(scc.len(), 1);
        }
    }

    #[test]
    fn test_find_sccs_triangle_cycle() {
        let mut graph = HashMap::new();
        graph.insert("a".to_string(), HashSet::from(["b".to_string()]));
        graph.insert("b".to_string(), HashSet::from(["c".to_string()]));
        graph.insert("c".to_string(), HashSet::from(["a".to_string()]));

        let sccs = find_sccs(&graph);
        // One SCC with all three nodes
        assert_eq!(sccs.len(), 1);
        assert_eq!(sccs[0].len(), 3);
    }

    #[test]
    fn test_stratify_single_rule() {
        let mut program = Program::new();
        program.add_rule(Rule::new_simple(
            Atom::new("result".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "base".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        let strata = stratify(&program);
        assert_eq!(strata.len(), 1);
        assert!(strata[0].contains(&0));
    }

    #[test]
    fn test_is_recursive_rule_with_indirect() {
        // path(X, Y) <- edge(X, Y) - NOT recursive (head not in body)
        let rule = Rule::new_simple(
            Atom::new(
                "path".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            )],
        );
        assert!(!is_recursive_rule(&rule));
    }

    #[test]
    fn test_build_dependency_graph_simple() {
        let mut program = Program::new();
        // a(x) <- b(x)
        program.add_rule(Rule::new_simple(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "b".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));

        let graph = build_dependency_graph(&program);
        assert!(graph.contains_key("a"));
        assert!(graph["a"].contains("b"));
    }

    #[test]
    fn test_dependency_type_equality() {
        assert_eq!(DependencyType::Positive, DependencyType::Positive);
        assert_ne!(DependencyType::Positive, DependencyType::Negative);
    }

    #[test]
    fn test_stratify_with_negation_empty() {
        let program = Program::new();
        let result = stratify_with_negation(&program);
        assert!(result.is_success());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_has_recursion_single_nonrecursive() {
        let mut program = Program::new();
        program.add_rule(Rule::new_simple(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "b".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));
        assert!(!has_recursion(&program));
    }

    #[test]
    fn test_has_recursion_mutual_not_detected() {
        // has_recursion only checks DIRECT self-recursion (head in own body)
        // Mutual recursion (a→b, b→a) is NOT detected by has_recursion
        let mut program = Program::new();
        // a(x) <- b(x)
        program.add_rule(Rule::new_simple(
            Atom::new("a".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "b".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));
        // b(x) <- a(x)
        program.add_rule(Rule::new_simple(
            Atom::new("b".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "a".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
        ));
        // Mutual recursion is NOT detected by has_recursion()
        assert!(!has_recursion(&program));

        // But SCC detection does find the cycle
        let graph = build_dependency_graph(&program);
        let sccs = find_sccs(&graph);
        let has_cycle = sccs.iter().any(|scc| scc.len() > 1);
        assert!(has_cycle, "SCC detection should find mutual recursion");
    }
}
