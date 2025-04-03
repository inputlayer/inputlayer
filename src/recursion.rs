//! # Recursion Support
//!
//! Recursion detection, dependency graphs, SCC detection (Tarjan's), and stratification
//! for Datalog programs. Handles both positive recursion and stratified negation.
//!
//! A rule is recursive if its head relation appears in its body:
//! ```datalog
//! tc(x, z) :- tc(x, y), edge(y, z).
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
/// reach(x) :- source(x).             // Stratum 0
/// reach(y) :- reach(x), edge(x, y).  // Stratum 0 (recursive)
/// unreachable(x) :- node(x), !reach(x). // Stratum 1 (negates reach)
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
            eprintln!("Warning: Program not stratifiable: {relation} - {reason}");
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

