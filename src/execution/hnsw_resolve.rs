//! Resolve `HnswScan` IR nodes into synthetic base relations.
//!
//! HNSW search runs outside Differential Dataflow. Right before a rule
//! executes, each `HnswScan` in its IR is replaced by a `Scan` over a
//! synthetic relation holding the search results:
//!
//! - literal query: rows `(Id, Dist)`
//! - variable query `QV`: rows `(QV, Id, Dist)`, one search per distinct
//!   vector bound to `QV`. The candidate vectors come from a positive atom in
//!   the same rule body that binds `QV` (already computed, since rules run in
//!   dependency order). The normal join on `QV` then keeps only the bindings
//!   the rest of the body actually produces.

use crate::index_manager::HnswSearchFn;
use crate::ir::{IRExpression, IRNode};
use crate::value::{Relation, RelationMap, Tuple, Value};
use std::collections::HashSet;

/// Relation data visible to the rule being resolved.
pub struct RuleInputs<'a> {
    /// Results of previously executed rules (searched first).
    pub derived: &'a RelationMap,
    /// Base facts.
    pub base: &'a RelationMap,
}

impl RuleInputs<'_> {
    fn relation(&self, name: &str) -> &Relation {
        static EMPTY: Relation = Relation::new();
        self.derived
            .get(name)
            .or_else(|| self.base.get(name))
            .unwrap_or(&EMPTY)
    }
}

/// Replace every `HnswScan` in `ir` and return the synthetic relations to load.
///
/// `rule_idx` keeps synthetic names unique across rules. `recursive` rejects
/// variable queries, whose bindings would not exist before the fixpoint.
pub fn resolve_rule(
    ir: &mut IRNode,
    search: Option<&HnswSearchFn>,
    inputs: &RuleInputs<'_>,
    rule_idx: usize,
    recursive: bool,
) -> Result<Vec<(String, Relation)>, String> {
    if !contains_hnsw_scan(ir) {
        return Ok(Vec::new());
    }
    let search = search.ok_or(
        "hnsw_nearest: this knowledge graph has no vector index. \
         Create one with `.index create <name> on <relation>(<column>)`.",
    )?;
    let mut resolver = Resolver {
        search,
        inputs,
        rule_idx,
        recursive,
        relations: Vec::new(),
    };
    resolver.resolve_branch(ir)?;
    Ok(resolver.relations)
}

/// True if the tree has an `HnswScan` node.
pub fn contains_hnsw_scan(ir: &IRNode) -> bool {
    match ir {
        IRNode::HnswScan { .. } => true,
        IRNode::Scan { .. } => false,
        IRNode::Map { input, .. }
        | IRNode::Filter { input, .. }
        | IRNode::Distinct { input }
        | IRNode::Aggregate { input, .. }
        | IRNode::Compute { input, .. }
        | IRNode::FlatMap { input, .. } => contains_hnsw_scan(input),
        IRNode::Join { left, right, .. }
        | IRNode::Antijoin { left, right, .. }
        | IRNode::JoinFlatMap { left, right, .. } => {
            contains_hnsw_scan(left) || contains_hnsw_scan(right)
        }
        IRNode::Union { inputs } => inputs.iter().any(contains_hnsw_scan),
    }
}

struct Resolver<'a> {
    search: &'a HnswSearchFn,
    inputs: &'a RuleInputs<'a>,
    rule_idx: usize,
    recursive: bool,
    relations: Vec<(String, Relation)>,
}

/// A positive scan that may bind a query variable: (relation, schema).
type Binding = (String, Vec<String>);

impl Resolver<'_> {
    /// Each `Union` input is a separate rule clause with its own variables.
    fn resolve_branch(&mut self, ir: &mut IRNode) -> Result<(), String> {
        let mut bindings = Vec::new();
        collect_bindings(ir, &mut bindings);
        self.rewrite(ir, &bindings)
    }

    fn rewrite(&mut self, ir: &mut IRNode, bindings: &[Binding]) -> Result<(), String> {
        match ir {
            IRNode::HnswScan { .. } => {
                let (relation, schema, tuples) = self.search_node(ir, bindings)?;
                self.relations
                    .push((relation.clone(), Relation::from(tuples)));
                *ir = IRNode::Scan { relation, schema };
                Ok(())
            }
            IRNode::Scan { .. } => Ok(()),
            IRNode::Union { inputs } => inputs
                .iter_mut()
                .try_for_each(|input| self.resolve_branch(input)),
            IRNode::Map { input, .. }
            | IRNode::Filter { input, .. }
            | IRNode::Distinct { input }
            | IRNode::Aggregate { input, .. }
            | IRNode::Compute { input, .. }
            | IRNode::FlatMap { input, .. } => self.rewrite(input, bindings),
            IRNode::Join { left, right, .. }
            | IRNode::Antijoin { left, right, .. }
            | IRNode::JoinFlatMap { left, right, .. } => {
                self.rewrite(left, bindings)?;
                self.rewrite(right, bindings)
            }
        }
    }

    fn search_node(
        &self,
        ir: &IRNode,
        bindings: &[Binding],
    ) -> Result<(String, Vec<String>, Vec<Tuple>), String> {
        let IRNode::HnswScan {
            index_name,
            query,
            k,
            ef_search,
            output_schema,
        } = ir
        else {
            unreachable!("search_node is only called on HnswScan");
        };
        let relation = format!("__hnsw_result_{}_{}__", self.rule_idx, self.relations.len());
        let search = |q: &[f32]| (self.search)(index_name, q, *k, *ef_search);

        let tuples = match query {
            IRExpression::VectorLiteral(q) => search(q)?
                .into_iter()
                .map(|(id, dist)| Tuple::new(vec![id, Value::Float64(dist)]))
                .collect(),
            IRExpression::Column(0) => {
                let var = &output_schema[0];
                if self.recursive {
                    return Err(format!(
                        "hnsw_nearest(\"{index_name}\", {var}, ...): a variable query vector \
                         is not supported in recursive rules. Compute the query vectors in a \
                         separate non-recursive rule."
                    ));
                }
                let mut tuples = Vec::new();
                for qv in self.candidate_vectors(index_name, var, bindings)? {
                    let Value::Vector(v) = &qv else {
                        return Err(format!(
                            "hnsw_nearest(\"{index_name}\", {var}, ...): {var} is bound to \
                             {qv:?}, but the query must be a vector"
                        ));
                    };
                    for (id, dist) in search(v)? {
                        tuples.push(Tuple::new(vec![qv.clone(), id, Value::Float64(dist)]));
                    }
                }
                tuples
            }
            other => {
                return Err(format!(
                    "hnsw_nearest(\"{index_name}\", ...): query must be a vector literal \
                     or a variable, got {other:?}"
                ));
            }
        };
        Ok((relation, output_schema.clone(), tuples))
    }

    /// Distinct values of `var` in the smallest binding relation.
    fn candidate_vectors(
        &self,
        index_name: &str,
        var: &str,
        bindings: &[Binding],
    ) -> Result<Vec<Value>, String> {
        let smallest = bindings
            .iter()
            .filter_map(|(relation, schema)| {
                let col = schema.iter().position(|c| c == var)?;
                Some((self.inputs.relation(relation), col))
            })
            .min_by_key(|(tuples, _)| tuples.len())
            .ok_or_else(|| {
                format!(
                    "hnsw_nearest(\"{index_name}\", {var}, ...): query variable {var} must be \
                     bound by a positive atom in the same rule body, e.g. \
                     `?query_vec({var}), hnsw_nearest(\"{index_name}\", {var}, 10, Id, Dist)`"
                )
            })?;
        let (tuples, col) = smallest;
        let mut seen = HashSet::new();
        Ok(tuples
            .iter()
            .filter_map(|t| t.get(col))
            .filter(|v| seen.insert(*v))
            .cloned()
            .collect())
    }
}

/// Positive scans in one clause. Stops at `Union` (other clauses) and skips
/// the right side of `Antijoin` (negated atoms bind nothing).
fn collect_bindings(ir: &IRNode, out: &mut Vec<Binding>) {
    match ir {
        IRNode::Scan { relation, schema } => out.push((relation.clone(), schema.clone())),
        IRNode::HnswScan { .. } | IRNode::Union { .. } => {}
        IRNode::Map { input, .. }
        | IRNode::Filter { input, .. }
        | IRNode::Distinct { input }
        | IRNode::Aggregate { input, .. }
        | IRNode::Compute { input, .. }
        | IRNode::FlatMap { input, .. } => collect_bindings(input, out),
        IRNode::Antijoin { left, .. } => collect_bindings(left, out),
        IRNode::Join { left, right, .. } | IRNode::JoinFlatMap { left, right, .. } => {
            collect_bindings(left, out);
            collect_bindings(right, out);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn echo_search() -> HnswSearchFn {
        // Returns one hit whose id is the query's first component.
        Arc::new(|_, q, _, _| Ok(vec![(Value::Int64(q[0] as i64), 0.5)]))
    }

    fn hnsw_var(var: &str) -> IRNode {
        IRNode::HnswScan {
            index_name: "idx".to_string(),
            query: IRExpression::Column(0),
            k: 1,
            ef_search: None,
            output_schema: vec![var.to_string(), "Id".to_string(), "D".to_string()],
        }
    }

    fn scan(relation: &str, schema: &[&str]) -> IRNode {
        IRNode::Scan {
            relation: relation.to_string(),
            schema: schema.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    fn join(left: IRNode, right: IRNode) -> IRNode {
        IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec![],
        }
    }

    fn vec_tuple(v: &[f32]) -> Tuple {
        Tuple::new(vec![Value::Vector(Arc::new(v.to_vec()))])
    }

    #[test]
    fn test_resolve_variable_query_searches_each_distinct_binding() {
        let mut base = RelationMap::new();
        base.insert(
            "q".to_string(),
            vec![vec_tuple(&[1.0]), vec_tuple(&[2.0]), vec_tuple(&[1.0])].into(),
        );
        let derived = RelationMap::new();
        let inputs = RuleInputs {
            derived: &derived,
            base: &base,
        };
        let mut ir = join(scan("q", &["QV"]), hnsw_var("QV"));
        let rels = resolve_rule(&mut ir, Some(&echo_search()), &inputs, 3, false).unwrap();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].0, "__hnsw_result_3_0__");
        assert_eq!(rels[0].1.len(), 2);
        assert!(!contains_hnsw_scan(&ir));
    }

    #[test]
    fn test_resolve_unbound_variable_errors() {
        let empty = RelationMap::new();
        let inputs = RuleInputs {
            derived: &empty,
            base: &empty,
        };
        let mut ir = join(scan("other", &["X"]), hnsw_var("QV"));
        let err = resolve_rule(&mut ir, Some(&echo_search()), &inputs, 0, false).unwrap_err();
        assert!(err.contains("must be bound by a positive atom"), "{err}");
    }

    #[test]
    fn test_resolve_negated_atom_does_not_bind() {
        let mut base = RelationMap::new();
        base.insert("neg".to_string(), vec![vec_tuple(&[1.0])].into());
        let derived = RelationMap::new();
        let inputs = RuleInputs {
            derived: &derived,
            base: &base,
        };
        let mut ir = IRNode::Antijoin {
            left: Box::new(hnsw_var("QV")),
            right: Box::new(scan("neg", &["QV"])),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec![],
        };
        let err = resolve_rule(&mut ir, Some(&echo_search()), &inputs, 0, false).unwrap_err();
        assert!(err.contains("must be bound"), "{err}");
    }

    #[test]
    fn test_resolve_variable_in_recursive_rule_errors() {
        let empty = RelationMap::new();
        let inputs = RuleInputs {
            derived: &empty,
            base: &empty,
        };
        let mut ir = join(scan("q", &["QV"]), hnsw_var("QV"));
        let err = resolve_rule(&mut ir, Some(&echo_search()), &inputs, 0, true).unwrap_err();
        assert!(err.contains("recursive"), "{err}");
    }

    #[test]
    fn test_resolve_non_vector_binding_errors() {
        let mut base = RelationMap::new();
        base.insert(
            "q".to_string(),
            vec![Tuple::new(vec![Value::Int64(1)])].into(),
        );
        let derived = RelationMap::new();
        let inputs = RuleInputs {
            derived: &derived,
            base: &base,
        };
        let mut ir = join(scan("q", &["QV"]), hnsw_var("QV"));
        let err = resolve_rule(&mut ir, Some(&echo_search()), &inputs, 0, false).unwrap_err();
        assert!(err.contains("must be a vector"), "{err}");
    }

    #[test]
    fn test_resolve_without_search_fn_errors() {
        let empty = RelationMap::new();
        let inputs = RuleInputs {
            derived: &empty,
            base: &empty,
        };
        let mut ir = hnsw_var("QV");
        let err = resolve_rule(&mut ir, None, &inputs, 0, false).unwrap_err();
        assert!(
            err.contains("hnsw_nearest") && err.contains(".index create"),
            "{err}"
        );
    }
}
