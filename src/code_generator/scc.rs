//! Joint fixpoint for a strongly connected component of mutually recursive
//! relations.
//!
//! Every member gets its own `Variable` in one `.iterative()` scope, and each
//! member's body reads all members through those variables. Differential
//! Dataflow propagates only changes between iterations, so evaluation is
//! semi-naive across the whole SCC.

use super::{format_panic_payload, is_query_cancelled, CodeGenerator, Iter, QUERY_CANCELLED};
use crate::boolean_specialization::SemiringType;
use crate::ir::IRNode;
use crate::semiring_types::{BooleanDiff, DiffType};
use crate::value::Tuple;
use differential_dataflow::collection::vec::Collection;
use differential_dataflow::operators::iterate::Variable;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::dataflow::operators::{Inspect, Probe};
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::order::Product;

/// One SCC member, split into clauses that read no member (base) and
/// clauses that read at least one member (recursive).
struct MemberPlan {
    name: String,
    base: IRNode,
    recursive: IRNode,
    agg_in_loop: Option<(Vec<usize>, usize, bool)>,
}

impl MemberPlan {
    fn new(name: &str, ir: &IRNode, members: &HashSet<String>) -> Self {
        let clauses: Vec<IRNode> = match ir {
            IRNode::Union { inputs } => inputs.clone(),
            other => vec![other.clone()],
        };
        let (recursive, base): (Vec<IRNode>, Vec<IRNode>) = clauses.into_iter().partition(|c| {
            members
                .iter()
                .any(|m| CodeGenerator::references_relation(c, m))
        });

        let agg_in_loop = CodeGenerator::minmax_in_loop(&recursive);
        let recursive: Vec<IRNode> = if agg_in_loop.is_some() {
            recursive
                .iter()
                .map(|r| CodeGenerator::strip_top_aggregate(r).clone())
                .collect()
        } else {
            recursive
        };

        // A member with no base clause starts from its stored facts, as the
        // self-recursive path does.
        let base = if base.is_empty() {
            IRNode::Scan {
                relation: name.to_string(),
                schema: Vec::new(),
            }
        } else {
            union_of(base)
        };

        MemberPlan {
            name: name.to_string(),
            base,
            recursive: union_of(recursive),
            agg_in_loop,
        }
    }
}

fn union_of(mut nodes: Vec<IRNode>) -> IRNode {
    if nodes.len() == 1 {
        nodes.remove(0)
    } else {
        IRNode::Union { inputs: nodes }
    }
}

impl CodeGenerator {
    /// Evaluate mutually recursive relations to their joint least fixpoint.
    ///
    /// `members` pairs each relation with its (unoptimized) rule IR. Returns
    /// every member's tuples. Negation inside the SCC must be rejected by the
    /// caller (stratification); aggregates are evaluated inside the loop, as
    /// for self-recursive rules.
    pub fn execute_recursive_scc(
        &self,
        members: &[(String, IRNode)],
    ) -> Result<HashMap<String, Vec<Tuple>>, String> {
        match self.semiring_type {
            SemiringType::Boolean => self.execute_recursive_scc_typed::<BooleanDiff>(members),
            _ => self.execute_recursive_scc_typed::<isize>(members),
        }
    }

    fn execute_recursive_scc_typed<R: DiffType>(
        &self,
        members: &[(String, IRNode)],
    ) -> Result<HashMap<String, Vec<Tuple>>, String> {
        let names: HashSet<String> = members.iter().map(|(n, _)| n.clone()).collect();
        let plans: Vec<MemberPlan> = members
            .iter()
            .map(|(name, ir)| MemberPlan::new(name, ir, &names))
            .collect();

        if std::env::var("INPUTLAYER_DEBUG").is_ok() {
            let order: Vec<&str> = plans.iter().map(|p| p.name.as_str()).collect();
            eprintln!("DEBUG: joint fixpoint over SCC {order:?}");
        }

        // Net multiplicity per tuple, per member. Retractions inside the loop
        // (min/max pruning) cancel out here.
        let results: Arc<Vec<Mutex<HashMap<Tuple, isize>>>> =
            Arc::new(plans.iter().map(|_| Mutex::new(HashMap::new())).collect());
        let results_clone = Arc::clone(&results);
        let input_data = self.input_tuples.clone();

        catch_unwind(AssertUnwindSafe(|| {
            timely::execute_directly(move |worker| {
                let probe = ProbeHandle::new();

                worker.dataflow::<(), _, _>(|scope| {
                    let bases: Vec<Collection<_, Tuple, R>> = plans
                        .iter()
                        .map(|p| {
                            Self::generate_collection_tuples::<_, R>(
                                scope,
                                &p.base,
                                &input_data,
                                None,
                            )
                        })
                        .collect();

                    let outputs = scope.iterative::<Iter, _, _>(|inner| {
                        let mut live: HashMap<String, Collection<_, Tuple, R>> = HashMap::new();
                        for (name, tuples) in &input_data {
                            let coll: Collection<_, Tuple, R> = Collection::new(
                                tuples
                                    .clone()
                                    .to_stream(inner)
                                    .map(|x| (x, Product::default(), R::one())),
                            );
                            live.insert(name.clone(), coll);
                        }
                        let mut variables = Vec::with_capacity(plans.len());
                        for plan in &plans {
                            let (variable, collection) = Variable::new(inner, Product::new((), 1));
                            live.insert(plan.name.clone(), collection);
                            variables.push(variable);
                        }

                        plans
                            .iter()
                            .zip(variables)
                            .zip(bases)
                            .map(|((plan, variable), base)| {
                                let recursive = Self::generate_collection_tuples::<_, R>(
                                    inner,
                                    &plan.recursive,
                                    &input_data,
                                    Some(&live),
                                );
                                let combined = base.enter(inner).concat(recursive);
                                let next =
                                    Self::fixpoint_dedup(combined, plan.agg_in_loop.as_ref());
                                variable.set(next.clone());
                                next.leave()
                            })
                            .collect::<Vec<_>>()
                    });

                    for (idx, output) in outputs.into_iter().enumerate() {
                        let results_ref = Arc::clone(&results_clone);
                        output
                            .inner
                            .inspect(move |(data, _time, diff)| {
                                let mut guard = results_ref[idx].lock();
                                *guard.entry(data.clone()).or_insert(0) += diff.to_count();
                            })
                            .probe_with(&probe);
                    }
                });

                while !probe.done() {
                    if is_query_cancelled() {
                        break;
                    }
                    worker.step();
                    std::thread::yield_now();
                }
            });
        }))
        .map_err(|e| {
            format!(
                "Internal error in query execution: {}",
                format_panic_payload(e)
            )
        })?;

        let mut out: HashMap<String, Vec<Tuple>> = HashMap::new();
        for ((name, _), counts) in members.iter().zip(results.iter()) {
            let tuples: Vec<Tuple> = counts
                .lock()
                .iter()
                .filter(|(_, &count)| count > 0)
                .map(|(t, _)| t.clone())
                .collect();
            out.insert(name.clone(), tuples);
        }

        if is_query_cancelled() {
            return Err(QUERY_CANCELLED.to_string());
        }

        Ok(out)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn ints(rows: &[&[i64]]) -> Vec<Tuple> {
        rows.iter()
            .map(|r| Tuple::new(r.iter().map(|&v| Value::Int64(v)).collect()))
            .collect()
    }

    fn scan(rel: &str, cols: &[&str]) -> IRNode {
        IRNode::Scan {
            relation: rel.to_string(),
            schema: cols.iter().map(ToString::to_string).collect(),
        }
    }

    /// `head(N) <- succ(M, N), other(M)` as IR: join on M, project N.
    fn step(other: &str) -> IRNode {
        IRNode::Map {
            input: Box::new(IRNode::Join {
                left: Box::new(scan("succ", &["M", "N"])),
                right: Box::new(scan(other, &["M"])),
                left_keys: vec![0],
                right_keys: vec![0],
                output_schema: vec!["M".into(), "N".into(), "M2".into()],
            }),
            projection: vec![1],
            output_schema: vec!["N".into()],
        }
    }

    fn sorted(mut v: Vec<Tuple>) -> Vec<Tuple> {
        v.sort();
        v
    }

    #[test]
    fn test_scc_even_odd_reaches_fixpoint() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input(
            "succ".into(),
            ints(&[&[0, 1], &[1, 2], &[2, 3], &[3, 4], &[4, 5]]),
        );
        codegen.add_input("zero".into(), ints(&[&[0]]));

        let even = IRNode::Union {
            inputs: vec![scan("zero", &["N"]), step("odd")],
        };
        let odd = step("even");
        let out = codegen
            .execute_recursive_scc(&[("even".into(), even), ("odd".into(), odd)])
            .unwrap();

        assert_eq!(sorted(out["even"].clone()), ints(&[&[0], &[2], &[4]]));
        assert_eq!(sorted(out["odd"].clone()), ints(&[&[1], &[3], &[5]]));
    }

    #[test]
    fn test_scc_member_without_base_clause_uses_stored_facts() {
        // a <- b, b <- a, with a stored fact for a only
        let mut codegen = CodeGenerator::new();
        codegen.add_input("a".into(), ints(&[&[7]]));
        let out = codegen
            .execute_recursive_scc(&[
                ("a".into(), scan("b", &["X"])),
                ("b".into(), scan("a", &["X"])),
            ])
            .unwrap();
        assert_eq!(out["a"], ints(&[&[7]]));
        assert_eq!(out["b"], ints(&[&[7]]));
    }
}
