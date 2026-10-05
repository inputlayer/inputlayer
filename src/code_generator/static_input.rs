//! Pre-filtering of static input before it enters the dataflow.
//!
//! A scan of static input data (not a live collection of an iterative scope),
//! optionally under filters, can be evaluated directly over the shared
//! [`Relation`]. Doing so before streaming into differential dataflow keeps
//! DD from ingesting, arranging and discarding tuples that cannot contribute:
//!
//! - `Filter(...Scan)` streams only matching tuples.
//! - `Join` of two such sources, one much smaller than the other, streams only
//!   the larger side's tuples whose join key occurs on the smaller side. An
//!   inner join cannot produce output from the others.
//!
//! Both are exact: every tuple DD would have used is still streamed, with the
//! same multiplicity.

use super::CodeGenerator;
use crate::ir::IRNode;
use crate::semiring_types::DiffType;
use crate::value::{Relation, RelationMap, Tuple, Value};
use differential_dataflow::collection::vec::Collection;
use differential_dataflow::lattice::Lattice;
use std::collections::{HashMap, HashSet};
use timely::dataflow::operators::vec::{Map, ToStream};
use timely::dataflow::Scope;

static EMPTY: Relation = Relation::new();

/// Join pre-filtering pays off only when the small side is this many times
/// smaller than the large side's relation.
const PREFILTER_RATIO: usize = 4;

type TuplePredicate = Box<dyn Fn(&Tuple) -> bool + Send + Sync + 'static>;

/// A static relation with the filters applied above its scan.
pub(super) struct StaticSource<'a> {
    relation: &'a Relation,
    predicates: Vec<TuplePredicate>,
}

impl StaticSource<'_> {
    fn matches(&self, tuple: &Tuple) -> bool {
        self.predicates.iter().all(|p| p(tuple))
    }

    fn collect(&self) -> Vec<Tuple> {
        self.relation
            .iter()
            .filter(|t| self.matches(t))
            .cloned()
            .collect()
    }
}

impl CodeGenerator {
    /// `node` as a filtered scan of static input, if it is one.
    pub(super) fn static_source<'a, G, R: DiffType>(
        node: &IRNode,
        input_data: &'a RelationMap,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Option<StaticSource<'a>>
    where
        G: Scope,
    {
        Self::source_unless(node, input_data, &|relation| {
            live.is_some_and(|l| l.contains_key(relation))
        })
    }

    /// Rows a `Filter(...Scan)` keeps of its relation in `input_data`, the
    /// tuples [`Self::prefiltered_scan`] would stream, or `None` when `node`
    /// is not one or scans a relation `derived` says is computed.
    pub(crate) fn count_filtered_scan(
        node: &IRNode,
        input_data: &RelationMap,
        derived: &dyn Fn(&str) -> bool,
    ) -> Option<u64> {
        if !matches!(node, IRNode::Filter { .. }) {
            return None;
        }
        let source = Self::source_unless(node, input_data, derived)?;
        Some(source.relation.iter().filter(|t| source.matches(t)).count() as u64)
    }

    fn source_unless<'a>(
        node: &IRNode,
        input_data: &'a RelationMap,
        excluded: &dyn Fn(&str) -> bool,
    ) -> Option<StaticSource<'a>> {
        match node {
            IRNode::Scan { relation, .. } => {
                if excluded(relation) {
                    return None;
                }
                Some(StaticSource {
                    relation: input_data.get(relation).unwrap_or(&EMPTY),
                    predicates: Vec::new(),
                })
            }
            IRNode::Filter { input, predicate } => {
                let mut source = Self::source_unless(input, input_data, excluded)?;
                source
                    .predicates
                    .push(Self::predicate_to_tuple_fn(predicate));
                Some(source)
            }
            _ => None,
        }
    }

    /// Stream tuples already in memory as a collection.
    pub(super) fn collection_from_tuples<G, R: DiffType>(
        scope: &mut G,
        tuples: Vec<Tuple>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        Collection::new(
            tuples
                .to_stream(scope)
                .map(|x| (x, Default::default(), R::one())),
        )
    }

    /// `Filter(...Scan)` over static input, filtered before entering DD.
    pub(super) fn prefiltered_scan<G, R: DiffType>(
        scope: &mut G,
        ir: &IRNode,
        input_data: &RelationMap,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Option<Collection<G, Tuple, R>>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        if !matches!(ir, IRNode::Filter { .. }) {
            return None;
        }
        let source = Self::static_source(ir, input_data, live)?;
        Some(Self::collection_from_tuples(scope, source.collect()))
    }

    /// Join inputs with the larger static side reduced to tuples whose key
    /// occurs on the smaller static side. `None` when not applicable.
    pub(super) fn prefiltered_join_inputs<G, R: DiffType>(
        left: &IRNode,
        right: &IRNode,
        left_keys: &[usize],
        right_keys: &[usize],
        input_data: &RelationMap,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Option<(Vec<Tuple>, Vec<Tuple>)>
    where
        G: Scope,
    {
        if left_keys.is_empty() || left_keys.len() != right_keys.len() {
            return None;
        }
        let left_source = Self::static_source(left, input_data, live)?;
        let right_source = Self::static_source(right, input_data, live)?;
        let left_is_small = left_source.relation.len() <= right_source.relation.len();
        let (small, small_keys, large, large_keys) = if left_is_small {
            (left_source, left_keys, right_source, right_keys)
        } else {
            (right_source, right_keys, left_source, left_keys)
        };
        let small_tuples = small.collect();
        if small_tuples.len().saturating_mul(PREFILTER_RATIO) > large.relation.len() {
            return None;
        }
        let keys: HashSet<Tuple> = small_tuples
            .iter()
            .map(|t| t.from_indices(small_keys))
            .collect();
        // Float keys compare by bits under Eq but by value under Ord (which DD
        // uses), so a hash lookup could drop a match; leave those to DD.
        if keys
            .iter()
            .any(|k| k.values().iter().any(|v| matches!(v, Value::Float64(_))))
        {
            return None;
        }
        let large_tuples: Vec<Tuple> = large
            .relation
            .iter()
            .filter(|t| large.matches(t) && keys.contains(&t.from_indices(large_keys)))
            .cloned()
            .collect();
        Some(if left_is_small {
            (small_tuples, large_tuples)
        } else {
            (large_tuples, small_tuples)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::ir::Predicate;

    fn scan(relation: &str) -> IRNode {
        IRNode::Scan {
            relation: relation.to_string(),
            schema: vec!["a".to_string(), "b".to_string()],
        }
    }

    fn pair(a: i64, b: i64) -> Tuple {
        Tuple::new(vec![Value::Int64(a), Value::Int64(b)])
    }

    fn sorted(mut v: Vec<Tuple>) -> Vec<Tuple> {
        v.sort();
        v
    }

    fn edges(n: i64) -> Vec<Tuple> {
        (0..n).map(|i| pair(i % 50, (i * 7) % 50)).collect()
    }

    /// Reference: same join with every tuple streamed into DD.
    fn join_reference(left: &[Tuple], right: &[Tuple]) -> Vec<Tuple> {
        let mut out: Vec<Tuple> = left
            .iter()
            .flat_map(|l| {
                right
                    .iter()
                    .filter(move |r| r.values()[0] == l.values()[1])
                    .map(move |r| l.concat(&Tuple::new(vec![r.values()[1].clone()])))
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    #[test]
    fn test_prefiltered_filter_scan_matches_dd_filter() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input("e".to_string(), edges(500));
        let ir = IRNode::Filter {
            input: Box::new(scan("e")),
            predicate: Predicate::ColumnEqConst(0, 3),
        };
        let expected: Vec<Tuple> = sorted(
            edges(500)
                .into_iter()
                .filter(|t| t.values()[0] == Value::Int64(3))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect(),
        );
        assert_eq!(sorted(codegen.execute(&ir).unwrap()), expected);
    }

    #[test]
    fn test_prefiltered_join_matches_full_join() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input("e".to_string(), edges(2000));
        // Small side: e(3, Y); large side: all of e, keyed on column 0.
        let small = IRNode::Filter {
            input: Box::new(scan("e")),
            predicate: Predicate::ColumnEqConst(0, 3),
        };
        let ir = IRNode::Map {
            input: Box::new(IRNode::Join {
                left: Box::new(small),
                right: Box::new(scan("e")),
                left_keys: vec![1],
                right_keys: vec![0],
                output_schema: vec!["a".into(), "b".into(), "c".into()],
            }),
            projection: vec![0, 1, 2],
            output_schema: vec!["a".into(), "b".into(), "c".into()],
        };
        let left: Vec<Tuple> = edges(2000)
            .into_iter()
            .filter(|t| t.values()[0] == Value::Int64(3))
            .collect();
        assert_eq!(
            sorted(codegen.execute(&ir).unwrap()),
            join_reference(&left, &edges(2000))
        );
    }

    #[test]
    fn test_prefilter_skips_float_keys() {
        let mut data = RelationMap::new();
        data.insert(
            "f".to_string(),
            Relation::from(vec![Tuple::new(vec![Value::Float64(0.0)])]),
        );
        data.insert(
            "g".to_string(),
            (0..100)
                .map(|i| Tuple::new(vec![Value::Float64(f64::from(i))]))
                .collect(),
        );
        let none = CodeGenerator::prefiltered_join_inputs::<
            timely::dataflow::scopes::Child<
                '_,
                timely::worker::Worker<timely::communication::allocator::Thread>,
                (),
            >,
            isize,
        >(&scan("f"), &scan("g"), &[0], &[0], &data, None);
        assert!(none.is_none());
    }
}
