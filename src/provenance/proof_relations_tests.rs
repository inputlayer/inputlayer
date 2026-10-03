use super::*;
use crate::value::Tuple;

fn relations(rows: Vec<Vec<Value>>) -> RelationMap {
    let mut map = RelationMap::new();
    map.insert("r".to_string(), rows.into_iter().map(Tuple::new).collect());
    map
}

/// Candidates of one lookup, as owned value rows.
fn lookup(relations: &ProofRelations<'_>, columns: &[usize], key: &[Value]) -> Vec<Vec<Value>> {
    let key: Vec<&Value> = key.iter().collect();
    relations
        .candidates("r", columns, &key)
        .map(|tuple| tuple.values().to_vec())
        .collect()
}

fn indexed(relations: &ProofRelations<'_>, columns: &[usize]) -> bool {
    relations
        .patterns
        .borrow()
        .get("r")
        .and_then(|by_columns| by_columns.get(columns))
        .is_some_and(|pattern| matches!(pattern, Pattern::Indexed(_)))
}

fn int(n: i32) -> Value {
    Value::Int32(n)
}

#[test]
fn indexed_candidates_are_the_matching_tuples_in_insertion_order() {
    let data = relations(vec![
        vec![int(1), int(10)],
        vec![int(2), int(20)],
        vec![int(1), int(30)],
        vec![int(3), int(10)],
        vec![int(1), int(10)],
    ]);
    let relations = ProofRelations::new(&data).with_scans_before_index(0);
    assert_eq!(
        lookup(&relations, &[0], &[int(1)]),
        vec![
            vec![int(1), int(10)],
            vec![int(1), int(30)],
            vec![int(1), int(10)],
        ]
    );
    assert_eq!(
        lookup(&relations, &[1], &[int(10)]),
        vec![
            vec![int(1), int(10)],
            vec![int(3), int(10)],
            vec![int(1), int(10)],
        ]
    );
    assert_eq!(
        lookup(&relations, &[0, 1], &[int(3), int(10)]),
        vec![vec![int(3), int(10)]]
    );
    assert!(lookup(&relations, &[0], &[int(9)]).is_empty());
}

#[test]
fn a_pattern_is_scanned_until_it_repeats_then_indexed_once() {
    let data = relations((0..100).map(|i| vec![int(i % 10), int(i)]).collect());
    let relations = ProofRelations::new(&data);
    for _ in 0..SCANS_BEFORE_INDEX {
        // A scan returns every tuple; the caller's equality filters.
        assert_eq!(lookup(&relations, &[0], &[int(3)]).len(), 100);
        assert!(!indexed(&relations, &[0]));
    }
    assert_eq!(lookup(&relations, &[0], &[int(3)]).len(), 10);
    assert!(indexed(&relations, &[0]));
    // Another column set is a separate pattern.
    assert!(!indexed(&relations, &[1]));

    let first = relations
        .index("r", &data["r"], &[0])
        .map(|i| Rc::as_ptr(&i));
    let again = relations
        .index("r", &data["r"], &[0])
        .map(|i| Rc::as_ptr(&i));
    assert!(first.is_some() && first == again, "the index is built once");
}

#[test]
fn integers_of_either_width_and_signed_zeros_share_a_key() {
    let data = relations(vec![
        vec![Value::Int64(7)],
        vec![int(7)],
        vec![Value::Float64(-0.0)],
        vec![Value::Float64(0.0)],
        vec![Value::string("7")],
    ]);
    let relations = ProofRelations::new(&data).with_scans_before_index(0);
    assert_eq!(
        lookup(&relations, &[0], &[int(7)]),
        vec![vec![Value::Int64(7)], vec![int(7)]]
    );
    assert_eq!(
        lookup(&relations, &[0], &[Value::Int64(7)]),
        vec![vec![Value::Int64(7)], vec![int(7)]]
    );
    assert_eq!(
        lookup(&relations, &[0], &[Value::Float64(0.0)]),
        vec![vec![Value::Float64(-0.0)], vec![Value::Float64(0.0)]]
    );
    assert_eq!(
        lookup(&relations, &[0], &[Value::string("7")]),
        vec![vec![Value::string("7")]]
    );
}

#[test]
fn tuples_lacking_a_bound_column_stay_candidates_in_order() {
    let data = relations(vec![
        vec![int(1)],
        vec![int(1), int(5)],
        vec![int(2), int(5)],
        vec![int(1)],
        vec![int(3), int(5)],
    ]);
    let relations = ProofRelations::new(&data).with_scans_before_index(0);
    assert_eq!(
        lookup(&relations, &[1], &[int(5)]),
        vec![
            vec![int(1)],
            vec![int(1), int(5)],
            vec![int(2), int(5)],
            vec![int(1)],
            vec![int(3), int(5)],
        ]
    );
    assert_eq!(
        lookup(&relations, &[1], &[int(6)]),
        vec![vec![int(1)], vec![int(1)]]
    );
}

#[test]
fn contains_uses_exact_tuple_equality_with_or_without_an_index() {
    let data = relations(vec![vec![int(1), int(2)], vec![int(2), int(3)]]);
    for scans in [0, usize::MAX] {
        let relations = ProofRelations::new(&data).with_scans_before_index(scans);
        for _ in 0..3 {
            assert!(relations.contains("r", &Tuple::new(vec![int(2), int(3)])));
            assert!(!relations.contains("r", &Tuple::new(vec![int(3), int(2)])));
            assert!(!relations.contains("r", &Tuple::new(vec![int(2)])));
            // Exact equality, as before: widths are not unified here.
            assert!(!relations.contains("r", &Tuple::new(vec![Value::Int64(1), int(2)])));
            assert!(!relations.contains("missing", &Tuple::new(vec![int(1)])));
        }
    }
}

#[test]
fn unbound_lookups_and_missing_relations_scan() {
    let data = relations(vec![vec![int(1)], vec![int(2)]]);
    let relations = ProofRelations::new(&data).with_scans_before_index(0);
    assert_eq!(lookup(&relations, &[], &[]).len(), 2);
    assert!(!indexed(&relations, &[]));
    let key = [&int(1)];
    assert_eq!(relations.candidates("missing", &[0], &key).count(), 0);
}
