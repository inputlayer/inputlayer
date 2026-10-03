//! Canonical grammar for knowledge graph and relation names.
//!
//! A name starts with `a-z`, contains only `a-z`, `0-9` and `_`, and fits the
//! byte limit for its kind. Persist shards are keyed `"{kg}:{relation}"`, so
//! keeping `:` out of both halves makes the key unambiguous.

use crate::auth::INTERNAL_KG;
use std::collections::HashSet;

/// Maximum byte length of a knowledge graph name.
pub const MAX_KG_NAME_BYTES: usize = 128;

/// Maximum byte length of a relation name.
pub const MAX_RELATION_NAME_BYTES: usize = 256;

/// Validate a knowledge graph name. The system KG `_internal` is also accepted.
pub fn validate_kg_name(name: &str) -> Result<(), String> {
    if name == INTERNAL_KG {
        return Ok(());
    }
    validate("Knowledge graph", name, MAX_KG_NAME_BYTES)
}

/// Validate a relation name.
pub fn validate_relation_name(name: &str) -> Result<(), String> {
    validate("Relation", name, MAX_RELATION_NAME_BYTES)
}

fn validate(kind: &str, name: &str, max: usize) -> Result<(), String> {
    let Some(first) = name.bytes().next() else {
        return Err(format!("{kind} name cannot be empty"));
    };
    if name.len() > max {
        return Err(format!(
            "{kind} name too long: {} bytes (max {max})",
            name.len()
        ));
    }
    if !first.is_ascii_lowercase() {
        return Err(format!(
            "{kind} name '{name}' must start with a lowercase letter (a-z)"
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(format!(
            "{kind} name '{name}' may only contain a-z, 0-9 and '_'"
        ));
    }
    Ok(())
}

/// Shard ownership index over the known KG names, built once per startup or
/// drop cleanup.
///
/// A shard key is `"{kg}:{relation}"` and is owned by the longest known KG
/// that prefixes it up to a `:`, leaving a non-empty relation. Longest match
/// keeps legacy names containing `:` (e.g. `user` and `user:42`) from
/// claiming each other's shards. Each lookup costs hash probes at the key's
/// `:` positions, independent of the number of KGs: one probe when no known
/// KG contains `:`, which canonical names cannot.
pub(crate) struct ShardOwners<'k> {
    kgs: HashSet<&'k str>,
    /// Whether some known KG is a legacy name containing `:`.
    legacy: bool,
}

impl<'k> ShardOwners<'k> {
    pub(crate) fn new(kgs: impl IntoIterator<Item = &'k str>) -> Self {
        let kgs: HashSet<&'k str> = kgs.into_iter().collect();
        let legacy = kgs.iter().any(|kg| kg.contains(':'));
        Self { kgs, legacy }
    }

    /// Split `shard` into `(kg, relation)` for its owning KG, if any.
    pub(crate) fn owner<'s>(&self, shard: &'s str) -> Option<(&'k str, &'s str)> {
        if self.legacy {
            shard
                .match_indices(':')
                .rev()
                .find_map(|(at, _)| self.split(shard, at))
        } else {
            self.split(shard, shard.find(':')?)
        }
    }

    fn split<'s>(&self, shard: &'s str, at: usize) -> Option<(&'k str, &'s str)> {
        let relation = &shard[at + 1..];
        let kg = self.kgs.get(&shard[..at])?;
        (!relation.is_empty()).then_some((*kg, relation))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical_names() {
        for name in ["a", "edge", "my_rel", "r2d2", "a_", "kg_1"] {
            assert!(validate_kg_name(name).is_ok(), "{name}");
            assert!(validate_relation_name(name).is_ok(), "{name}");
        }
        assert!(validate_kg_name(INTERNAL_KG).is_ok());
    }

    #[test]
    fn rejects_non_canonical_names() {
        for name in [
            "", "user:42", "Foo", "fooBar", "__dunder", "_x", "1abc", "a-b", "a.b", "a/b", "a b",
            "é", "a\0",
        ] {
            assert!(validate_kg_name(name).is_err(), "kg {name:?}");
            assert!(validate_relation_name(name).is_err(), "relation {name:?}");
        }
        assert!(validate_relation_name(INTERNAL_KG).is_err());
    }

    #[test]
    fn enforces_byte_limits() {
        assert!(validate_kg_name(&"a".repeat(MAX_KG_NAME_BYTES)).is_ok());
        let err = validate_kg_name(&"a".repeat(MAX_KG_NAME_BYTES + 1)).unwrap_err();
        assert!(err.contains("too long"), "{err}");
        assert!(validate_relation_name(&"a".repeat(MAX_RELATION_NAME_BYTES)).is_ok());
        assert!(validate_relation_name(&"a".repeat(MAX_RELATION_NAME_BYTES + 1)).is_err());
    }

    #[test]
    fn shard_owner_prefers_longest_kg() {
        let owners = ShardOwners::new(["user", "user:42", "default"]);
        assert_eq!(owners.owner("user:42:fact"), Some(("user:42", "fact")));
        assert_eq!(owners.owner("user:fact"), Some(("user", "fact")));
        assert_eq!(owners.owner("default:edge"), Some(("default", "edge")));
        assert_eq!(owners.owner("other:edge"), None);
        assert_eq!(owners.owner("user:"), None);
        assert_eq!(owners.owner("username:x"), None);
    }

    #[test]
    fn canonical_owner_keeps_colons_in_relation() {
        let owners = ShardOwners::new(["user", "default"]);
        assert_eq!(owners.owner("user:42:fact"), Some(("user", "42:fact")));
        assert_eq!(owners.owner("user:edge"), Some(("user", "edge")));
        assert_eq!(owners.owner("useredge"), None);
        assert_eq!(owners.owner(":edge"), None);
    }

    /// The pre-index lookup: try every known KG as a prefix.
    fn scan_owner<'k, 's>(shard: &'s str, kgs: &[&'k str]) -> Option<(&'k str, &'s str)> {
        kgs.iter()
            .filter_map(|kg| {
                let relation = shard.strip_prefix(*kg)?.strip_prefix(':')?;
                (!relation.is_empty()).then_some((*kg, relation))
            })
            .max_by_key(|(kg, _)| kg.len())
    }

    /// Every string over `alphabet` of up to `max_len` characters.
    fn strings(alphabet: &[char], max_len: usize) -> Vec<String> {
        let mut all = vec![String::new()];
        let mut layer = vec![String::new()];
        for _ in 0..max_len {
            layer = layer
                .iter()
                .flat_map(|s| alphabet.iter().map(move |c| format!("{s}{c}")))
                .collect();
            all.extend(layer.iter().cloned());
        }
        all
    }

    #[test]
    fn owner_matches_scan_for_legacy_and_canonical_names() {
        let names = strings(&['a', 'b', ':'], 4);
        let shards = strings(&['a', 'b', ':'], 6);
        for stride in 1..8 {
            for offset in 0..stride {
                let mixed: Vec<&str> = names
                    .iter()
                    .skip(offset)
                    .step_by(stride)
                    .map(String::as_str)
                    .collect();
                let canonical: Vec<&str> = mixed
                    .iter()
                    .copied()
                    .filter(|kg| !kg.contains(':'))
                    .collect();
                for kgs in [mixed, canonical] {
                    let owners = ShardOwners::new(kgs.iter().copied());
                    for shard in &shards {
                        assert_eq!(
                            owners.owner(shard),
                            scan_owner(shard, &kgs),
                            "shard {shard:?}, kgs {kgs:?}"
                        );
                    }
                }
            }
        }
    }
}
