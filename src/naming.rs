//! Canonical grammar for knowledge graph and relation names.
//!
//! A name starts with `a-z`, contains only `a-z`, `0-9` and `_`, and fits the
//! byte limit for its kind. Persist shards are keyed `"{kg}:{relation}"`, so
//! keeping `:` out of both halves makes the key unambiguous.

use crate::auth::INTERNAL_KG;

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

/// Split a shard key into `(kg, relation)`, picking the longest KG in `kgs`
/// that prefixes it. Longest match keeps legacy names containing `:` (e.g.
/// `user` and `user:42`) from claiming each other's shards.
pub(crate) fn shard_owner<'k, 's>(
    shard: &'s str,
    kgs: impl IntoIterator<Item = &'k str>,
) -> Option<(&'k str, &'s str)> {
    kgs.into_iter()
        .filter_map(|kg| {
            let relation = shard.strip_prefix(kg)?.strip_prefix(':')?;
            (!relation.is_empty()).then_some((kg, relation))
        })
        .max_by_key(|(kg, _)| kg.len())
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
        let kgs = ["user", "user:42", "default"];
        assert_eq!(shard_owner("user:42:fact", kgs), Some(("user:42", "fact")));
        assert_eq!(shard_owner("user:fact", kgs), Some(("user", "fact")));
        assert_eq!(shard_owner("default:edge", kgs), Some(("default", "edge")));
        assert_eq!(shard_owner("other:edge", kgs), None);
        assert_eq!(shard_owner("user:", kgs), None);
        assert_eq!(shard_owner("username:x", kgs), None);
    }
}
