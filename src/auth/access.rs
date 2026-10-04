//! What an identity may do on one knowledge graph: a [`KgRole`], and for the
//! fact-writing roles the relations whose facts it may write.
//!
//! The roles, from most to least privileged:
//!
//! | Role      | May                                                        |
//! |-----------|------------------------------------------------------------|
//! | `owner`   | everything on the KG, including drop and access lists      |
//! | `editor`  | facts, rules, schema, indexes, drops, ontologies           |
//! | `writer`  | facts only, in every relation or in its granted relations  |
//! | `decider` | facts only, and only in its granted relations              |
//! | `viewer`  | queries only                                               |
//!
//! `writer` and `decider` never change policy: they cannot register, edit or
//! drop a rule, declare a schema, drop a relation, clear a prefix, load a
//! file or install an ontology. A `decider` with no granted relation writes
//! nothing, so a decider credential is always an explicit list of the claim
//! and decision relations it records.
//!
//! A [`KeyScope`] pins an API key to one KG and one access; the key's
//! effective access is the [`KgAccess::meet`] of that scope and its owner's
//! own access, so a scope only ever narrows what the owner may do.

use std::collections::BTreeSet;
use std::fmt;

use super::KgRole;
use crate::statement::{MetaCommand, Statement};

/// One identity's access to one knowledge graph. See the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KgAccess {
    role: KgRole,
    /// `writer` and `decider` only: the relations whose facts it may write.
    /// `None` is every relation, and only a `writer` has it.
    relations: Option<BTreeSet<String>>,
}

impl KgAccess {
    /// `role`, limited to `relations` when given. Only `writer` and `decider`
    /// take a relation list, and a `decider` needs one.
    pub fn new(role: KgRole, relations: Option<Vec<String>>) -> Result<Self, String> {
        let Some(relations) = relations else {
            if role == KgRole::Decider {
                return Err("The decider role needs the relations it may write: \
                     add 'relations <name>, ...'"
                    .to_string());
            }
            return Ok(role.into());
        };
        if !matches!(role, KgRole::Writer | KgRole::Decider) {
            return Err(format!(
                "Only the writer and decider roles take relations; '{role}' covers every relation"
            ));
        }
        if relations.is_empty() {
            return Err("A relations list needs at least one relation".to_string());
        }
        for relation in &relations {
            crate::naming::validate_relation_name(relation)?;
        }
        Ok(Self {
            role,
            relations: Some(relations.into_iter().collect()),
        })
    }

    pub fn role(&self) -> KgRole {
        self.role
    }

    /// The relations a `writer` or `decider` may write; `None` for every
    /// relation (and for the roles that take no list).
    pub fn relations(&self) -> Option<&BTreeSet<String>> {
        self.relations.as_ref()
    }

    /// Whether this access may write facts of `relation`.
    pub fn may_write(&self, relation: &str) -> bool {
        match self.role {
            KgRole::Owner | KgRole::Editor => true,
            KgRole::Writer | KgRole::Decider => self
                .relations
                .as_ref()
                .is_none_or(|relations| relations.contains(relation)),
            KgRole::Viewer => false,
        }
    }

    /// The access both `self` and `other` allow: the lesser role, writing only
    /// relations both may write.
    #[must_use]
    pub fn meet(&self, other: &Self) -> Self {
        let role = if rank(self.role) <= rank(other.role) {
            self.role
        } else {
            other.role
        };
        if !matches!(role, KgRole::Writer | KgRole::Decider) {
            return role.into();
        }
        let relations = match (self.fact_relations(), other.fact_relations()) {
            (None, None) => None,
            (Some(only), None) | (None, Some(only)) => Some(only.clone()),
            (Some(a), Some(b)) => Some(a.intersection(b).cloned().collect()),
        };
        Self { role, relations }
    }

    /// The relations whose facts this access may write; `None` for every
    /// relation.
    fn fact_relations(&self) -> Option<&BTreeSet<String>> {
        match self.role {
            KgRole::Writer | KgRole::Decider => self.relations.as_ref(),
            KgRole::Owner | KgRole::Editor | KgRole::Viewer => None,
        }
    }
}

impl From<KgRole> for KgAccess {
    /// `role` over every relation; a `decider` this way writes nothing.
    fn from(role: KgRole) -> Self {
        let relations = (role == KgRole::Decider).then(BTreeSet::new);
        Self { role, relations }
    }
}

impl fmt::Display for KgAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.role)?;
        if let Some(relations) = &self.relations {
            let names: Vec<&str> = relations.iter().map(String::as_str).collect();
            write!(f, " (relations {})", names.join(", "))?;
        }
        Ok(())
    }
}

fn rank(role: KgRole) -> u8 {
    match role {
        KgRole::Viewer => 0,
        KgRole::Decider => 1,
        KgRole::Writer => 2,
        KgRole::Editor => 3,
        KgRole::Owner => 4,
    }
}

/// An API key's scope: the one KG it may use, and its access there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyScope {
    pub kg: String,
    pub access: KgAccess,
}

impl KeyScope {
    /// A scope on `kg`. An `owner` scope is refused: access lists stay with
    /// users, not keys.
    pub fn new(kg: &str, access: KgAccess) -> Result<Self, String> {
        crate::naming::validate_kg_name(kg)?;
        if kg == super::INTERNAL_KG {
            return Err(format!("An API key cannot be scoped to '{kg}'"));
        }
        if access.role() == KgRole::Owner {
            return Err("An API key cannot carry the owner role; \
                 use editor, writer, decider or viewer"
                .to_string());
        }
        Ok(Self {
            kg: kg.to_string(),
            access,
        })
    }
}

impl fmt::Display for KeyScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let role = self.access.role();
        write!(f, "{role} on {}", self.kg)?;
        if let Some(relations) = self.access.relations() {
            let names: Vec<&str> = relations.iter().map(String::as_str).collect();
            write!(f, " (relations {})", names.join(", "))?;
        }
        Ok(())
    }
}

/// A relation list as stored in `_internal`: `*` for every relation, else the
/// names joined by commas.
pub(crate) fn encode_relations(relations: Option<&BTreeSet<String>>) -> String {
    relations.map_or_else(
        || "*".to_string(),
        |relations| {
            relations
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(",")
        },
    )
}

/// The inverse of [`encode_relations`].
pub(crate) fn decode_relations(stored: &str) -> Result<Option<Vec<String>>, String> {
    if stored == "*" {
        return Ok(None);
    }
    let relations: Vec<String> = stored
        .split(',')
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect();
    for relation in &relations {
        crate::naming::validate_relation_name(relation)?;
    }
    Ok(Some(relations))
}

/// The relations whose facts `stmt` writes, for the statements that write
/// facts; `None` for every other statement.
pub(crate) fn written_relations(stmt: &Statement) -> Option<Vec<&str>> {
    match stmt {
        Statement::Insert(op) => Some(vec![op.relation.as_str()]),
        Statement::Delete(op) => Some(vec![op.relation.as_str()]),
        Statement::Update(op) => Some(
            op.deletes
                .iter()
                .map(|target| target.relation.as_str())
                .chain(op.inserts.iter().map(|target| target.relation.as_str()))
                .collect(),
        ),
        _ => None,
    }
}

/// Authorization for the fact-writing roles (`writer`, `decider`) on `kg`.
pub(super) fn authorize_fact_writer(
    access: &KgAccess,
    kg: &str,
    stmt: &Statement,
) -> Result<(), String> {
    let role = access.role();
    let needs_editor = |what: &str| {
        Err(format!(
            "Permission denied: {what} needs the editor role on '{kg}'; \
             the {role} role may only write facts"
        ))
    };
    if let Some(relations) = written_relations(stmt) {
        return match relations.into_iter().find(|r| !access.may_write(r)) {
            None => Ok(()),
            Some(relation) => Err(format!(
                "Permission denied: the {role} role on '{kg}' has no write grant \
                 for relation '{relation}'"
            )),
        };
    }
    match stmt {
        // Session facts and rules last one request and change nothing stored.
        Statement::Query(_) | Statement::SessionRule(_) | Statement::Fact(_) => Ok(()),
        Statement::PersistentRule(_) => needs_editor("registering a rule"),
        Statement::SchemaDecl(_) | Statement::TypeDecl(_) => needs_editor("declaring a schema"),
        Statement::DeleteRelationOrRule(_) => needs_editor("dropping a relation or rule"),
        Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_) => Ok(()),
        Statement::Meta(cmd) => match cmd {
            MetaCommand::KgDrop(_)
            | MetaCommand::KgAclGrant { .. }
            | MetaCommand::KgAclRevoke { .. } => Err(format!(
                "Permission denied: only owners of '{kg}' can drop it or manage its access"
            )),
            // Everything a viewer may run, a fact writer may run too.
            _ if super::authorize_kg_viewer(stmt).is_ok() => Ok(()),
            MetaCommand::RelDrop(_) => needs_editor("dropping a relation"),
            MetaCommand::RuleDrop(_)
            | MetaCommand::RuleDropPrefix(_)
            | MetaCommand::RuleEdit { .. }
            | MetaCommand::RuleClear(_)
            | MetaCommand::RuleRemove { .. } => needs_editor("changing a rule"),
            MetaCommand::ClearPrefix(_) => needs_editor("clearing relations"),
            MetaCommand::Load { .. } => needs_editor("loading a file"),
            MetaCommand::OntologyInstall(_)
            | MetaCommand::OntologyRemove(_)
            | MetaCommand::OntologyUpgrade(_) => needs_editor("changing an ontology"),
            _ => needs_editor("this command"),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn rels(names: &[&str]) -> Option<Vec<String>> {
        Some(names.iter().map(|s| (*s).to_string()).collect())
    }

    #[test]
    fn new_validates_relation_lists() {
        assert!(KgAccess::new(KgRole::Decider, None).is_err());
        assert!(KgAccess::new(KgRole::Decider, rels(&[])).is_err());
        assert!(KgAccess::new(KgRole::Editor, rels(&["a"])).is_err());
        assert!(KgAccess::new(KgRole::Writer, rels(&["Bad-Name"])).is_err());
        let writer = KgAccess::new(KgRole::Writer, None).unwrap();
        assert!(writer.may_write("anything"));
        let decider = KgAccess::new(KgRole::Decider, rels(&["attempt", "decision"])).unwrap();
        assert!(decider.may_write("attempt"));
        assert!(!decider.may_write("kill_switch"));
        assert_eq!(decider.to_string(), "decider (relations attempt, decision)");
        assert!(!KgAccess::from(KgRole::Decider).may_write("attempt"));
        assert!(!KgAccess::from(KgRole::Viewer).may_write("attempt"));
    }

    #[test]
    fn meet_takes_the_lesser_role_and_the_common_relations() {
        let owner = KgAccess::from(KgRole::Owner);
        let editor = KgAccess::from(KgRole::Editor);
        let viewer = KgAccess::from(KgRole::Viewer);
        let writer = KgAccess::new(KgRole::Writer, None).unwrap();
        let ops = KgAccess::new(KgRole::Writer, rels(&["kill_switch", "eta"])).unwrap();
        let decider = KgAccess::new(KgRole::Decider, rels(&["attempt", "eta"])).unwrap();

        assert_eq!(owner.meet(&editor), editor);
        assert_eq!(owner.meet(&decider), decider);
        assert_eq!(decider.meet(&owner), decider);
        assert_eq!(editor.meet(&writer), writer);
        assert_eq!(writer.meet(&ops), ops);
        assert_eq!(decider.meet(&viewer), viewer);
        let both = ops.meet(&decider);
        assert_eq!(both.role(), KgRole::Decider);
        assert!(both.may_write("eta"));
        assert!(!both.may_write("attempt") && !both.may_write("kill_switch"));
    }

    #[test]
    fn relation_lists_round_trip_through_storage() {
        for access in [
            KgAccess::new(KgRole::Writer, None).unwrap(),
            KgAccess::new(KgRole::Decider, rels(&["b", "a"])).unwrap(),
        ] {
            let stored = encode_relations(access.relations());
            let decoded = KgAccess::new(access.role(), decode_relations(&stored).unwrap());
            assert_eq!(decoded.unwrap(), access);
        }
        assert!(decode_relations("ok,Not Ok").is_err());
    }

    #[test]
    fn key_scope_refuses_owner() {
        assert!(KeyScope::new("shop", KgRole::Owner.into()).is_err());
        let scope = KeyScope::new(
            "shop",
            KgAccess::new(KgRole::Decider, rels(&["attempt"])).unwrap(),
        )
        .unwrap();
        assert_eq!(scope.to_string(), "decider on shop (relations attempt)");
    }
}
