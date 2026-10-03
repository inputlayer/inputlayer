//! Read access to a knowledge graph, checked for every pushed frame.
//!
//! A connection's credential is fenced by the outbound writer; its access to
//! the knowledge graph a push is about can be revoked separately, with
//! `.kg acl revoke`, and its user's role with `.user role`. Looking the
//! access list up per frame would scan it, so each connection caches its
//! answer per knowledge graph and asks again only when the handler's
//! access-list generation or the user's role moved: two atomic loads per
//! frame while nothing changes. Subscription pushes ask before their delta is built
//! (see [`crate::protocol::subscription::Subscriber::deliver`]).

use crate::auth::{Principal, Role};
use crate::protocol::Handler;

/// The last answer, valid while the access-list generation and the role are
/// unchanged.
struct Answer {
    generation: u64,
    role: Role,
    knowledge_graph: String,
    readable: bool,
}

/// One connection's cached read access.
#[derive(Default)]
pub(super) struct KgReadAccess {
    last: Option<Answer>,
}

impl KgReadAccess {
    /// Whether `principal` may currently read `knowledge_graph`.
    pub(super) fn allows(
        &mut self,
        handler: &Handler,
        principal: &Principal,
        knowledge_graph: &str,
    ) -> bool {
        // Read before the lookup: a change after it bumps the generation
        // again, so a stale answer is never kept.
        let generation = handler.kg_acl_generation();
        let Ok(role) = principal.role() else {
            return false;
        };
        if let Some(last) = &self.last {
            if last.generation == generation
                && last.role == role
                && last.knowledge_graph == knowledge_graph
            {
                return last.readable;
            }
        }
        let readable = handler
            .get_kg_role_for_user(knowledge_graph, principal.username(), &role)
            .is_some();
        self.last = Some(Answer {
            generation,
            role,
            knowledge_graph: knowledge_graph.to_string(),
            readable,
        });
        readable
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::Config;

    const KG: &str = "acl";

    fn handler_with_bob() -> (Handler, Principal, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().join("data");
        config.http.auth.bootstrap_admin_password = Some("admin-pw".to_string());
        config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
        let handler = Handler::from_config(config).unwrap();
        handler.bootstrap_auth();
        handler.get_storage().create_knowledge_graph(KG).unwrap();
        handler
            .handle_user_create("bob", "bob-pw", "editor")
            .unwrap();
        handler.handle_kg_acl_grant(KG, "bob", "viewer").unwrap();
        let key = handler.create_api_key("bob-key", "bob", None).unwrap();
        let bob = handler.authenticate_api_key(&key).unwrap();
        (handler, bob, tmp)
    }

    #[test]
    fn access_follows_grants_and_revocations() {
        let (handler, bob, _tmp) = handler_with_bob();
        let mut access = KgReadAccess::default();
        assert!(access.allows(&handler, &bob, KG));
        assert!(!access.allows(&handler, &bob, "elsewhere"));

        handler.handle_kg_acl_revoke(KG, "bob").unwrap();
        assert!(
            !access.allows(&handler, &bob, KG),
            "revocation is seen at once"
        );
        handler.handle_kg_acl_grant(KG, "bob", "viewer").unwrap();
        assert!(access.allows(&handler, &bob, KG), "and so is a new grant");
    }

    #[test]
    fn access_follows_role_changes() {
        let (handler, bob, _tmp) = handler_with_bob();
        let mut access = KgReadAccess::default();
        handler.handle_user_role("bob", "admin").unwrap();
        assert!(
            access.allows(&handler, &bob, "elsewhere"),
            "admins read all"
        );
        handler.handle_user_role("bob", "editor").unwrap();
        assert!(
            !access.allows(&handler, &bob, "elsewhere"),
            "a demotion is seen at once"
        );
        assert!(access.allows(&handler, &bob, KG), "the grant still holds");
    }
}
