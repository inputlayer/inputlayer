//! Read access to a knowledge graph, checked for every pushed frame.
//!
//! A connection's credential is fenced by the outbound writer; its access to
//! the knowledge graph a push is about can be revoked separately, with
//! `.kg acl revoke`. Looking the access list up per frame would scan it, so
//! each connection caches its answer per knowledge graph and asks again only
//! when the handler's access-list generation moved: one atomic load per frame
//! while nothing changes.

use inputlayer_ws_protocol::SubscriptionPush;

use crate::auth::Principal;
use crate::protocol::Handler;

/// The last answer, valid while the access-list generation is unchanged.
struct Answer {
    generation: u64,
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
        if let Some(last) = &self.last {
            if last.generation == generation && last.knowledge_graph == knowledge_graph {
                return last.readable;
            }
        }
        let readable = principal.identity().is_ok_and(|identity| {
            handler
                .get_kg_role_for_user(knowledge_graph, &identity.username, &identity.role)
                .is_some()
        });
        self.last = Some(Answer {
            generation,
            knowledge_graph: knowledge_graph.to_string(),
            readable,
        });
        readable
    }

    /// `push`, or a `subscription_error` in place of a delta whose knowledge
    /// graph `principal` may no longer read: its rows must not leave. The
    /// withheld delta's `seq` is not reused, so a client that later receives
    /// a delta sees the gap and resubscribes.
    pub(super) fn fence(
        &mut self,
        handler: &Handler,
        principal: &Principal,
        push: SubscriptionPush,
    ) -> SubscriptionPush {
        match push {
            SubscriptionPush::SubscriptionDelta {
                subscription,
                generation,
                knowledge_graph,
                seq,
                ..
            } if !self.allows(handler, principal, &knowledge_graph) => {
                SubscriptionPush::SubscriptionError {
                    subscription,
                    generation,
                    message: format!(
                        "Access denied to knowledge graph '{knowledge_graph}'; delta {seq} was \
                         withheld. Resubscribe once access is restored."
                    ),
                }
            }
            push => push,
        }
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

    fn delta() -> SubscriptionPush {
        SubscriptionPush::SubscriptionDelta {
            subscription: "s".to_string(),
            generation: 1,
            knowledge_graph: KG.to_string(),
            seq: 3,
            revision: 9,
            columns: vec!["x".to_string()],
            inserted: vec![vec![serde_json::json!(1)]],
            retracted: Vec::new(),
        }
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
    fn a_delta_for_an_unreadable_graph_is_withheld() {
        let (handler, bob, _tmp) = handler_with_bob();
        let mut access = KgReadAccess::default();
        assert_eq!(access.fence(&handler, &bob, delta()), delta());

        handler.handle_kg_acl_revoke(KG, "bob").unwrap();
        let SubscriptionPush::SubscriptionError {
            subscription,
            generation,
            message,
        } = access.fence(&handler, &bob, delta())
        else {
            panic!("the delta's rows must not leave");
        };
        assert_eq!((subscription.as_str(), generation), ("s", 1));
        assert!(message.contains("delta 3 was withheld"), "{message}");
    }
}
