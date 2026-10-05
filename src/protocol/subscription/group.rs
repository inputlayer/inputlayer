//! Subscription groups: several standing queries refreshed together.
//!
//! A group refresh pins the knowledge graph's current snapshot once and
//! evaluates its queries on it concurrently, each through the normal query
//! path ([`ReevaluatingQuery::evaluate_on`]). The refresh succeeds only if all
//! of them do, adopting no result before then, so the group's results are
//! always exact at one revision together.
//!
//! A query whose inputs did not change is not run again: when the snapshot's
//! change log shows that neither the persistent rules nor any relation the
//! query reads changed after the revision its result is exact at
//! ([`Dependencies::changed_after`]), its result is the same, exactly, at the
//! new revision. So a commit that touches one member costs one evaluation, not
//! one per member. The test reads the snapshot itself, never the
//! notifications, which may announce a commit after a snapshot that already
//! holds it. A query that reads state no relation tracks (HNSW indexes)
//! always runs.

use std::sync::Arc;

use futures_util::future::{join_all, BoxFuture};

use crate::protocol::Handler;
use crate::statement::QueryGoal;

use super::{Dependencies, ReevaluatingQuery, Refresh, StandingQuery};

/// Standing queries kept current together by re-evaluation.
pub struct GroupQuery {
    members: Vec<Member>,
}

/// One query of a [`GroupQuery`].
struct Member {
    query: ReevaluatingQuery,
    /// The revision the current result is exact at; `None` before the first.
    revision: Option<u64>,
}

impl GroupQuery {
    /// Prepare `queries` (each `?body`) on `knowledge_graph`, evaluated
    /// together.
    pub fn new(
        handler: Arc<Handler>,
        knowledge_graph: &str,
        queries: &[&str],
    ) -> Result<Self, String> {
        if queries.is_empty() {
            return Err("A subscription group needs at least one query.".to_string());
        }
        let members = queries
            .iter()
            .map(|query| {
                Ok(Member {
                    query: ReevaluatingQuery::new(Arc::clone(&handler), knowledge_graph, query)?,
                    revision: None,
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(Self { members })
    }

    /// The parsed queries, in order.
    pub fn goals(&self) -> impl Iterator<Item = &QueryGoal> {
        self.members.iter().map(|member| member.query.goal())
    }

    async fn reevaluate(&mut self) -> Result<Refresh, String> {
        let snapshot = self.members[0].query.current_snapshot()?;
        let revision = snapshot.revision;
        let grouped = self.members.len() > 1;
        let runs = self.members.iter().map(|member| {
            let dependencies = Dependencies::for_query(member.query.goal(), &snapshot.rules);
            let snapshot = Arc::clone(&snapshot);
            async move {
                let unchanged = member
                    .revision
                    .is_some_and(|at| !dependencies.changed_after(at, snapshot.changes()));
                if unchanged {
                    return (dependencies, Ok(None));
                }
                let evaluated = member
                    .query
                    .evaluate_on(snapshot)
                    .await
                    .map(Some)
                    .map_err(|e| {
                        if grouped {
                            format!("{}: {e}", member.query.query())
                        } else {
                            e
                        }
                    });
                (dependencies, evaluated)
            }
        });
        let outcomes = join_all(runs).await;
        // The refresh fails as a whole, before any query adopts its result,
        // so every result stays the base for the next delta.
        if let Some(error) = outcomes
            .iter()
            .find_map(|(_, outcome)| outcome.as_ref().err())
        {
            return Err(error.clone());
        }
        let mut dependencies = Dependencies::default();
        let mut queries = Vec::with_capacity(self.members.len());
        for (member, (member_dependencies, outcome)) in self.members.iter_mut().zip(outcomes) {
            dependencies.merge(&member_dependencies);
            member.revision = Some(revision);
            queries.push(match outcome {
                Ok(Some(evaluated)) => {
                    let refresh = member.query.adopt(evaluated);
                    refresh.queries.into_iter().next().unwrap_or_default()
                }
                _ => member.query.unchanged(),
            });
        }
        Ok(Refresh {
            queries,
            dependencies,
            revision,
        })
    }
}

impl StandingQuery for GroupQuery {
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>> {
        Box::pin(self.reevaluate())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
