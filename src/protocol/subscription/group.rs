//! Subscription groups: several standing queries refreshed together.
//!
//! A group refresh pins the knowledge graph's current snapshot once and
//! evaluates its queries on it concurrently, each through the normal query
//! path ([`ReevaluatingQuery::evaluate_on`]). The refresh succeeds only if all
//! of them do, adopting no result before then, so the group's results are
//! always exact at one revision together.
//!
//! A query whose inputs did not change is not run again: when the persistent
//! rules defining the relations it reads are the same as at its last
//! evaluation, and the snapshot shares every one of those relations' tuples
//! with that evaluation's snapshot ([`Relation::shares_tuples_with`]), its
//! result is the same, exactly, at the new revision. So a commit that touches
//! one member costs one evaluation, not one per member. The test reads the
//! snapshot itself, never the notifications, which may announce a commit
//! after a snapshot that already holds it. A query that reads state no
//! relation tracks (HNSW indexes) always runs.

use std::sync::Arc;

use futures_util::future::{join_all, BoxFuture};

use crate::ast::Rule;
use crate::protocol::Handler;
use crate::statement::QueryGoal;
use crate::storage_engine::KnowledgeGraphSnapshot;
use crate::value::Relation;

use super::{Dependencies, ReevaluatingQuery, Refresh, StandingQuery};

/// Standing queries kept current together by re-evaluation.
pub struct GroupQuery {
    members: Vec<Member>,
}

/// One query of a [`GroupQuery`].
struct Member {
    query: ReevaluatingQuery,
    /// What the current result was computed from; `None` before the first
    /// result, or when the query reads untracked state.
    inputs: Option<Inputs>,
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
                    inputs: None,
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
                    .inputs
                    .as_ref()
                    .is_some_and(|inputs| inputs.hold_in(&dependencies, &snapshot));
                if unchanged {
                    return (dependencies, Ok(None));
                }
                let inputs = Inputs::of(&dependencies, &snapshot);
                let evaluated = member
                    .query
                    .evaluate_on(snapshot)
                    .await
                    .map(|evaluated| Some((evaluated, inputs)))
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
            queries.push(match outcome {
                Ok(Some((evaluated, inputs))) => {
                    member.inputs = inputs;
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

/// What one query's result was computed from: the persistent rules that
/// define the relations it reads, and those relations' tuples. Holding the
/// tuples keeps their chunks from being reused, so a later snapshot that
/// shares them holds exactly these tuples.
struct Inputs {
    dependencies: Dependencies,
    rules: Vec<Rule>,
    /// Tuples of each relation in `dependencies`, in its order.
    relations: Vec<Option<Relation>>,
}

impl Inputs {
    /// The inputs of a query with `dependencies` in `snapshot`; `None` when
    /// it reads state that relations do not track, which only evaluating can
    /// compare.
    fn of(dependencies: &Dependencies, snapshot: &KnowledgeGraphSnapshot) -> Option<Self> {
        if dependencies.reads_untracked_state() {
            return None;
        }
        Some(Self {
            dependencies: dependencies.clone(),
            rules: rules_defining(dependencies, snapshot).cloned().collect(),
            relations: dependencies
                .relations()
                .map(|relation| snapshot.input_tuples.get(relation).cloned())
                .collect(),
        })
    }

    /// Whether a query with `dependencies` in `snapshot` has the same inputs,
    /// and so the same result.
    fn hold_in(&self, dependencies: &Dependencies, snapshot: &KnowledgeGraphSnapshot) -> bool {
        self.dependencies == *dependencies
            && rules_defining(dependencies, snapshot).eq(self.rules.iter())
            && dependencies
                .relations()
                .zip(&self.relations)
                .all(
                    |(relation, held)| match (snapshot.input_tuples.get(relation), held) {
                        (None, None) => true,
                        (Some(now), Some(held)) => now.shares_tuples_with(held),
                        _ => false,
                    },
                )
    }
}

/// The persistent rules of `snapshot` that define a relation in `dependencies`.
fn rules_defining<'a>(
    dependencies: &'a Dependencies,
    snapshot: &'a KnowledgeGraphSnapshot,
) -> impl Iterator<Item = &'a Rule> {
    snapshot
        .rules
        .iter()
        .filter(|rule| dependencies.contains(&rule.head.relation))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
