//! A view's publications: its first results, and the news of each later refresh.

use std::sync::Arc;

use super::super::publication::{
    Outcome, Publication, RowChange, SubscriberId, ViewCell, ViewResult,
};
use super::super::{Refresh, Row};
use super::{Live, View};

/// Make `refresh` the view's first publication; returns its rows, sorted, one
/// list per query.
pub(super) fn start(view: &mut View, refresh: Refresh) -> Arc<Vec<Vec<Row>>> {
    view.dependencies = refresh.dependencies;
    let mut results = Vec::with_capacity(refresh.queries.len());
    let mut rows = Vec::with_capacity(refresh.queries.len());
    for query in refresh.queries {
        results.push(ViewResult {
            columns: query.columns,
            rows: query.result,
        });
        rows.push(query.inserted);
    }
    let publication = Arc::new(Publication {
        number: 1,
        revision: refresh.revision,
        result_number: 1,
        results: results.into(),
        outcome: Outcome::Snapshot,
    });
    view.live = Some(Live {
        cell: Arc::new(ViewCell::new(Arc::clone(&publication))),
        latest: publication,
    });
    Arc::new(rows)
}

/// Mark the view's result exact at `revision` too: no news for subscribers.
pub(super) fn advance(live: &mut Live, revision: u64) {
    let last = &live.latest;
    let publication = Arc::new(Publication {
        revision,
        results: Arc::clone(&last.results),
        outcome: last.outcome.clone(),
        ..**last
    });
    live.cell.publish(Arc::clone(&publication));
    live.latest = publication;
}

/// Publish a refresh's news and ring every subscriber; returns those whose connection is gone.
pub(super) fn publish(
    view: &mut View,
    result: Result<Refresh, String>,
    retry: bool,
) -> Vec<SubscriberId> {
    let Some(live) = &mut view.live else {
        return Vec::new();
    };
    let last = &live.latest;
    let number = last.number + 1;
    let publication = match result {
        Ok(refresh) => {
            let unchanged = refresh.is_unchanged();
            view.dependencies = refresh.dependencies;
            if unchanged && !matches!(last.outcome, Outcome::Failed(_)) {
                advance(live, refresh.revision);
                return Vec::new();
            }
            let mut results = Vec::with_capacity(refresh.queries.len());
            let mut changes = Vec::with_capacity(refresh.queries.len());
            for query in refresh.queries {
                results.push(ViewResult {
                    columns: query.columns,
                    rows: query.result,
                });
                changes.push(RowChange {
                    inserted: query.inserted,
                    retracted: query.retracted,
                });
            }
            Publication {
                number,
                revision: refresh.revision,
                result_number: number,
                results: results.into(),
                outcome: Outcome::Delta {
                    base: last.result_number,
                    changes,
                },
            }
        }
        Err(e) if retry && matches!(&last.outcome, Outcome::Failed(f) if *f == e) => return vec![],
        Err(message) => Publication {
            number,
            revision: last.revision,
            result_number: last.result_number,
            results: Arc::clone(&last.results),
            outcome: Outcome::Failed(message),
        },
    };
    let publication = Arc::new(publication);
    live.cell.publish(Arc::clone(&publication));
    live.latest = publication;
    view.subscribers
        .values()
        .filter(|doorbell| !doorbell.ring())
        .map(|doorbell| doorbell.id())
        .collect()
}
