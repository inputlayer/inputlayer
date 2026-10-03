//! A view's publications: its first result, and the news of each later refresh.

use std::sync::Arc;

use super::super::publication::{Outcome, Publication, SubscriberId, ViewCell};
use super::super::{Refresh, Row};
use super::{Live, View};

/// Make `refresh` the view's first publication; returns its rows, sorted.
pub(super) fn start(view: &mut View, refresh: Refresh) -> Arc<Vec<Row>> {
    view.dependencies = refresh.dependencies;
    let publication = Arc::new(Publication {
        number: 1,
        revision: refresh.revision,
        result_number: 1,
        columns: refresh.columns,
        result: refresh.result,
        outcome: Outcome::Snapshot,
    });
    view.live = Some(Live {
        cell: Arc::new(ViewCell::new(Arc::clone(&publication))),
        latest: publication,
    });
    Arc::new(refresh.inserted)
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
                return Vec::new();
            }
            Publication {
                number,
                revision: refresh.revision,
                result_number: number,
                columns: refresh.columns,
                result: refresh.result,
                outcome: Outcome::Delta {
                    base: last.result_number,
                    inserted: refresh.inserted,
                    retracted: refresh.retracted,
                },
            }
        }
        Err(e) if retry && matches!(&last.outcome, Outcome::Failed(f) if *f == e) => return vec![],
        Err(message) => Publication {
            number,
            revision: last.revision,
            result_number: last.result_number,
            columns: last.columns.clone(),
            result: Arc::clone(&last.result),
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
