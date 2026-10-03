//! A view's evaluation, run by whoever drives the registry.

use super::ViewId;
use crate::protocol::subscription::{Refresh, StandingQuery};

/// An evaluation to run: the caller drives it and hands back the [`Completion`].
pub struct Dispatch {
    pub view: ViewId,
    pub(super) query: Box<dyn StandingQuery>,
}

/// Outcome of a [`Dispatch`].
pub struct Completion {
    pub view: ViewId,
    pub(super) query: Box<dyn StandingQuery>,
    pub(super) result: Result<Refresh, String>,
}

impl Dispatch {
    /// Run the evaluation, turning a panic into an error so the view survives.
    pub async fn run(self) -> Completion {
        use futures_util::FutureExt;
        let Dispatch { view, mut query } = self;
        let result = std::panic::AssertUnwindSafe(query.refresh())
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("Internal error while evaluating subscription".to_string()));
        Completion {
            view,
            query,
            result,
        }
    }
}
