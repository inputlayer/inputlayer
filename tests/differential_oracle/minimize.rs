//! Delta debugging: shrink a failing history while it still reproduces the
//! same divergence (same kind, adapter and query).

use crate::adapter::AdapterFactory;
use crate::model::History;
use crate::oracle::{run, Signature};

pub fn minimize(history: &History, factory: &AdapterFactory<'_>, target: &Signature) -> History {
    let reproduces = |candidate: &History| run(candidate, factory).finds(target);
    let mut best = history.clone();
    // Observe only the diverging query, when there is one.
    if let Some(query) = &target.query {
        let narrowed = History {
            queries: vec![query.clone()],
            steps: best.steps.clone(),
        };
        if reproduces(&narrowed) {
            best = narrowed;
        }
    }
    // ddmin over steps: try dropping ever smaller chunks.
    let mut chunks = 2;
    while best.steps.len() >= 2 {
        let size = best.steps.len().div_ceil(chunks);
        let mut reduced = false;
        for start in (0..best.steps.len()).step_by(size) {
            let mut steps = best.steps.clone();
            steps.drain(start..(start + size).min(steps.len()));
            let candidate = History {
                queries: best.queries.clone(),
                steps,
            };
            if reproduces(&candidate) {
                best = candidate;
                reduced = true;
                break;
            }
        }
        if reduced {
            chunks = (chunks - 1).max(2);
        } else if size == 1 {
            break;
        } else {
            chunks = (chunks * 2).min(best.steps.len());
        }
    }
    best
}
