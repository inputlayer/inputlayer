//! The request ids of a connection's unanswered requests.
//!
//! An id names one request until its reply is released, so it must be unique
//! among the requests still awaiting a reply; it may be reused afterwards.
//! Owned by the connection loop: no lock.

use std::collections::{BTreeMap, HashSet};

use inputlayer_ws_protocol::RequestId;

use super::pipeline::Ticket;

#[derive(Default)]
pub(super) struct InFlight {
    ids: HashSet<RequestId>,
    by_ticket: BTreeMap<Ticket, RequestId>,
}

/// The id is already naming an unanswered request.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct DuplicateId;

impl InFlight {
    /// Whether `id` names an unanswered request.
    pub(super) fn contains(&self, id: &RequestId) -> bool {
        self.ids.contains(id)
    }

    /// Record `id` as naming the request admitted with `ticket`.
    pub(super) fn insert(&mut self, id: RequestId, ticket: Ticket) -> Result<(), DuplicateId> {
        if !self.ids.insert(id.clone()) {
            return Err(DuplicateId);
        }
        self.by_ticket.insert(ticket, id);
        Ok(())
    }

    /// Forget the id of the request released with `ticket`, if it had one.
    pub(super) fn release(&mut self, ticket: Ticket) -> Option<RequestId> {
        let id = self.by_ticket.remove(&ticket)?;
        self.ids.remove(&id);
        Some(id)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::protocol::rest::handlers::ws::pipeline::Access;
    use crate::protocol::rest::handlers::ws::pipeline::RequestPipeline;

    #[test]
    fn an_id_is_unique_until_its_request_is_released() {
        let mut pipeline = RequestPipeline::<(), ()>::new(4);
        let first = pipeline.admit(Access::Shared, ());
        let second = pipeline.admit(Access::Shared, ());
        let id = RequestId::new("q").unwrap();
        let mut in_flight = InFlight::default();

        in_flight.insert(id.clone(), first).unwrap();
        assert!(in_flight.contains(&id));
        assert_eq!(in_flight.insert(id.clone(), second), Err(DuplicateId));
        assert_eq!(in_flight.release(second), None, "second never held the id");
        assert!(in_flight.contains(&id));

        assert_eq!(in_flight.release(first), Some(id.clone()));
        assert!(!in_flight.contains(&id));
        in_flight.insert(id.clone(), second).unwrap();
    }
}
