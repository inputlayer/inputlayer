//! A connection's unanswered requests: their ids and their controls.
//!
//! An id names one request until its reply is released, so it must be unique
//! among the requests still awaiting a reply; it may be reused afterwards.
//! Each `execute` has a [`RequestControl`] carrying its deadline, which a
//! `cancel` naming its id stops. Owned by the connection loop: no lock.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use inputlayer_ws_protocol::{CancelOutcome, RequestId};

use super::pipeline::{Access, Ticket};
use crate::execution::{Halt, RequestControl};

#[derive(Default)]
pub(super) struct InFlight {
    by_id: HashMap<RequestId, Ticket>,
    by_ticket: BTreeMap<Ticket, Entry>,
}

struct Entry {
    id: Option<RequestId>,
    access: Access,
    /// `None` for requests that cannot be cancelled.
    control: Option<Arc<RequestControl>>,
}

impl InFlight {
    /// Whether `id` names an unanswered request.
    pub(super) fn contains(&self, id: &RequestId) -> bool {
        self.by_id.contains_key(id)
    }

    /// Record the request admitted with `ticket`. The caller checked that its
    /// id, if any, is not in flight.
    pub(super) fn insert(
        &mut self,
        ticket: Ticket,
        id: Option<RequestId>,
        access: Access,
        control: Option<Arc<RequestControl>>,
    ) {
        if let Some(id) = &id {
            self.by_id.insert(id.clone(), ticket);
        }
        self.by_ticket.insert(
            ticket,
            Entry {
                id,
                access,
                control,
            },
        );
    }

    /// The control of the request admitted with `ticket`, if it has one.
    pub(super) fn control(&self, ticket: Ticket) -> Option<&Arc<RequestControl>> {
        self.by_ticket.get(&ticket)?.control.as_ref()
    }

    /// Forget the request released with `ticket`; returns its id.
    pub(super) fn release(&mut self, ticket: Ticket) -> Option<RequestId> {
        let id = self.by_ticket.remove(&ticket)?.id?;
        self.by_id.remove(&id);
        Some(id)
    }

    /// Cancel the unanswered request named `target`.
    pub(super) fn cancel(&self, target: &RequestId) -> CancelOutcome {
        let control = self
            .by_id
            .get(target)
            .and_then(|ticket| self.by_ticket.get(ticket))
            .and_then(|entry| entry.control.as_ref());
        match control.map(|control| control.cancel()) {
            None => CancelOutcome::NotFound,
            Some(Halt::Stopped | Halt::AlreadyStopped(_)) => CancelOutcome::Cancelled,
            Some(Halt::TooLate) => CancelOutcome::TooLate,
        }
    }

    /// On disconnect: stop every read, whose reply nobody will receive, so
    /// its computation exits and frees its compute permit. Writes are left to
    /// finish (see the `pipeline` module).
    pub(super) fn cancel_reads(&self) {
        for entry in self.by_ticket.values() {
            if let (Access::Shared, Some(control)) = (entry.access, &entry.control) {
                control.cancel();
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::protocol::rest::handlers::ws::pipeline::RequestPipeline;

    fn tickets(n: usize) -> Vec<Ticket> {
        let mut pipeline = RequestPipeline::<(), ()>::new(n);
        (0..n).map(|_| pipeline.admit(Access::Shared, ())).collect()
    }

    #[test]
    fn an_id_is_in_flight_until_its_request_is_released() {
        let t = tickets(2);
        let id = RequestId::new("q").unwrap();
        let mut in_flight = InFlight::default();
        in_flight.insert(t[0], Some(id.clone()), Access::Shared, None);
        in_flight.insert(t[1], None, Access::Shared, None);
        assert!(in_flight.contains(&id));
        assert_eq!(in_flight.release(t[1]), None);
        assert!(in_flight.contains(&id));
        assert_eq!(in_flight.release(t[0]), Some(id.clone()));
        assert!(!in_flight.contains(&id));
    }

    #[test]
    fn cancel_reaches_the_named_request_only() {
        let t = tickets(3);
        let (a, b) = (RequestId::new("a").unwrap(), RequestId::new("b").unwrap());
        let (ca, cb) = (RequestControl::new(None), RequestControl::new(None));
        let mut in_flight = InFlight::default();
        in_flight.insert(t[0], Some(a.clone()), Access::Shared, Some(Arc::clone(&ca)));
        in_flight.insert(
            t[1],
            Some(b.clone()),
            Access::Exclusive,
            Some(Arc::clone(&cb)),
        );
        in_flight.insert(
            t[2],
            Some(RequestId::new("s").unwrap()),
            Access::Exclusive,
            None,
        );

        assert_eq!(in_flight.cancel(&a), CancelOutcome::Cancelled);
        assert!(ca.is_stopped() && !cb.is_stopped());
        assert_eq!(in_flight.cancel(&a), CancelOutcome::Cancelled, "idempotent");

        cb.begin_commit().unwrap();
        assert_eq!(in_flight.cancel(&b), CancelOutcome::TooLate);
        assert_eq!(
            in_flight.cancel(&RequestId::new("s").unwrap()),
            CancelOutcome::NotFound,
            "not cancellable"
        );
        assert_eq!(
            in_flight.cancel(&RequestId::new("zz").unwrap()),
            CancelOutcome::NotFound
        );
        in_flight.release(t[0]);
        assert_eq!(in_flight.cancel(&a), CancelOutcome::NotFound, "answered");
    }

    #[test]
    fn disconnect_cancels_reads_and_spares_writes() {
        let t = tickets(2);
        let (read, write) = (RequestControl::new(None), RequestControl::new(None));
        let mut in_flight = InFlight::default();
        in_flight.insert(t[0], None, Access::Shared, Some(Arc::clone(&read)));
        in_flight.insert(t[1], None, Access::Exclusive, Some(Arc::clone(&write)));
        in_flight.cancel_reads();
        assert!(read.is_stopped());
        assert!(!write.is_stopped());
    }
}
