//! The worker that owns every shared view of a server.
//!
//! One task owns the [`ViewRegistry`]: connections attach and detach
//! subscribers through a command channel, committed changes arrive on the
//! worker's own notification receiver, and evaluations run as tasks the worker
//! awaits. No lock guards the views: only the worker touches them. A
//! connection reads results from the view's cell when its doorbell rings (see
//! [`super::publication`]), so the worker never waits for a connection.
//!
//! Commands are handled before notifications, so a subscriber detached before
//! a commit is announced is not evaluated for it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{AbortHandle, JoinError, JoinSet};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::protocol::handler::Notification;

use super::publication::{Doorbell, SubscriberId};
use super::views::{Attach, Attachment, Completion, Dispatch, ViewId, ViewKey, ViewRegistry};
use super::{change_of, changes_rules, StandingQuery, SubscriptionMetrics};

type Reply = oneshot::Sender<Result<Attachment, String>>;

enum Command {
    Attach {
        key: ViewKey,
        doorbell: Arc<Doorbell>,
        query: Box<dyn StandingQuery>,
        reply: Reply,
    },
    Detach(SubscriberId),
}

/// Handle to the worker; cheap to clone. The worker stops once every handle
/// is dropped or the notification stream closes.
#[derive(Clone)]
pub struct SubscriptionHub {
    commands: mpsc::UnboundedSender<Command>,
    next_subscriber: Arc<AtomicU64>,
}

impl SubscriptionHub {
    /// Start the worker on the current runtime. `notifications` must be
    /// subscribed before the first view exists; `window` is the coalescing
    /// window (zero: evaluate at once).
    pub fn spawn(
        notifications: broadcast::Receiver<Notification>,
        metrics: Arc<SubscriptionMetrics>,
        window: Duration,
    ) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let worker = Worker {
            registry: ViewRegistry::new(window),
            commands: receiver,
            notifications,
            in_flight: JoinSet::new(),
            running: HashMap::new(),
            replies: HashMap::new(),
            metrics,
        };
        tokio::spawn(worker.run());
        Self {
            commands,
            next_subscriber: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A fresh subscriber id.
    pub fn next_subscriber_id(&self) -> SubscriberId {
        self.next_subscriber.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Attach a subscriber to the view of `key`; `query` evaluates it if the
    /// view does not exist yet. Resolves to the subscriber's snapshot.
    pub async fn attach(
        &self,
        key: ViewKey,
        doorbell: Arc<Doorbell>,
        query: Box<dyn StandingQuery>,
    ) -> Result<Attachment, String> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Attach {
                key,
                doorbell,
                query,
                reply,
            })
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// Detach a subscriber; its view goes when it was the last one.
    pub fn detach(&self, subscriber: SubscriberId) {
        // A stopped worker holds no views.
        let _ = self.commands.send(Command::Detach(subscriber));
    }
}

fn stopped() -> String {
    "Subscriptions are unavailable: the server is shutting down.".to_string()
}

struct Worker {
    registry: ViewRegistry,
    commands: mpsc::UnboundedReceiver<Command>,
    notifications: broadcast::Receiver<Notification>,
    in_flight: JoinSet<Completion>,
    /// Running evaluation of each view, to stop it when the view goes.
    running: HashMap<ViewId, AbortHandle>,
    /// Subscribers waiting for their view's next result.
    replies: HashMap<SubscriberId, Reply>,
    metrics: Arc<SubscriptionMetrics>,
}

impl Worker {
    async fn run(mut self) {
        loop {
            let due = self.registry.next_due();
            tokio::select! {
                biased;
                command = self.commands.recv() => match command {
                    Some(command) => self.on_command(command),
                    None => break,
                },
                Some(joined) = self.in_flight.join_next(), if !self.in_flight.is_empty() => {
                    self.on_joined(joined);
                }
                notification = self.notifications.recv() => match notification {
                    Ok(notification) => {
                        let (knowledge_graph, change) = change_of(&notification);
                        let now = Instant::now().into_std();
                        let dispatches = if changes_rules(&notification) {
                            self.registry.on_rule_change(knowledge_graph, &change, now)
                        } else {
                            self.registry.on_change(knowledge_graph, &change, now)
                        };
                        self.start_all(dispatches);
                    }
                    Err(RecvError::Lagged(missed)) => {
                        debug!(missed, "subscription_hub_lagged");
                        let now = Instant::now().into_std();
                        let dispatches = self.registry.on_unknown_changes(now);
                        self.start_all(dispatches);
                    }
                    Err(RecvError::Closed) => break,
                },
                () = sleep_until(due), if due.is_some() => {
                    let dispatches = self.registry.take_due(Instant::now().into_std());
                    self.start_all(dispatches);
                }
            }
            self.metrics.set_views(self.registry.len() as u64);
        }
        self.in_flight.abort_all();
    }

    fn on_command(&mut self, command: Command) {
        match command {
            Command::Attach {
                key,
                doorbell,
                query,
                reply,
            } => {
                let subscriber = doorbell.id();
                match self.registry.attach(key, doorbell, || query) {
                    Attach::Attached(attachment) => self.answer(subscriber, reply, Ok(attachment)),
                    Attach::Waiting(dispatch) => {
                        self.replies.insert(subscriber, reply);
                        self.start_all(dispatch);
                    }
                }
            }
            Command::Detach(subscriber) => self.detach(subscriber),
        }
    }

    fn detach(&mut self, subscriber: SubscriberId) {
        self.replies.remove(&subscriber);
        if let Some(view) = self.registry.detach(subscriber) {
            if let Some(evaluation) = self.running.remove(&view) {
                evaluation.abort();
            }
        }
    }

    /// Hand `subscriber` its attachment; one nobody awaits any more is detached.
    fn answer(
        &mut self,
        subscriber: SubscriberId,
        reply: Reply,
        answer: Result<Attachment, String>,
    ) {
        if reply.send(answer).is_err() {
            self.detach(subscriber);
        }
    }

    fn on_joined(&mut self, joined: Result<Completion, JoinError>) {
        let completion = match joined {
            Ok(completion) => completion,
            // Aborted because its view went; a panic is caught by `Dispatch::run`.
            Err(e) => {
                if !e.is_cancelled() {
                    warn!(error = %e, "subscription_evaluation_failed");
                }
                return;
            }
        };
        self.running.remove(&completion.view);
        let completed = self
            .registry
            .on_complete(completion, Instant::now().into_std());
        for (subscriber, answer) in completed.replies {
            if let Some(reply) = self.replies.remove(&subscriber) {
                self.answer(subscriber, reply, answer);
            }
        }
        self.start_all(completed.follow_up);
    }

    fn start_all(&mut self, dispatches: impl IntoIterator<Item = Dispatch>) {
        for dispatch in dispatches {
            // Counted when started, so callers see it before any resulting push.
            self.metrics.record_evaluation();
            let view = dispatch.view;
            let evaluation = self.in_flight.spawn(dispatch.run());
            self.running.insert(view, evaluation);
        }
    }
}

async fn sleep_until(due: Option<std::time::Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(Instant::from_std(due)).await,
        None => std::future::pending().await,
    }
}
