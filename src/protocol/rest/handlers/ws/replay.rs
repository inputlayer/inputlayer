//! Replaying missed notifications to a reconnecting client.
//!
//! A client that reconnects with `last_seq` and `epoch` gets the retained
//! notifications after its cursor, then live ones with nothing skipped or
//! repeated in between (see [`NotificationLog::resume`]). When the retained
//! notifications are not the complete history since the cursor it gets one
//! `replay_gap` notice instead and must re-read the state it tracks; nothing
//! is ever replayed from a different engine run.
//!
//! Replayed frames pass the same credential fence and knowledge graph access
//! check as live ones, so access revoked during the replay stops the frames
//! that follow.
//!
//! [`NotificationLog::resume`]: crate::protocol::notification_log::NotificationLog::resume

use futures_util::Sink;
use inputlayer_ws_protocol::{NoticeCode, ServerFrame};
use tracing::debug;

use super::outbound::Outbound;
use crate::protocol::handler::Notification;
use crate::protocol::notification_log::ReplayGap;

/// Send the replay for a cursor at `last_seq`: each notification `visible`
/// accepts, or the gap notice. Returns `false` if the connection is dead.
pub(super) async fn send_replay<S: Sink<axum::extract::ws::Message> + Unpin>(
    sender: &mut Outbound<S>,
    last_seq: u64,
    replay: Result<Vec<Notification>, ReplayGap>,
    mut visible: impl FnMut(&Notification) -> bool,
) -> bool {
    let notifications = match replay {
        Ok(notifications) => notifications,
        Err(gap) => {
            debug!(last_seq, ?gap, "ws_replay_gap");
            return sender
                .send_notice(NoticeCode::ReplayGap, gap.message(last_seq))
                .await;
        }
    };
    debug!(
        last_seq,
        count = notifications.len(),
        "ws_replaying_missed_notifications"
    );
    for notification in notifications.into_iter().filter(|n| visible(n)) {
        if !sender
            .send_frame(&ServerFrame::Notification(notification))
            .await
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use axum::extract::ws::Message;

    use super::*;
    use crate::auth::{ApiKeyRecord, ApiKeyTimes, CredentialRegistry, Role, UserRecord};

    /// A sink that revokes the connection's key once it accepted `revoke_after`
    /// frames, as the next frame is prepared: between frames, like a revocation
    /// from another connection. (Not inside `start_send`: the hand-off holds the
    /// credential's end lock, which a revocation waits for.)
    struct RevokingSink {
        registry: CredentialRegistry,
        revoke_after: usize,
        sent: Vec<Message>,
    }

    impl Sink<Message> for RevokingSink {
        type Error = std::convert::Infallible;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.sent.len() == self.revoke_after {
                self.registry.revoke_key("k");
            }
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            self.sent.push(message);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    fn change(kg: &str, seq: u64) -> Notification {
        Notification::PersistentUpdate {
            knowledge_graph: kg.to_string(),
            relation: "r".to_string(),
            operation: "insert".to_string(),
            count: 1,
            timestamp_ms: 0,
            session_id: None,
            seq,
        }
    }

    fn outbound(revoke_after: usize) -> Outbound<RevokingSink> {
        let registry = CredentialRegistry::default();
        registry.load(
            vec![UserRecord {
                username: "bob".to_string(),
                password_hash: String::new(),
                role: Role::Viewer,
            }],
            vec![ApiKeyRecord {
                label: "k".to_string(),
                key_hash: "h".to_string(),
                username: "bob".to_string(),
                times: ApiKeyTimes::default(),
            }],
        );
        let principal = registry.authenticate_key("h").unwrap();
        let mut outbound = Outbound::new(RevokingSink {
            registry,
            revoke_after,
            sent: Vec::new(),
        });
        outbound.bind(principal);
        outbound
    }

    fn sent_seqs(outbound: &Outbound<RevokingSink>) -> Vec<u64> {
        outbound
            .sink()
            .sent
            .iter()
            .map(|message| {
                let Message::Text(text) = message else {
                    panic!("{message:?}");
                };
                match serde_json::from_str::<ServerFrame>(text).unwrap() {
                    ServerFrame::Notification(n) => n.seq(),
                    other => panic!("{other:?}"),
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn replays_only_visible_notifications_in_order() {
        let mut sender = outbound(usize::MAX);
        let replay = Ok(vec![change("kg", 4), change("other", 5), change("kg", 6)]);
        assert!(send_replay(&mut sender, 3, replay, |n| n.knowledge_graph() == "kg").await);
        assert_eq!(sent_seqs(&sender), [4, 6]);
    }

    #[tokio::test]
    async fn a_gap_sends_one_notice_and_no_notifications() {
        let mut sender = outbound(usize::MAX);
        let gap = Err(ReplayGap::Evicted { oldest_retained: 9 });
        assert!(send_replay(&mut sender, 3, gap, |_| true).await);
        let [Message::Text(text)] = sender.sink().sent.as_slice() else {
            panic!("{:?}", sender.sink().sent);
        };
        let ServerFrame::Notice { code, message } = serde_json::from_str(text).unwrap() else {
            panic!("{text}");
        };
        assert_eq!(code, NoticeCode::ReplayGap);
        assert!(message.contains("after seq 3"), "{message}");
    }

    #[tokio::test]
    async fn a_credential_revoked_during_replay_stops_it_at_the_next_frame() {
        let mut sender = outbound(2);
        let replay = Ok((1..=5).map(|seq| change("kg", seq)).collect());
        assert!(!send_replay(&mut sender, 0, replay, |_| true).await);
        assert_eq!(
            sent_seqs(&sender),
            [1, 2],
            "nothing passes the fence after revocation"
        );
    }
}
