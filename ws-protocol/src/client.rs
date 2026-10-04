//! Client → server frames.

use serde::{Deserialize, Serialize};

use crate::RequestId;

/// A request from the client. Each may carry an `id`, echoed on its replies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// Authenticate with username and password.
    Login {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        username: String,
        password: String,
    },
    /// Authenticate with an API key.
    Authenticate {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        api_key: String,
    },
    /// Run an IQL program or meta command.
    Execute {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        program: String,
        /// Milliseconds the request may take, from its arrival: queueing,
        /// admission and computation together. Capped by the engine's own
        /// query timeout.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    /// Read several queries at one knowledge graph revision; answered by a
    /// `snapshot` holding one result per query, in order. Reads persistent
    /// data only, as a subscription does: session facts and session rules
    /// are not visible. Deadline and cancellation work as for `execute`.
    Read {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        queries: Vec<NamedQuery>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    /// Subscribe to a group of queries kept current together, on the
    /// connection's knowledge graph; answered by a `snapshot` naming the
    /// subscription. Its results change only together: each later push is a
    /// `subscription_group_delta` after which every member is exact at the
    /// push's revision. Ended by `.unsubscribe <subscription>`.
    Subscribe {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        /// 1-128 characters from `[A-Za-z0-9_.:-]`, as a `.subscribe` id.
        subscription: String,
        queries: Vec<NamedQuery>,
    },
    /// Cancel the unanswered request `target`; answered by `cancel_ack`.
    Cancel {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        target: RequestId,
    },
    /// Keep-alive; answered by `pong`.
    Ping {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
    },
}

impl ClientFrame {
    /// The id the replies to this request echo.
    pub fn id(&self) -> Option<&RequestId> {
        match self {
            Self::Login { id, .. }
            | Self::Authenticate { id, .. }
            | Self::Execute { id, .. }
            | Self::Read { id, .. }
            | Self::Subscribe { id, .. }
            | Self::Cancel { id, .. }
            | Self::Ping { id } => id.as_ref(),
        }
    }
}

/// One query of a `read` or `subscribe`, and the name its result goes by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedQuery {
    /// Unique within the request.
    pub name: String,
    /// `?body`, without limit or offset.
    pub query: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn id_is_optional() {
        let frame: ClientFrame =
            serde_json::from_str(r#"{"type":"execute","program":"?a(X)"}"#).unwrap();
        assert_eq!(frame.id(), None);
        assert_eq!(
            serde_json::to_string(&frame).unwrap(),
            r#"{"type":"execute","program":"?a(X)"}"#
        );
    }

    #[test]
    fn cancel_and_timeout_round_trip() {
        let cancel: ClientFrame =
            serde_json::from_str(r#"{"type":"cancel","id":"c","target":"q"}"#).unwrap();
        assert_eq!(
            cancel,
            ClientFrame::Cancel {
                id: Some(RequestId::new("c").unwrap()),
                target: RequestId::new("q").unwrap(),
            }
        );
        let execute: ClientFrame =
            serde_json::from_str(r#"{"type":"execute","program":"?a(X)","timeout_ms":250}"#)
                .unwrap();
        assert!(matches!(
            execute,
            ClientFrame::Execute {
                timeout_ms: Some(250),
                ..
            }
        ));
        assert!(serde_json::from_str::<ClientFrame>(r#"{"type":"cancel"}"#).is_err());
    }

    #[test]
    fn read_and_subscribe_round_trip() {
        let read: ClientFrame = serde_json::from_str(
            r#"{"type":"read","id":"r","queries":[{"name":"a","query":"?a(X)"}],"timeout_ms":5}"#,
        )
        .unwrap();
        assert_eq!(
            read,
            ClientFrame::Read {
                id: Some(RequestId::new("r").unwrap()),
                queries: vec![NamedQuery {
                    name: "a".to_string(),
                    query: "?a(X)".to_string(),
                }],
                timeout_ms: Some(5),
            }
        );
        let json = r#"{"type":"subscribe","id":"s","subscription":"w","queries":[{"name":"a","query":"?a(X)"},{"name":"b","query":"?b(Y)"}]}"#;
        let subscribe: ClientFrame = serde_json::from_str(json).unwrap();
        assert_eq!(subscribe.id(), Some(&RequestId::new("s").unwrap()));
        assert_eq!(serde_json::to_string(&subscribe).unwrap(), json);
        for bad in [
            r#"{"type":"read","id":"r"}"#,
            r#"{"type":"subscribe","id":"s","queries":[]}"#,
            r#"{"type":"subscribe","subscription":"w","queries":[{"name":"a"}]}"#,
        ] {
            assert!(serde_json::from_str::<ClientFrame>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn id_round_trips() {
        let frame = ClientFrame::Ping {
            id: Some(RequestId::from(3)),
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(json, r#"{"type":"ping","id":"3"}"#);
        assert_eq!(serde_json::from_str::<ClientFrame>(&json).unwrap(), frame);
    }

    #[test]
    fn malformed_or_duplicate_id_rejects_the_frame() {
        for bad in [
            r#"{"type":"ping","id":7}"#,
            r#"{"type":"ping","id":""}"#,
            r#"{"type":"ping","id":"a","id":"b"}"#,
            r#"{"type":"execute","id":"a"}"#,
            r#"{"type":"bogus","id":"a"}"#,
        ] {
            assert!(serde_json::from_str::<ClientFrame>(bad).is_err(), "{bad}");
        }
    }
}
