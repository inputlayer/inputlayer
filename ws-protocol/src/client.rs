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
            | Self::Ping { id } => id.as_ref(),
        }
    }
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
