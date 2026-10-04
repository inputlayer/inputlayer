//! Client → server frames.

use serde::{Deserialize, Serialize};

use crate::{Params, RequestId};

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
        /// Values of the program's `$name` parameters, bound by the engine
        /// without passing through the parser (see [`Params`]). Every
        /// parameter the program names must be given, and every one given
        /// must be named.
        #[serde(default, skip_serializing_if = "Params::is_empty")]
        params: Params,
        /// Milliseconds the request may take, from its arrival: queueing,
        /// admission and computation together. Capped by the engine's own
        /// query timeout.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
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
            | Self::Cancel { id, .. }
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
    fn execute_carries_params_beside_the_program() {
        let frame: ClientFrame = serde_json::from_str(
            r#"{"type":"execute","program":"+eta($s, $d)","params":{"s":"S-77","d":{"int":"20261010"}}}"#,
        )
        .unwrap();
        let ClientFrame::Execute { params, .. } = &frame else {
            panic!("not an execute: {frame:?}");
        };
        assert_eq!(
            params.get("s"),
            Some(&crate::ParamValue::String("S-77".into()))
        );
        assert_eq!(params.get("d"), Some(&crate::ParamValue::Int(20_261_010)));
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(serde_json::from_str::<ClientFrame>(&json).unwrap(), frame);
        // No params: the field is omitted, and an empty object is the same.
        let bare: ClientFrame =
            serde_json::from_str(r#"{"type":"execute","program":"?a(X)","params":{}}"#).unwrap();
        assert_eq!(
            serde_json::to_string(&bare).unwrap(),
            r#"{"type":"execute","program":"?a(X)"}"#
        );
        // A bad value fails the frame, naming the parameter.
        let error = serde_json::from_str::<ClientFrame>(
            r#"{"type":"execute","program":"+a($x)","params":{"x":null}}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("parameter \"x\""), "{error}");
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
