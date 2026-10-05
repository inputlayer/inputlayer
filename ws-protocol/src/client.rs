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
        /// Commit the program's writes only if the knowledge graph state in
        /// scope is as it was at this revision: no relation in scope and no
        /// persistent rule changed after it. Otherwise nothing is applied and
        /// the program fails with [`ErrorCode::PreconditionFailed`]. Only a
        /// program that writes persistent state may set it.
        ///
        /// [`ErrorCode::PreconditionFailed`]: crate::ErrorCode::PreconditionFailed
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expect_revision: Option<u64>,
        /// The scope of `expect_revision`: these relations and every relation
        /// they are derived from through persistent rules. Absent: the whole
        /// knowledge graph. Requires `expect_revision`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expect_relations: Option<Vec<String>>,
        /// The stream epoch (`authenticated.stream_epoch`) `expect_revision`
        /// belongs to: revisions restart with the engine, so a revision from
        /// another engine run fails the precondition. Requires
        /// `expect_revision`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expect_epoch: Option<String>,
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
    fn expect_revision_round_trips_and_is_omitted_when_unset() {
        let json = r#"{"type":"execute","program":"+a(1)","expect_revision":17,"expect_relations":["a","b"],"expect_epoch":"00ff"}"#;
        let frame: ClientFrame = serde_json::from_str(json).unwrap();
        assert_eq!(
            frame,
            ClientFrame::Execute {
                id: None,
                program: "+a(1)".to_string(),
                params: crate::Params::new(),
                timeout_ms: None,
                expect_revision: Some(17),
                expect_relations: Some(vec!["a".to_string(), "b".to_string()]),
                expect_epoch: Some("00ff".to_string()),
            }
        );
        assert_eq!(serde_json::to_string(&frame).unwrap(), json);
        assert!(serde_json::from_str::<ClientFrame>(
            r#"{"type":"execute","program":"+a(1)","expect_revision":-1}"#
        )
        .is_err());
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
