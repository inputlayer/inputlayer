//! Client-chosen request identifiers.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

/// Longest accepted [`RequestId`], in bytes.
pub const MAX_REQUEST_ID_LEN: usize = 64;

/// Identifies one request on a connection; echoed on every frame answering it.
///
/// A non-empty JSON string of at most [`MAX_REQUEST_ID_LEN`] bytes. The client
/// chooses it and keeps it unique among its requests still awaiting a reply;
/// the engine only copies it back.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct RequestId(String);

/// Why a string is not a [`RequestId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidRequestId {
    Empty,
    TooLong(usize),
}

impl fmt::Display for InvalidRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "request id is empty"),
            Self::TooLong(len) => write!(
                f,
                "request id is {len} bytes, longer than {MAX_REQUEST_ID_LEN}"
            ),
        }
    }
}

impl std::error::Error for InvalidRequestId {}

impl RequestId {
    pub fn new(id: impl Into<String>) -> Result<Self, InvalidRequestId> {
        let id = id.into();
        match id.len() {
            0 => Err(InvalidRequestId::Empty),
            len if len > MAX_REQUEST_ID_LEN => Err(InvalidRequestId::TooLong(len)),
            _ => Ok(Self(id)),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Counter-based ids, the common client choice.
impl From<u64> for RequestId {
    fn from(n: u64) -> Self {
        Self(n.to_string())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RequestId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let id = String::deserialize(deserializer)?;
        Self::new(id).map_err(serde::de::Error::custom)
    }
}

/// The `id` of a frame that failed to parse as a whole, so the error answering
/// it can still be correlated. `None` when the text is not a JSON object or its
/// `id` is missing, malformed or given twice.
pub fn probe_request_id(text: &str) -> Option<RequestId> {
    #[derive(Deserialize)]
    struct Probe {
        id: Option<RequestId>,
    }
    // A struct also deserializes from a JSON array; only objects carry an `id`.
    if !text.trim_start().starts_with('{') {
        return None;
    }
    serde_json::from_str::<Probe>(text).ok()?.id
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn accepts_bounded_non_empty_strings() {
        let id: RequestId = serde_json::from_str(r#""req-1""#).unwrap();
        assert_eq!(id.as_str(), "req-1");
        assert_eq!(serde_json::to_string(&id).unwrap(), r#""req-1""#);
        let longest = "x".repeat(MAX_REQUEST_ID_LEN);
        assert!(RequestId::new(longest).is_ok());
    }

    #[test]
    fn rejects_empty_long_and_non_string_ids() {
        assert_eq!(RequestId::new(""), Err(InvalidRequestId::Empty));
        let long = "x".repeat(MAX_REQUEST_ID_LEN + 1);
        assert_eq!(
            RequestId::new(long),
            Err(InvalidRequestId::TooLong(MAX_REQUEST_ID_LEN + 1))
        );
        for bad in ["7", "null", "{}", "[]", r#""""#] {
            assert!(serde_json::from_str::<RequestId>(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn counter_ids_are_decimal() {
        assert_eq!(RequestId::from(42).as_str(), "42");
    }

    #[test]
    fn probe_reads_the_id_of_an_otherwise_bad_frame() {
        let probe = |text| probe_request_id(text).map(|id| id.0);
        assert_eq!(probe(r#"{"type":"bogus","id":"a"}"#), Some("a".into()));
        assert_eq!(probe(r#"{"id":"a","program":7}"#), Some("a".into()));
        assert_eq!(probe(r#"{"type":"ping"}"#), None);
        assert_eq!(probe(r#"{"id":7}"#), None);
        assert_eq!(probe(r#"{"id":"a","id":"b"}"#), None);
        assert_eq!(probe("not json"), None);
        assert_eq!(probe(r#"["id"]"#), None);
    }
}
