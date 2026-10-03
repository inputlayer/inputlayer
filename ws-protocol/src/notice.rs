//! Connection events the server announces unprompted.

use serde::{Deserialize, Serialize};

/// What a `notice` frame announces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoticeCode {
    /// Notifications were dropped because the client read too slowly. Standing
    /// queries re-evaluate; the connection stays open.
    NotificationsMissed,
    /// The reconnect cursor (`last_seq` with `epoch`) cannot be honoured: it
    /// belongs to another engine run, or notifications after it are no longer
    /// retained. Nothing was replayed; re-read the state the client tracks.
    /// The connection stays open and live notifications follow.
    ReplayGap,
    /// Too many notifications were dropped; the server closes the connection.
    SlowConsumer,
    /// No request arrived within the idle timeout; the server closes the connection.
    IdleTimeout,
    /// The connection reached its maximum lifetime; the server closes it.
    LifetimeExceeded,
    /// No authentication arrived in time; the server closes the connection.
    AuthTimeout,
    /// The connection's credential was revoked; the server closes the connection.
    CredentialRevoked,
    /// The connection's API key expired; the server closes the connection.
    CredentialExpired,
    /// The server is shutting down and closes the connection.
    ServerShutdown,
}

impl NoticeCode {
    /// Whether the server closes the connection right after this notice.
    pub fn closes_connection(self) -> bool {
        !matches!(self, Self::NotificationsMissed | Self::ReplayGap)
    }
}
