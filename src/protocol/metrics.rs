//! Server-wide operational counters and gauges for `/metrics/prometheus`:
//! HTTP responses by status, rejected and throttled requests, failed
//! authentications, WebSocket notices and error replies, and the connections
//! and requests open right now.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use inputlayer_ws_protocol::{ErrorCode, NoticeCode};

/// Why a request or connection was refused before it ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    /// HTTP 503: `rate_limit.max_connections` requests already in flight.
    HttpConnectionLimit,
    /// HTTP 429: the client IP is over `rate_limit.per_ip_max_rps`.
    HttpRateLimit,
    /// HTTP 401: no valid API key.
    HttpUnauthorized,
    /// HTTP 403: a valid key without the role the endpoint needs.
    HttpForbidden,
    /// HTTP 503 on `/ws`: `rate_limit.max_ws_connections` already open.
    WsConnectionLimit,
    /// HTTP 429 on `/ws`: the client IP holds `rate_limit.ws_max_preauth_per_ip`
    /// unauthenticated connections.
    WsPreauthLimit,
    /// A WebSocket message over `rate_limit.ws_max_messages_per_sec`.
    WsRateLimit,
    /// A password login refused by the failed-login throttle.
    LoginThrottled,
    /// A password login refused because the login queue was full.
    LoginBusy,
}

impl Rejection {
    const ALL: [Self; 9] = [
        Self::HttpConnectionLimit,
        Self::HttpRateLimit,
        Self::HttpUnauthorized,
        Self::HttpForbidden,
        Self::WsConnectionLimit,
        Self::WsPreauthLimit,
        Self::WsRateLimit,
        Self::LoginThrottled,
        Self::LoginBusy,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::HttpConnectionLimit => "http_connection_limit",
            Self::HttpRateLimit => "http_rate_limit",
            Self::HttpUnauthorized => "http_unauthorized",
            Self::HttpForbidden => "http_forbidden",
            Self::WsConnectionLimit => "ws_connection_limit",
            Self::WsPreauthLimit => "ws_preauth_limit",
            Self::WsRateLimit => "ws_rate_limit",
            Self::LoginThrottled => "login_throttled",
            Self::LoginBusy => "login_busy",
        }
    }
}

/// How a client tried to authenticate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Password,
    ApiKey,
}

impl AuthMethod {
    const ALL: [Self; 2] = [Self::Password, Self::ApiKey];

    fn label(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::ApiKey => "api_key",
        }
    }
}

const NOTICE_CODES: [NoticeCode; 9] = [
    NoticeCode::NotificationsMissed,
    NoticeCode::ReplayGap,
    NoticeCode::SlowConsumer,
    NoticeCode::IdleTimeout,
    NoticeCode::LifetimeExceeded,
    NoticeCode::AuthTimeout,
    NoticeCode::CredentialRevoked,
    NoticeCode::CredentialExpired,
    NoticeCode::ServerShutdown,
];

const ERROR_CODES: [ErrorCode; 13] = [
    ErrorCode::StoreReadOnly,
    ErrorCode::Validation,
    ErrorCode::NotFound,
    ErrorCode::Conflict,
    ErrorCode::Unsupported,
    ErrorCode::Internal,
    ErrorCode::InvalidRequest,
    ErrorCode::RateLimited,
    ErrorCode::DeadlineExceeded,
    ErrorCode::Cancelled,
    ErrorCode::PreconditionFailed,
    ErrorCode::OutcomeUnknown,
    ErrorCode::ResourceExhausted,
];

/// HTTP status codes counted one by one; any other is counted as 0.
const STATUS_CODES: std::ops::Range<u16> = 100..600;

/// The wire name of a `#[serde(rename_all = "snake_case")]` unit variant.
fn wire_name(code: impl serde::Serialize) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// Counters and gauges every part of the server records into. Lock-free.
#[derive(Debug)]
pub struct ServerMetrics {
    http_responses: Box<[AtomicU64]>,
    rejections: [AtomicU64; Rejection::ALL.len()],
    auth_failures: [AtomicU64; AuthMethod::ALL.len()],
    ws_notices: [AtomicU64; NOTICE_CODES.len()],
    /// Error replies by code, and those without one last.
    ws_errors: [AtomicU64; ERROR_CODES.len() + 1],
    ws_send_timeouts: AtomicU64,
    http_in_flight: AtomicI64,
    ws_connections: AtomicI64,
    ws_unauthenticated: AtomicI64,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            http_responses: (0..=STATUS_CODES.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            rejections: Default::default(),
            auth_failures: Default::default(),
            ws_notices: Default::default(),
            ws_errors: Default::default(),
            ws_send_timeouts: AtomicU64::new(0),
            http_in_flight: AtomicI64::new(0),
            ws_connections: AtomicI64::new(0),
            ws_unauthenticated: AtomicI64::new(0),
        }
    }
}

/// Decrements its gauge when dropped.
#[derive(Debug)]
pub struct GaugeGuard<'a>(&'a AtomicI64);

impl Drop for GaugeGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn hold(gauge: &AtomicI64) -> GaugeGuard<'_> {
    gauge.fetch_add(1, Ordering::Relaxed);
    GaugeGuard(gauge)
}

impl ServerMetrics {
    pub fn record_http_response(&self, status: u16) {
        let slot = if STATUS_CODES.contains(&status) {
            usize::from(status - STATUS_CODES.start) + 1
        } else {
            0
        };
        self.http_responses[slot].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_rejection(&self, rejection: Rejection) {
        let slot = Rejection::ALL.iter().position(|r| *r == rejection);
        if let Some(slot) = slot {
            self.rejections[slot].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn rejections(&self, rejection: Rejection) -> u64 {
        Rejection::ALL
            .iter()
            .position(|r| *r == rejection)
            .map_or(0, |slot| self.rejections[slot].load(Ordering::Relaxed))
    }

    pub fn record_auth_failure(&self, method: AuthMethod) {
        let slot = AuthMethod::ALL.iter().position(|m| *m == method);
        if let Some(slot) = slot {
            self.auth_failures[slot].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn auth_failures(&self, method: AuthMethod) -> u64 {
        AuthMethod::ALL
            .iter()
            .position(|m| *m == method)
            .map_or(0, |slot| self.auth_failures[slot].load(Ordering::Relaxed))
    }

    pub fn record_ws_notice(&self, code: NoticeCode) {
        if let Some(slot) = NOTICE_CODES.iter().position(|c| *c == code) {
            self.ws_notices[slot].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn ws_notices(&self, code: NoticeCode) -> u64 {
        NOTICE_CODES
            .iter()
            .position(|c| *c == code)
            .map_or(0, |slot| self.ws_notices[slot].load(Ordering::Relaxed))
    }

    /// Count an `error` reply; `None` for one without a code.
    pub fn record_ws_error(&self, code: Option<ErrorCode>) {
        let slot = code
            .and_then(|code| ERROR_CODES.iter().position(|c| *c == code))
            .unwrap_or(ERROR_CODES.len());
        self.ws_errors[slot].fetch_add(1, Ordering::Relaxed);
    }

    pub fn ws_errors(&self, code: Option<ErrorCode>) -> u64 {
        let slot = code
            .and_then(|code| ERROR_CODES.iter().position(|c| *c == code))
            .unwrap_or(ERROR_CODES.len());
        self.ws_errors[slot].load(Ordering::Relaxed)
    }

    pub fn record_ws_send_timeout(&self) {
        self.ws_send_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Count an HTTP request in flight until the guard drops.
    pub fn http_request(&self) -> GaugeGuard<'_> {
        hold(&self.http_in_flight)
    }

    /// Count an open WebSocket connection until the guard drops.
    pub fn ws_connection(&self) -> GaugeGuard<'_> {
        hold(&self.ws_connections)
    }

    /// Count a WebSocket connection not yet authenticated until the guard drops.
    pub fn ws_unauthenticated(&self) -> GaugeGuard<'_> {
        hold(&self.ws_unauthenticated)
    }

    pub fn http_in_flight(&self) -> i64 {
        self.http_in_flight.load(Ordering::Relaxed)
    }

    pub fn ws_connections(&self) -> i64 {
        self.ws_connections.load(Ordering::Relaxed)
    }

    pub fn ws_unauthenticated_connections(&self) -> i64 {
        self.ws_unauthenticated.load(Ordering::Relaxed)
    }

    /// Append these metrics in Prometheus text format.
    pub fn format_prometheus(&self, out: &mut Prometheus) {
        let responses = self
            .http_responses
            .iter()
            .enumerate()
            .filter_map(|(slot, n)| {
                let n = n.load(Ordering::Relaxed);
                let code = match slot {
                    0 => 0,
                    slot => STATUS_CODES.start + slot as u16 - 1,
                };
                (n > 0).then(|| (vec![("code", code.to_string())], n))
            });
        out.family(
            "inputlayer_http_responses_total",
            "counter",
            "HTTP responses by status code (0: outside 100-599).",
            responses,
        );
        out.family(
            "inputlayer_rejections_total",
            "counter",
            "Requests and connections refused before they ran, by reason.",
            Rejection::ALL.iter().zip(&self.rejections).map(|(r, n)| {
                (
                    vec![("reason", r.label().to_string())],
                    n.load(Ordering::Relaxed),
                )
            }),
        );
        out.family(
            "inputlayer_auth_failures_total",
            "counter",
            "Failed authentications (wrong password, unknown, revoked or expired API key), by method.",
            AuthMethod::ALL.iter().zip(&self.auth_failures).map(|(m, n)| {
                (
                    vec![("method", m.label().to_string())],
                    n.load(Ordering::Relaxed),
                )
            }),
        );
        out.family(
            "inputlayer_ws_notices_total",
            "counter",
            "WebSocket notices sent, by code. All but notifications_missed and replay_gap close the connection.",
            NOTICE_CODES.iter().zip(&self.ws_notices).map(|(c, n)| {
                (
                    vec![("code", wire_name(c))],
                    n.load(Ordering::Relaxed),
                )
            }),
        );
        let codes = ERROR_CODES
            .iter()
            .map(wire_name)
            .chain(std::iter::once("none".to_string()));
        out.family(
            "inputlayer_ws_errors_total",
            "counter",
            "WebSocket requests answered with an error frame, by error code (none: no code).",
            codes
                .zip(&self.ws_errors)
                .map(|(code, n)| (vec![("code", code)], n.load(Ordering::Relaxed))),
        );
        out.single(
            "inputlayer_ws_send_timeouts_total",
            "counter",
            "WebSocket connections closed because the client left a frame unread for http.ws_send_timeout_ms.",
            self.ws_send_timeouts.load(Ordering::Relaxed),
        );
        out.single(
            "inputlayer_http_requests_in_flight",
            "gauge",
            "HTTP requests being served (limit: rate_limit.max_connections).",
            self.http_in_flight.load(Ordering::Relaxed),
        );
        out.single(
            "inputlayer_ws_connections",
            "gauge",
            "Open WebSocket connections (limit: rate_limit.max_ws_connections).",
            self.ws_connections.load(Ordering::Relaxed),
        );
        out.single(
            "inputlayer_ws_connections_unauthenticated",
            "gauge",
            "Open WebSocket connections that have not authenticated yet.",
            self.ws_unauthenticated.load(Ordering::Relaxed),
        );
    }
}

/// A Prometheus text exposition being written.
#[derive(Debug, Default)]
pub struct Prometheus(String);

impl Prometheus {
    pub fn new() -> Self {
        Self(String::with_capacity(8192))
    }

    fn header(&mut self, name: &str, kind: &str, help: &str) {
        use std::fmt::Write;
        let _ = writeln!(self.0, "# HELP {name} {help}");
        let _ = writeln!(self.0, "# TYPE {name} {kind}");
    }

    /// A metric with no labels.
    pub fn single(&mut self, name: &str, kind: &str, help: &str, value: impl std::fmt::Display) {
        use std::fmt::Write;
        self.header(name, kind, help);
        let _ = writeln!(self.0, "{name} {value}");
    }

    /// A metric with one sample per label set. Written with no samples when
    /// `samples` is empty, so the family is always declared.
    pub fn family<V: std::fmt::Display>(
        &mut self,
        name: &str,
        kind: &str,
        help: &str,
        samples: impl IntoIterator<Item = (Vec<(&'static str, String)>, V)>,
    ) {
        use std::fmt::Write;
        self.header(name, kind, help);
        for (labels, value) in samples {
            let labels: Vec<String> = labels
                .iter()
                .map(|(key, value)| format!("{key}=\"{}\"", escape_label(value)))
                .collect();
            let _ = writeln!(self.0, "{name}{{{}}} {value}", labels.join(","));
        }
    }

    /// Text already in exposition format.
    pub fn raw(&mut self, text: &str) {
        self.0.push_str(text);
    }

    pub fn finish(self) -> String {
        self.0
    }
}

/// Escape a label value: backslash, double quote and newline.
fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            c => escaped.push(c),
        }
    }
    escaped
}

/// Resident set size of this process in bytes; `None` where it cannot be read.
pub fn resident_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let kib: u64 = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))?
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse()
            .ok()?;
        Some(kib * 1024)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn render(metrics: &ServerMetrics) -> String {
        let mut out = Prometheus::new();
        metrics.format_prometheus(&mut out);
        out.finish()
    }

    #[test]
    fn counts_render_with_their_labels() {
        let metrics = ServerMetrics::default();
        metrics.record_http_response(200);
        metrics.record_http_response(200);
        metrics.record_http_response(503);
        metrics.record_http_response(42);
        metrics.record_rejection(Rejection::HttpRateLimit);
        metrics.record_auth_failure(AuthMethod::ApiKey);
        metrics.record_ws_notice(NoticeCode::SlowConsumer);
        metrics.record_ws_error(Some(ErrorCode::DeadlineExceeded));
        metrics.record_ws_error(None);
        let body = render(&metrics);
        for line in [
            "inputlayer_http_responses_total{code=\"200\"} 2",
            "inputlayer_http_responses_total{code=\"503\"} 1",
            "inputlayer_http_responses_total{code=\"0\"} 1",
            "inputlayer_rejections_total{reason=\"http_rate_limit\"} 1",
            "inputlayer_rejections_total{reason=\"ws_connection_limit\"} 0",
            "inputlayer_auth_failures_total{method=\"api_key\"} 1",
            "inputlayer_ws_notices_total{code=\"slow_consumer\"} 1",
            "inputlayer_ws_errors_total{code=\"deadline_exceeded\"} 1",
            "inputlayer_ws_errors_total{code=\"none\"} 1",
        ] {
            assert!(body.lines().any(|l| l == line), "missing {line}:\n{body}");
        }
        assert!(!body.contains("code=\"404\""), "zero statuses are omitted");
    }

    #[test]
    fn every_code_has_a_wire_name() {
        for code in NOTICE_CODES {
            assert!(!wire_name(code).is_empty());
        }
        for code in ERROR_CODES {
            assert!(!wire_name(code).is_empty());
        }
    }

    #[test]
    fn gauges_follow_their_guards() {
        let metrics = ServerMetrics::default();
        let a = metrics.ws_connection();
        let b = metrics.ws_connection();
        assert_eq!(metrics.ws_connections(), 2);
        drop(a);
        assert_eq!(metrics.ws_connections(), 1);
        drop(b);
        assert_eq!(metrics.ws_connections(), 0);
    }

    #[test]
    fn label_values_are_escaped() {
        let mut out = Prometheus::new();
        out.family(
            "m",
            "gauge",
            "h",
            [(vec![("kg", "a\"b\\c\nd".to_string())], 1)],
        );
        assert!(out.finish().contains("m{kg=\"a\\\"b\\\\c\\nd\"} 1"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resident_memory_is_read() {
        assert!(resident_memory_bytes().unwrap() > 0);
    }
}
