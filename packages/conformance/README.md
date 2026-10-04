# SDK conformance fixtures

Frame sequences every InputLayer SDK replays through a mock server in its unit
tests, so the Python and TypeScript connection cores are held to the same
behaviour. The frames follow the `/ws` protocol (`ws-protocol/src`,
`docs/spec/asyncapi.yaml`).

## `connection/`

One JSON file per scenario:

| Field | Meaning |
|---|---|
| `name`, `description` | What the scenario proves. |
| `routes` | Subscription ids the client routes before its calls (the route takes its generation from the `.subscribe` reply). |
| `calls` | Requests the client issues in order without waiting for replies: `{"execute": program, "expect": ...}`. |
| `server` | What the mock server does once the client has authenticated, in order. |
| `expect` | What the client observed besides its calls' outcomes. |

Server steps:

- `{"recv": {...}, "as": "q"}`: read the next request (keepalive pings are answered and skipped); every field given must match. Its `id` is remembered as `q`.
- `{"send": frame}`: send a frame; a string value `"$q"` is replaced by the id remembered as `q`.
- `{"close": code}`: close the socket with that WebSocket close code.

A call's `expect` is `{"rows": [...], "columns": [...]}` for a result (`columns` optional), or `{"error": kind, "code": ..., "may_have_committed": ...}` (`code` and `may_have_committed` checked only when given). Error kinds:

| Kind | Python | TypeScript |
|---|---|---|
| `query` | `QueryError` (none of the kinds below) | `QueryError` |
| `statement_failed` | `StatementFailedError` | `StatementFailedError` |
| `deadline_exceeded` | `DeadlineExceeded` | `DeadlineExceeded` |
| `cancelled` | `Cancelled` | `Cancelled` |
| `outcome_unknown` | `OutcomeUnknownError` | `OutcomeUnknownError` |
| `protocol` | `ProtocolError` | `ProtocolError` |
| `internal` | `InternalError` | `InternalError` |
| `connection_lost` | `ConnectionLost` | `ConnectionLost` |

The top-level `expect` may hold `notifications` (the `seq`s delivered, in
order), `pushes` (`subscription`, `generation` and `seq` of each push routed to
a subscription), `stale_pushes` (pushes dropped as stale or unknown) and
`events` (connection event types emitted, in order).

The client runs with reconnecting off, so a `close` ends the scenario.
