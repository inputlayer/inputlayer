# Connection frame fixtures

Adversarial frame sequences for an SDK's connection core: each file is one
scenario a scripted server plays against a client, with the outcome the client
must reach. They are language-neutral so every SDK can replay them
(`packages/inputlayer-js/tests/connection.test.ts` is the JS runner).

```json
{
  "name": "out_of_order_replies",
  "about": "what the scenario proves",
  "options": { "maxInFlight": 2, "timeoutMs": 50, "timeoutGraceMs": 50 },
  "calls": ["?a(X)", "?b(X)"],
  "server": [ ...steps ],
  "expect": { ... }
}
```

- `calls` are started together, in order, once the client is authenticated.
  A call is a program (a string, sent as `execute`), `{"read": [queries]}`
  (a snapshot read of `{"name", "query"}` objects) or
  `{"subscribe": {"subscription", "queries"}}` (a subscription group). Each
  is unique, so the server knows which call a request belongs to.
- `server` steps run in order:
  - `{"await": {"executes": n}}`: wait until `n` requests (`execute`, `read`
    or `subscribe` frames) arrived;
  - `{"await": {"cancel": i}}`: wait for a `cancel` whose target is call `i`;
  - `{"send": frame}`: send a frame; the strings `"$r<i>"` and `"$c<i>"` are
    replaced by the id of call `i`'s latest request and of its cancel;
  - `{"raw": text}`: send text as is;
  - `{"assert": {"executes": n}}`: exactly `n` requests arrived so far;
  - `{"sleep": ms}`, `{"close": true}` (close the socket),
    `{"stall": true}` (stop reading: no more replies, no transport pongs).
- `expect`:
  - `calls[i]`: `{"rows": [...]}` for a program, `{"revision": n, "results":
    {"<name>": [...rows]}}` for a read or a group's snapshot, or
    `{"error": "<SDK error class>", "code"?, "mayHaveCommitted"?}`;
  - `notifications`: the `seq`s dispatched, in order; `lastSeq`: the cursor;
  - `events`: connection event types emitted, in order;
  - `stats`: counters of dropped frames (`staleReplies`, `stalePushes`,
    `malformedFrames`);
  - `sentTimeoutMs`: the `timeout_ms` every `execute` and `read` carried (a
    `subscribe` carries none).
  - `connected`: the connection is still open and answers a ping afterwards.
