# inputlayer-js-dev

TypeScript SDK for [InputLayer](https://github.com/inputlayer/inputlayer), the live rules engine for AI agents.

Take the rules out of your prompts: declare facts and rules in TypeScript, and InputLayer keeps every conclusion current as facts change. The SDK compiles your declarations into IQL, InputLayer's rule language, and sends it over WebSocket. A knowledge graph (`il.knowledgeGraph("support")`) is the facts and rule-derived conclusions of one domain.

**Today and next.** This package declares relations and rules (`relation()`, `from()`), writes and deletes facts, and queries the derived views. Phase 1 of the new SDK adds `kg.subscribe()` (the engine pushes each change to the agent), `kg.read()` and `kg.subscribeGroup()` (several results at one revision, [below](#subscriptions-and-snapshot-reads)) and `kg.claim()` (an agent's action is recorded only while the rules allow it): merged on main, not in a release yet. Until then, standing queries run over the [WebSocket API](https://inputlayer.ai/docs/guides/websocket-api/). The [main README](../../README.md) shows both forms.

## Installation

The SDK is not published to npm yet. Build and pack it from a source checkout:

```bash
git clone https://github.com/inputlayer/inputlayer.git
(cd inputlayer/packages/inputlayer-js && npm ci && npm run build && npm pack)   # writes inputlayer-js-dev-0.1.1.tgz
```

Then install the tarball in your project under the name `inputlayer`, so the imports below resolve:

```bash
npm install inputlayer@file:/path/to/inputlayer/packages/inputlayer-js/inputlayer-js-dev-0.1.1.tgz
```

Requirements: Node.js 18+ (or any runtime with WebSocket support) and a running InputLayer server with its admin API key exported as `INPUTLAYER_API_KEY` ([Get Started](../../README.md#get-started) shows both in three commands).

## Quick start

One rule: an order needs a carrier check when its shipment is late, the tool policy says `auto`, and no kill switch is set. Flip the kill switch and the conclusion is retracted; lift it and the conclusion is back.

<!-- quickstart:typescript (run by .github/workflows/quickstart.yml) -->
```typescript
import { InputLayer, relation, from, AND } from "inputlayer";

const Shipment    = relation("Shipment",    { order: "string", shipment: "string" });
const Eta         = relation("Eta",         { shipment: "string", due: "string" });
const Promised    = relation("Promised",    { order: "string", due: "string" });
const ToolPolicy  = relation("ToolPolicy",  { tool: "string", mode: "string" });
const KillSwitch  = relation("KillSwitch",  { tool: "string" });
const CheckNeeded = relation("CheckNeeded", { order: "string", shipment: "string" });

const il = new InputLayer({ url: process.env.INPUTLAYER_URL ?? "ws://localhost:8080/ws", apiKey: process.env.INPUTLAYER_API_KEY });
await il.connect();
const kg = il.knowledgeGraph("support");
await kg.define(Shipment, Eta, Promised, ToolPolicy, KillSwitch);

// the rule: late, policy says auto, no kill switch
await kg.defineRules("check_needed", ["order", "shipment"], [from(Shipment, Eta, Promised, ToolPolicy)
  .where((s, e, p, t) => AND(AND(AND(e.col("shipment").eq(s.col("shipment")), p.col("order").eq(s.col("order"))), e.col("due").gt(p.col("due"))),
          AND(AND(t.col("tool").eq("carrier_check"), t.col("mode").eq("auto")), t.col("tool").notIn(KillSwitch.col("tool")))))
  .select({ order: Shipment.col("order"), shipment: Shipment.col("shipment") })]);

await kg.insert(Shipment, { order: "ORD-4821", shipment: "S-77" });
await kg.insert(Promised, { order: "ORD-4821", due: "2026-10-08" });
await kg.insert(ToolPolicy, { tool: "carrier_check", mode: "auto" });
await kg.insert(Eta, { shipment: "S-77", due: "2026-10-10" });          // late
console.log((await kg.query({ select: [CheckNeeded] })).rows);         // [ [ 'ORD-4821', 'S-77' ] ]
await kg.insert(KillSwitch, { tool: "carrier_check" });                 // kill switch: the need is retracted
console.log((await kg.query({ select: [CheckNeeded] })).rows);         // []
await kg.delete(KillSwitch, { tool: "carrier_check" });                 // lifted: the need is back
console.log((await kg.query({ select: [CheckNeeded] })).rows);         // [ [ 'ORD-4821', 'S-77' ] ]
await il.close();
```

`AND` takes two expressions in this release, so longer conditions nest it.

## Subscriptions and snapshot reads

`kg.subscribe(target)` keeps one query's result exact on the client. It is an async iterator of `Change` events: a `snapshot` (every row inserted), then a `delta` per change (`inserted` and `retracted` rows, at `revision`). An `unverified` event (connection lost, a missed delta, a server reset, a consumer that fell `queue` events behind) says the rows may be stale; the SDK resubscribes and the `resync` that follows is the exact difference from what you hold. A `select` of some columns counts the rows behind each projected row, so a row still supported by another is never retracted.

```typescript
for await (const change of kg.subscribe(CheckNeeded)) {
  for (const row of change.retracted) cancel(row);
  for (const row of change.inserted) start(row);
  if (!change.verified) pause();
}
```

Two subscriptions refresh independently, so their revisions are unrelated: an event of one can be ahead of the other's. When several results must agree, read or subscribe to them together.

`kg.read({ name: target, ... })` runs every query on one snapshot of the knowledge graph. Every result is the exact answer at the returned `revision`, whatever commits meanwhile, so rows that a program writes together are seen together or not at all. A target may carry `limit`, `offset` (with a limit) and `orderBy`; `truncated` names the results a limit or the server's result cap cut short. The read fails as a whole (the error names the failing query), and `timeoutMs` and `signal` stop the whole read.

```typescript
const { revision, results } = await kg.read({ shipments: Shipment, etas: Eta });
```

`kg.subscribeGroup({ name: target, ... })` keeps several queries current as one subscription. Each `GroupChange` carries every member by name, with its `inserted` and `retracted` rows and `unchanged: true` when it has none. The engine evaluates all members on one snapshot per refresh, so after every verified event (`snapshot`, `delta`, `resync`) every member is the exact answer at the event's `revision`: no member is ever ahead of another. `unverified` and `resync` cover the whole group, and a delta that no member sees after projection delivers nothing.

```typescript
for await (const change of kg.subscribeGroup({ shipments: Shipment, etas: Eta })) {
  apply(change.members.shipments);
  apply(change.members.etas);
  // Both are at change.revision.
}
```

What is not promised: commits between two refreshes coalesce into one delta at the latest revision, so a row that appears and disappears in between is never seen (keep what must not be missed in facts). Reads and subscriptions see persistent data only: session facts and session rules are invisible to them, and a target that reads a session rule is refused. A target that is not one query (an OR condition, an aggregate) is refused with `SubscriptionRejectedError` before anything is sent; define a persistent rule and subscribe to (or read) it instead. A group counts once per member against the server's subscription limit.

## Documentation

The [TypeScript SDK guide](https://inputlayer.ai/docs/guides/js-sdk/) covers connecting, schemas, inserts and deletes, queries, joins, aggregations, rules (including recursive ones), vector search, sessions, notifications, users and access, and errors.

## License

Apache 2.0.
