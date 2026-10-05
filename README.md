[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Elastic%202.0-blue.svg)](./LICENSE)

*The live rules engine for AI agents*

# Take the rules out of your prompts.

**Your agents act on what's true now.** Declare your facts and rules once. InputLayer derives every conclusion from the current facts when it is read, tells your agents what changed, and records an agent's action only while the rules allow it, so the tool runs only then.

A rules engine, made live: a conclusion disappears from the next evaluation once its facts stop supporting it, the exact change is pushed to every subscribed agent, and any row can be explained with a proof, on request. The model proposes; the rules decide.

**Replaces:** the trigger service and the crons that re-check data for changes (a check that waits on time keeps a one-line clock writer); the precomputed eligibility flags you cache and invalidate; the business rules you wrote into system prompts and tool handlers (and the eligibility logic that ended up in OPA policies); the "already handled" rows and `claimed_by` columns that keep two agents from doing the same work (TTL locks too, once `WorkQueue` leases ship in phase 3).<br>
**Keeps:** your models and gateway, LangGraph, MCP tool servers, Zep, pgvector, Redis for plain lookups, your systems of record, Debezium/Kafka and webhooks (they feed InputLayer), OPA for who may call, NeMo for content safety, Temporal for durable effects (its workflow id becomes the claim key), LangSmith/OTel.

**Where it sits.** Facts arrive from your CDC feed and webhooks through a small adapter you run ([recipe shipped](docs/content/docs/guides/ingestion.mdx) for Debezium Server and signed webhooks), with per-key revisions, so a late or replayed event never overwrites a newer one. InputLayer is not in your tool's call path: your agent claims the action in InputLayer, the claim is recorded only if the rules hold at that instant, and your handler or Temporal runs the tool only if the claim won, with the claim as its idempotency key. If a supporting fact changes afterwards, the need is retracted and the running work is cancelled; an effect that already landed is yours to compensate, as it is today. How stale is too stale is a rule too: guard the claim on a source health lease that your adapter renews, and a tool is refused once its source has gone quiet. It is one node today: facts are durable in its write-ahead log, a restart resumes from it and the adapter's revisions make a replayed feed safe, and while it is unreachable no claim can win, so gated tools wait rather than run unchecked.

> **The upcoming SDK.** `subscribe()` and `claim()` are phase 1 of the new Python and TypeScript SDK: merged on main in both SDKs, not in a release yet. The code below is the file that phase's CI will execute. [What runs today](#what-runs-today) is right after it.

```python
async def main() -> None:
    url, key = os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"), os.environ["INPUTLAYER_API_KEY"]
    async with InputLayer(url, api_key=key) as il:
        kg = il.knowledge_graph("support"); running = {}
        await kg.define(Shipment, Eta, Promised, ToolPolicy, KillSwitch, Attempt); await kg.define_rules(CheckNeeded)
        async for change in kg.subscribe(CheckNeeded):     # the engine wakes the agent: replaces the trigger service and the re-check cron
            for row in change.retracted:                   # first: a need vanished (back on time, killed, re-shipped): stop it and free it
                if entry := running.pop(row.order, None):
                    task, attempt = entry; task.cancel(); await kg.retract(attempt)
            for row in change.inserted:                    # then: a need appeared: claim it once, then act
                c = await kg.claim(Attempt(order=row.order, tool="carrier_check", attempt=uuid.uuid4().hex[:8]),
                                   when=[CheckNeeded.any(order=row.order)], unless=Attempt.any(order=row.order, tool="carrier_check"))
                if c.won: running[row.order] = (asyncio.create_task(carrier_check(row.shipment)), c.holder)   # the claim: replaces the "already handled" row
```

```python
import asyncio, os, uuid
from inputlayer import InputLayer, Relation, Derived, From

class Shipment(Relation):   order: str; shipment: str
class Eta(Relation):        shipment: str; due: str
class Promised(Relation):   order: str; due: str
class ToolPolicy(Relation): tool: str; mode: str          # policy as facts, deployed with the rules, flipped by operators:
class KillSwitch(Relation): tool: str                     #   replaces "only auto-check when..." prose in the prompt
class Attempt(Relation):    order: str; tool: str; attempt: str

class CheckNeeded(Derived):                                # the rule: replaces the precomputed needs_check flag and the cron that rebuilt it
    order: str; shipment: str
    rules = [From(Shipment, Eta, Promised, ToolPolicy)
             .where(lambda s, e, p, t: (e.shipment == s.shipment) & (p.order == s.order) & (e.due > p.due)
                                     & (t.tool == "carrier_check") & (t.mode == "auto") & ~KillSwitch.any(tool=t.tool))
             .select(order=Shipment.order, shipment=Shipment.shipment)]

async def carrier_check(shipment: str) -> None:            # the tool; a real one calls the carrier API with an idempotency key
    await asyncio.sleep(3); print(f"checked {shipment}")

asyncio.run(main())
```

*No model in this loop, on purpose: it shows the gate. Where a model proposes an action and the same gate decides, see the [agent-loop guide](#the-agent-loop-with-a-model).*

<sub>Self-hosted, single node. Measured today for tens of concurrent agent sessions per knowledge graph; a change that lifts this is in progress (draft): https://github.com/inputlayer/inputlayer/pull/232. Python and TypeScript SDKs; subscriptions and claims ship with the next release, standing queries run over WebSocket today. A knowledge graph here is the facts and rule-derived conclusions of one domain, not a memory store. Source-available under the Elastic License 2.0.</sub>

---

## What runs today

The same rule and the same world on today's Python SDK, installed [from source](#sdks). The rule, the writes and the derivation run now; what the SDK does not have yet is the push (`subscribe()`) and the claim, so the agent's view is read with `query()` here and watched over the WebSocket API below.

```python
import asyncio, os
from inputlayer import InputLayer, Relation, Derived, From

class Shipment(Relation):   order: str; shipment: str
class Eta(Relation):        shipment: str; due: str
class Promised(Relation):   order: str; due: str
class ToolPolicy(Relation): tool: str; mode: str
class KillSwitch(Relation): tool: str

class CheckNeeded(Derived):            # the rule: late, policy says auto, no kill switch
    order: str; shipment: str
    rules = [From(Shipment, Eta, Promised, ToolPolicy)
             .where(lambda s, e, p, t: (e.shipment == s.shipment) & (p.order == s.order) & (e.due > p.due)
                                     & (t.tool == "carrier_check") & (t.mode == "auto") & ~t.tool.in_(KillSwitch.tool))
             .select(order=Shipment.order, shipment=Shipment.shipment)]

async def main() -> None:
    url = os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws")
    async with InputLayer(url, username="admin", password=os.environ["INPUTLAYER_ADMIN_PASSWORD"]) as il:
        kg = il.knowledge_graph("support")
        await kg.define(Shipment, Eta, Promised, ToolPolicy, KillSwitch)
        await kg.define_rules(CheckNeeded)
        await kg.insert([Shipment(order="ORD-4821", shipment="S-77")])
        await kg.insert([Promised(order="ORD-4821", due="2026-10-08")])
        await kg.insert([ToolPolicy(tool="carrier_check", mode="auto")])
        await kg.insert([Eta(shipment="S-77", due="2026-10-10")])      # late
        print("late:", [(r.order, r.shipment) for r in await kg.query(CheckNeeded)])
        await kg.insert([KillSwitch(tool="carrier_check")])           # kill switch: the need is retracted
        print("killed:", list(await kg.query(CheckNeeded)))
        await kg.delete(KillSwitch(tool="carrier_check"))
        print("restored:", [(r.order, r.shipment) for r in await kg.query(CheckNeeded)])

asyncio.run(main())
```

```
late: [('ORD-4821', 'S-77')]
killed: []
restored: [('ORD-4821', 'S-77')]
```

An agent subscribed over the [WebSocket API](https://inputlayer.ai/docs/guides/websocket-api/) gets the snapshot and then each change as a `subscription_delta` frame, including the row that stopped being true:

```iql
.subscribe needs ?check_needed(Order, Shipment)
// snapshot: [["ORD-4821", "S-77"]]
+kill_switch("carrier_check")
// subscription_delta  inserted: []                       retracted: [["ORD-4821", "S-77"]]
-kill_switch("carrier_check")
// subscription_delta  inserted: [["ORD-4821", "S-77"]]   retracted: []
```

And `.why` returns the proof for the row the agent acted on (the engine's proof tree, abridged):

```iql
.why ?check_needed("ORD-4821", S)
// [rule] check_needed(Order, Shipment) <- shipment(Order, Shipment), eta(Shipment, Due), promised(Order, Due_1),
//          tool_policy(Tool, Mode), !kill_switch(Tool), Due > Due_1, Tool = "carrier_check", Mode = "auto"
//   [base] shipment("ORD-4821", "S-77")
//   [base] eta("S-77", "2026-10-10")
//   [base] promised("ORD-4821", "2026-10-08")
//   [base] tool_policy("carrier_check", "auto")
//   [negation] no kill_switch("carrier_check")
```

---

## What it does

1. **Told what changed, including what stopped being true.** When a fact changes, the engine re-evaluates each affected standing query, diffs the result against the previous one, and pushes the rows that left and entered to every subscribed agent as exact deltas, with a proof on request. This replaces the trigger service, the re-check crons and the flags you invalidate by hand.
2. **No duplicate starts, no action the rules do not support when it commits.** One agent's claim per need, checked at commit against the live policy view; twenty connections racing one claim gave one winner and no errors. The effect runs outside the engine with the claim key as its idempotency key. In the reference architecture a model's output never authorizes an action by itself. This replaces the policy in your prompts and handlers and the "already handled" rows.
3. **Cancellation derived from state, not left to the model.** The need leaves the view when the facts or the policy stop supporting it, and the same loop that started the work cancels it.
4. **Scope, then sharing.** Measured today for tens of concurrent sessions per tenant knowledge graph with per-session subscriptions; many agents asking the same question share one evaluation (64-subscriber fan-out p99 13.8 ms); per-session questions fan out from one subscription per process until the engine-side change lands.

Rules, transactions, standing queries and proofs live in one engine, with one transaction boundary; the comparable stack takes several systems.

## How it recovers

- **Durable.** Writes go to a write-ahead log; on restart the engine reloads its batch files and replays the log's committed transactions ([persistence](https://inputlayer.ai/docs/guides/persistence/)). Back it up like any database ([backup](docs/content/docs/guides/backup.mdx)).
- **Single writer.** One engine per data directory, one replica; never scale it out ([deployment](https://inputlayer.ai/docs/guides/deployment/)). There is no high availability today.
- **Replays are safe.** The ingestion adapter stores the last applied revision per key in the same transaction as the data, so a replayed or late event is skipped and a replayed insert cannot resurrect a retracted fact ([ingestion](docs/content/docs/guides/ingestion.mdx)).
- **Unreachable means wait.** While the engine is down no claim can win, so a tool gated by a claim waits instead of running unchecked. A subscriber that reconnects gets a fresh snapshot and continues from the exact current answer.

## Where it sits beside models

> Decision models judge. Language models think. InputLayer knows.

"Knows" means accepted facts plus rule-derived conclusions; source freshness and delivery still apply.

## What you delete

- **The trigger service and the re-check cron.** Before: a service maps change events to agents, and a cron recomputes "is anything late, eligible, pending" every few minutes. After: the agent subscribes to the view; an unchanged answer pushes nothing. The CDC feed stays and writes facts. Boundary: views do not read the clock, so a check that waits on time (an SLA that expires at 48 hours) keeps a one-line clock writer whose fact the rule reads.
- **The precomputed flags and their invalidation.** Before: a `needs_check` flag in Redis or a column, plus a handler per upstream event to delete it. After: a rule. A conclusion is retracted only when every path that supported it is gone; nothing to invalidate two hops away.
- **The policy in your prompts and tool handlers.** Before: "only auto-check when the customer allows it" in the system prompt and `if not eligible: raise` in the handler. After: policy as facts and rules, enforced where the action is recorded: the claim commits only while the policy holds. OPA still decides who may call; eligibility logic that lives in Rego only because OPA was the only policy box is a candidate to move.
- **The "already handled" rows.** Before: a `claimed_by` column or a "processed" row checked by hand. After: `claim()` returns who won, checked at commit against the live view. Temporal keeps the durable effect, keyed by the claim. Phase-1 claims do not expire; leases for crashed holders come with `WorkQueue` in phase 3.

## Before and after

The "before" is the stack a good team ships today: an agent framework, Postgres with pgvector, deterministic tool guardrails before each call, tracing, and webhooks and CDC or a reactive database for change.

| Concern | Best-practice stack today | With InputLayer in the loop |
|---|---|---|
| Where "is this order late?" is decided | A SQL view or a server function decides it once, too; what differs is liveness: the agent re-queries or re-runs it each turn | In the engine, as a rule evaluated after each relevant commit; the answer is a row the agent is pushed when it changes |
| How the agent learns a fact changed | With Postgres plus webhooks: an event names a table row and the agent re-queries and re-reasons; with a reactive database: the query re-runs and the new result is pushed | The engine also re-runs the query after each relevant commit; the difference is that the delta names the derived rows that entered and left the conclusion, so the agent knows which of its own actions the change invalidates |
| What happens when a fact is corrected | Materialize emits retractions and Convex re-pushes the result, so the data layer knows; nothing connects the retraction to what the agent already did | The next evaluation recomputes from the remaining facts and the delta withdraws the agent's own derived work: the need disappears and the running tool is cancelled. Unlike Materialize, InputLayer does not maintain views incrementally today; it re-evaluates and diffs |
| N agents watching one question (Postgres plus webhooks or CDC) | N queries per change, or a cache the team builds and invalidates | One evaluation shared across subscribers of the same question; per-session questions fan out from one subscription per process, scope as stated above |
| Reconnect, missed event (Postgres plus webhooks or CDC) | Replay from an event log if there is one; otherwise a full re-read and a duplicate-suppression layer | Snapshot, deltas, and a fresh snapshot on reconnect; the same loop |
| "May this tool run?" | Deterministic guardrails before the call (tool guardrails, LangGraph interrupts, Cedar, OPA): per-call policy over the request | Policy over live derived state (tool policy, kill switch, source health, consent), checked at commit by the claim, with cancellation derived when it changes mid-run |
| Why did the agent do that? | Traces of prompts and spans, plus policy decision logs | A proof over facts and rules for the derived row the agent acted on (`.why`), beside the traces |
| What goes | | The trigger service and re-check crons, the precomputed flags and their invalidation, the policy in prompts and handlers, the "already handled" rows |
| What stays | the framework, the model, the vector store, Postgres as system of record, the tracing | all of it; InputLayer holds the live conclusions the agent acts on |

## The agent loop with a model

*Upcoming, phase 3 of the new SDK: `watch_context()`, `actions.schema()` and `act()` are designed, not built.* The model reads a compact window rendered from the derived views, proposes actions against a schema generated at that revision, and every proposal goes through the same gate as the hero's claim. A refusal comes back as data with the failing condition, for the next turn.

```python
async def agent_loop(kg, sid, model):
    async for ctx in kg.watch_context(session=sid, include=[Claim, ToolReady, Work, Speech], budget=1500):
        if not ctx.verified:                        # connection lost or resubscribing: do nothing on a stale view
            continue
        if not ctx.changes:                         # nothing new since the last turn
            continue
        decision = await model.decide(ctx.render(), tools=kg.actions.schema(session=sid, at=ctx.revision))
        for action in kg.actions.parse(decision):
            out = await kg.act(action, session=sid)
            if out.refused:
                ctx.note(out)                       # "refund refused: source billing stale" lands in the next window
        # consequences arrive as the next ctx: the deltas the actions and the world produced
```

## Proof points

Lab measurements on a shared 32-vCPU host, not a benchmark rig:

- **Writer to delta:** 2 to 7 ms p50 for one shared question at 64 subscribers; about 25 ms p50 per session with the full reference rule pack at ten sessions.
- **Shared questions:** 64-subscriber fan-out, p99 13.8 ms.
- **The race:** twenty connections racing one guarded insert: one winner, nineteen no-ops, no errors.
- **Recursive queries** (a recursive-query result, not an agent-latency figure): after inserting 100 edges into a 2,000-node graph with transitive-closure rules, the bound query `?reach(1, Y)` (Magic Sets computes only the demanded slice) answers in **6.83 ms** against **11.3 s** for the full closure, **1,652x**; both are recomputations, not incremental maintenance ([BENCHMARKS.md](BENCHMARKS.md)).

---

## Get Started

<!-- quickstart:docker (run by .github/workflows/quickstart.yml) -->
```bash
docker run -d --name inputlayer -p 8080:8080 ghcr.io/inputlayer/inputlayer
until curl -sf http://localhost:8080/health > /dev/null; do sleep 1; done   # first boot

# On first boot the server generates the admin password and an admin API key
docker exec inputlayer cat /var/lib/inputlayer/data/credentials.toml

# The SDKs and inputlayer-client read the key from the environment
export INPUTLAYER_API_KEY=$(docker exec inputlayer sed -n 's/^api_key = "\(.*\)"/\1/p' /var/lib/inputlayer/data/credentials.toml)
```

Open [http://localhost:8080](http://localhost:8080) for the interactive GUI and sign in as `admin` with the `admin_password` from `credentials.toml`, or connect via WebSocket at `ws://localhost:8080/ws`. To choose the secrets yourself, start the container with `-e INPUTLAYER_ADMIN_PASSWORD=...` and `-e INPUTLAYER_BOOTSTRAP_API_KEY=...`, at least 12 characters each (the first boot refuses shorter ones); supplied values are never written to disk.

Or build from source (Rust 1.88+); the server writes the same file to `./data/credentials.toml`:

```bash
git clone https://github.com/inputlayer/inputlayer.git
cd inputlayer
cargo build --release
./target/release/inputlayer-server --port 8080
```

See the [Quick Start Guide](https://inputlayer.ai/docs/guides/quickstart/) to load a sample and watch conclusions change as facts do.

## SDKs

The SDKs are not on PyPI or npm yet; install them from a source checkout of this repository.

**Python** (3.10+):
```bash
git clone https://github.com/inputlayer/inputlayer.git
pip install ./inputlayer/packages/inputlayer-py
# extras: pip install "./inputlayer/packages/inputlayer-py[pandas,langchain,langgraph]"
```

**TypeScript** (Node.js 18+):
```bash
# build and pack the SDK from the checkout
(cd inputlayer/packages/inputlayer-js && npm ci && npm run build && npm pack)
# then, in your project, install the tarball under the name `inputlayer`
npm install inputlayer@file:/path/to/inputlayer/packages/inputlayer-js/inputlayer-js-dev-0.1.1.tgz
```

See the [Python SDK](packages/inputlayer-py/README.md) and [TypeScript SDK](packages/inputlayer-js/README.md) READMEs and the [Python](https://inputlayer.ai/docs/guides/python-sdk/) and [TypeScript](https://inputlayer.ai/docs/guides/js-sdk/) guides. Both fit the stack you have: [LangGraph](https://inputlayer.ai/docs/guides/langgraph/) memory, state and checkpointer; [LangChain](https://inputlayer.ai/docs/guides/langchain/) tool and retriever; and an OpenAI-compatible [fact-checking gateway](https://inputlayer.ai/docs/guides/verified-completions/).

## Under the hood: IQL

The SDKs compile to IQL, InputLayer's rule language; you can also write it directly. Connecting flights: direct routes as facts, reachable destinations derived:

```iql
// Facts: direct flight routes
+direct_flight[("New York", "London"), ("London", "Paris"), ("Paris", "Tokyo"), ("Tokyo", "Sydney")]

// Rules: you can reach a destination directly, or through connections
+can_reach(A, B) <- direct_flight(A, B)
+can_reach(A, C) <- direct_flight(A, B), can_reach(B, C)

// Query: where can you fly from New York?
?can_reach("New York", Dest)
```

```
┌────────────┬──────────┐
│ New York   │ Dest     │
├────────────┼──────────┤
│ "New York" │ "London" │
│ "New York" │ "Paris"  │
│ "New York" │ "Sydney" │
│ "New York" │ "Tokyo"  │
└────────────┴──────────┘
4 rows
```

`.why` shows which facts and rules produced a row; `.why_not` shows which condition blocked one ([explainability](https://inputlayer.ai/docs/guides/explainability/)):

```iql
.why ?can_reach("New York", "Sydney")
// [rule] can_reach(A, C) <- direct_flight(A, B), can_reach(B, C)
//   [base] direct_flight("New York", "London")
//   [rule] can_reach(A, C) <- direct_flight(A, B), can_reach(B, C)
//     [base] direct_flight("London", "Paris")
//     [rule] can_reach(A, C) <- direct_flight(A, B), can_reach(B, C)
//       [base] direct_flight("Paris", "Tokyo")
//       [rule] can_reach(A, B) <- direct_flight(A, B)
//         [base] direct_flight("Tokyo", "Sydney")
```

The same engine evaluates rules and vector similarity in one query ([vectors](https://inputlayer.ai/docs/guides/vectors/)): "similar, and compatible with this printer" is a join, not a post-filter. Recursion, stratified negation and aggregation are in the rule language ([recursion](https://inputlayer.ai/docs/guides/recursion/)).

## Ontologies, ready to go

InputLayer ships ready-made rule packs in the [ontology registry](https://github.com/inputlayer/ontology-registry), installed into a running engine with one command. The first is **`consistency-core` (Verified Completions)**: logical-consistency checks for AI conversations (contradictions, timeline cycles, identity mix-ups and policy violations), every finding backed by verbatim quoted spans and a proof tree, validated against a 1,628-scenario adversarial corpus.

```bash
il search                                      # browse the registry
il install consistency-core --kg mychat --create   # sha256-verified, one atomic deploy
il list --kg mychat                            # what's installed, pinned by version+digest
```

The `il` CLI builds with the engine (`cargo build --bin il`) and talks to the server over the same WebSocket API as every other client. It also carries schema migrations (`il migration generate / apply / revert / status`) with language-neutral JSON migration files (see the [migrations guide](https://inputlayer.ai/docs/guides/migrations/)). The rule pack's rules are human-written, reviewed in the registry and frozen at load; the LLM only ever writes data. See `docs/internals/verified-completions/` for the pack's design, benchmark corpus and extraction contract.

## Who it is for

InputLayer fits when your agent works over structured facts that change, when its decisions chain through several conditions, when acting on a stale answer costs something, and when the agent lives long enough for the world to change under it. It is not for document chat or one-shot Q&A, it is not a vector database replacement, and it is not a hosted platform.

---

## Built On

[Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) by Frank McSherry, used today as the per-query execution engine: each query runs as a fresh dataflow over a snapshot of the facts, and persistent rules are re-derived on every read. Keeping deployed rules as incrementally maintained live views is planned in [milestone 9 (#305)](https://github.com/inputlayer/inputlayer/issues/305). Single binary, no external dependencies.

## Documentation

- [Quick Start](https://inputlayer.ai/docs/guides/quickstart/)
- [Core Concepts](https://inputlayer.ai/docs/guides/core-concepts/)
- [WebSocket API and standing queries](https://inputlayer.ai/docs/guides/websocket-api/)
- [Ingestion: Postgres CDC and webhooks](docs/content/docs/guides/ingestion.mdx)
- [Explainability (.why / .why_not)](https://inputlayer.ai/docs/guides/explainability/)
- [Persistence](https://inputlayer.ai/docs/guides/persistence/) and [Deployment](https://inputlayer.ai/docs/guides/deployment/)
- [Vector Search](https://inputlayer.ai/docs/guides/vectors/)
- [Recursion](https://inputlayer.ai/docs/guides/recursion/)
- [Python SDK](https://inputlayer.ai/docs/guides/python-sdk/)
- [TypeScript SDK](https://inputlayer.ai/docs/guides/js-sdk/)

## Contributing

See [CONTRIBUTING](CONTRIBUTING).

## License

InputLayer uses a split licensing model: the core is protected, the clients are permissive.

| Component | Path | License |
|-----------|------|---------|
| **Core** (server, engine, everything not listed below) | repository root | [Elastic License 2.0](./LICENSE) |
| Python SDK | `packages/inputlayer-py` | Apache 2.0 |
| TypeScript SDK | `packages/inputlayer-js` | Apache 2.0 |
| API client | `packages/api-client` | Apache 2.0 |
| VS Code extension | `packages/inputlayer-vscode` | MIT |

**Core (Elastic License 2.0):** free to use, copy, modify, and run - including commercially and in production. You may not provide InputLayer to third parties as a hosted or managed service, and you may not circumvent license-key functionality or remove licensing notices. For rights beyond that, see [COMMERCIAL_LICENSE.md](./COMMERCIAL_LICENSE.md).

These terms apply to all versions of InputLayer, including every pre-1.0 development version preceding the official 1.0 release.

**Clients (Apache 2.0 / MIT):** embed them in any application without restriction.

"InputLayer" is a trademark of InputLayer - see [NOTICE](./NOTICE).
