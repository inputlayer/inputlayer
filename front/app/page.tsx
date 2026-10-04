import Link from "next/link"
import type { ReactNode } from "react"
import { ArrowRight } from "lucide-react"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"
import { ComparisonTable } from "@/components/comparison-table"
import { BatchVsLiveDiagram } from "@/components/batch-vs-live-diagram"
import { highlightGeneric } from "@/lib/generic-highlight"

const QUICKSTART_URL = "/docs/guides/quickstart/"
const WEBSOCKET_DOCS_URL = "/docs/guides/websocket-api/"
const INGESTION_DOCS_URL = "/docs/guides/ingestion/"
const PERSISTENCE_DOCS_URL = "/docs/guides/persistence/"
const DEPLOYMENT_DOCS_URL = "/docs/guides/deployment/"
const PYTHON_SDK_URL = "/docs/guides/python-sdk/"
const BENCHMARK_URL = "/blog/benchmarks-1587x-faster-recursive-queries/"
const ARTICLE_URL = "/blog/building-a-voice-agent-that-knows/"
const SCALE_PR_URL = "https://github.com/inputlayer/inputlayer/pull/232"
const CONTACT_URL = "mailto:sam@inputlayer.ai?subject=Design-partner%20evaluation"

// ── Code samples ────────────────────────────────────────────────────────

// The agent loop. subscribe() and claim() are the phase-1 SDK, in review:
// keep the label under this block until they ship in a release.
const loopCode = `async def carrier_check(shipment: str) -> None:            # the tool; a real one calls the carrier API with an idempotency key
    await asyncio.sleep(3); print(f"checked {shipment}")

async def main() -> None:
    url, key = os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"), os.environ["INPUTLAYER_API_KEY"]
    async with InputLayer(url, api_key=key) as il:
        kg = il.knowledge_graph("support"); running = {}
        await kg.define(Shipment, Eta, Promised, ToolPolicy, KillSwitch, Attempt); await kg.define_rules(CheckNeeded)
        async for change in kg.subscribe(CheckNeeded):     # the engine wakes the agent: replaces the trigger service and the data re-check cron
            for row in change.retracted:                   # first: a need vanished (back on time, killed, re-shipped): stop it and free it
                if entry := running.pop(row.order, None):
                    task, attempt = entry; task.cancel(); await kg.retract(attempt)
            for row in change.inserted:                    # then: a need appeared: claim it once, then act
                c = await kg.claim(Attempt(order=row.order, tool="carrier_check", attempt=uuid.uuid4().hex[:8]),
                                   when=[CheckNeeded.any(order=row.order)], unless=Attempt.any(order=row.order, tool="carrier_check"))
                if c.won: running[row.order] = (asyncio.create_task(carrier_check(row.shipment)), c.holder)   # the claim: replaces the "already handled" row

asyncio.run(main())`

// The declarations. This block runs on today's Python SDK (define, define_rules,
// insert and query); the loop above is the part that waits for the next release.
const rulesCode = `import asyncio, os, uuid
from inputlayer import InputLayer, Relation, Derived, From

class Shipment(Relation):   order: str; shipment: str
class Eta(Relation):        shipment: str; due: str
class Promised(Relation):   order: str; due: str
class ToolPolicy(Relation): tool: str; mode: str          # policy as facts, deployed with the pack, flipped by operators:
class KillSwitch(Relation): tool: str                     #   replaces "only auto-check when..." prose in the prompt
class Attempt(Relation):    order: str; tool: str; attempt: str

class CheckNeeded(Derived):                                # the rule: replaces the precomputed needs_check flag and the cron that rebuilt it
    order: str; shipment: str
    rules = [From(Shipment, Eta, Promised, ToolPolicy)
             .where(lambda s, e, p, t: (e.shipment == s.shipment) & (p.order == s.order) & (e.due > p.due)
                                     & (t.tool == "carrier_check") & (t.mode == "auto") & ~t.tool.in_(KillSwitch.tool))
             .select(order=Shipment.order, shipment=Shipment.shipment)]`

const todayAgentCode = `while ticket.open:                       # the cron re-check, every 5 min
    order  = oms.get_order(id)           # re-fetch
    eta    = carrier.get_eta(order)      # re-fetch
    if redis.get(f"needs_check:{id}"):   # a cached flag someone must invalidate
        if db.mark_handled(id, me):      # an "already handled" row so two agents don't both start
            prompt = render(order, eta, POLICY_PROSE)  # "only auto-check when..." in the prompt
            llm.decide(prompt)           # the model is trusted to follow it
# + one invalidation handler per upstream event
# + the cancel path nobody wrote for the rare case`

// ── Section building blocks ─────────────────────────────────────────────

function Section({ id, eyebrow, title, children }: { id?: string; eyebrow: string; title: ReactNode; children: ReactNode }) {
  const headingId = id ? `${id}-heading` : undefined
  return (
    <section id={id} aria-labelledby={headingId} className="border-b border-border/50 scroll-mt-16">
      <div className="mx-auto max-w-6xl px-6 py-20">
        <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">{eyebrow}</p>
        <h2 id={headingId} className="text-3xl font-bold tracking-tight max-w-3xl mb-10">
          {title}
        </h2>
        {children}
      </div>
    </section>
  )
}

function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <div className={`rounded-xl border border-border bg-card p-6 min-w-0 ${className}`}>{children}</div>
}

function Tag({ children, tone = "neutral" }: { children: ReactNode; tone?: "neutral" | "bad" | "ok" }) {
  const toneClass =
    tone === "bad"
      ? "bg-destructive/15 text-foreground"
      : tone === "ok"
        ? "bg-[var(--success)]/20 text-foreground"
        : "bg-muted text-muted-foreground"
  return (
    <span className={`inline-block rounded-full px-2.5 py-0.5 font-mono text-xs ${toneClass}`}>{children}</span>
  )
}

function CodeBlock({ html, label }: { html: string; label: string }) {
  return (
    <pre
      aria-label={label}
      className="mt-3 rounded-lg bg-[var(--code-bg)] p-4 overflow-x-auto text-xs leading-relaxed font-mono"
    >
      <code dangerouslySetInnerHTML={{ __html: html }} />
    </pre>
  )
}

const primaryButton =
  "inline-flex items-center gap-2 rounded-md bg-primary px-5 py-2.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
const secondaryButton =
  "inline-flex items-center gap-2 rounded-md border border-border bg-background px-5 py-2.5 text-sm font-medium hover:bg-secondary transition-colors"

// ── Page ─────────────────────────────────────────────────────────────────

export default function LandingPage() {
  const loopHtml = highlightGeneric(loopCode, "python") ?? loopCode
  const rulesHtml = highlightGeneric(rulesCode, "python") ?? rulesCode
  const todayHtml = highlightGeneric(todayAgentCode, "python") ?? todayAgentCode

  return (
    <div className="flex flex-col min-h-dvh">
      <SiteHeader />

      <main className="flex-1">
        {/* ── Hero ───────────────────────────────────────────────────── */}
        <section className="relative overflow-hidden border-b border-border/50">
          <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent" />
          <div className="relative mx-auto max-w-6xl px-6 py-24 lg:py-32">
            <p className="font-mono text-sm font-medium uppercase tracking-wider text-primary">
              The live rules engine for AI agents
            </p>
            <h1 className="mt-4 text-5xl sm:text-6xl lg:text-7xl font-extrabold tracking-tight leading-[1.05] max-w-4xl">
              Take the rules out of <span className="text-primary">your prompts.</span>
            </h1>
            <p className="mt-6 text-xl sm:text-2xl max-w-3xl">
              <strong className="font-semibold">Your agents act on what&apos;s true now.</strong>{" "}
              <span className="text-muted-foreground">
                Declare your facts and rules once. InputLayer keeps every conclusion current as facts change, tells your
                agents what changed, and records an agent&apos;s action only while the rules allow it, so the tool runs
                only then.
              </span>
            </p>
            <p className="mt-4 text-lg text-muted-foreground max-w-3xl">
              A rules engine, made live: a conclusion is retracted when its facts stop supporting it, the exact
              change is pushed to every subscribed agent, and any row can be explained with a proof, on request. The
              model proposes; the rules decide.
            </p>
            <div className="mt-8 flex flex-wrap gap-3">
              <Link href={QUICKSTART_URL} className={primaryButton}>
                Quickstart
                <ArrowRight className="h-4 w-4" aria-hidden="true" />
              </Link>
              <a href="#how" className={secondaryButton}>
                See the code
              </a>
            </div>
            <p className="mt-6 text-xs text-muted-foreground max-w-3xl">
              Self-hosted, single node. Measured today for tens of concurrent agent sessions per knowledge graph; a change
              that lifts this is in progress (draft:{" "}
              <a href={SCALE_PR_URL} className="text-primary hover:underline">
                pull request 232
              </a>
              ). Python and TypeScript SDKs; subscriptions and claims ship with the next release, standing queries run over
              WebSocket today.
            </p>
            <p className="mt-2 text-xs text-muted-foreground">
              Source-available under the Elastic License 2.0 · Rust engine
            </p>
          </div>
        </section>

        {/* ── Replaces / Keeps ───────────────────────────────────────── */}
        <Section id="replaces" eyebrow="What it replaces" title="Four pieces of glue go. Every product in your stack stays.">
          <div className="grid gap-6 lg:grid-cols-2">
            <Card className="space-y-4">
              <Tag tone="bad">Replaces</Tag>
              <ul className="space-y-3 text-sm">
                {[
                  "The trigger service and the crons that re-check data for changes (a check that waits on time keeps a one-line clock writer).",
                  "The precomputed eligibility flags you cache and invalidate.",
                  "The business rules you wrote into system prompts and tool handlers (and the eligibility logic that ended up in OPA policies).",
                  "The “already handled” rows and claimed_by columns that keep two agents from doing the same work (TTL locks follow, with leases, in a later release).",
                ].map((item) => (
                  <li key={item} className="flex gap-3">
                    <span aria-hidden="true" className="text-primary font-mono">
                      &minus;
                    </span>
                    <span>{item}</span>
                  </li>
                ))}
              </ul>
            </Card>
            <Card className="space-y-4">
              <Tag tone="ok">Keeps</Tag>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Your models and gateway, LangGraph, MCP tool servers, Zep, pgvector, Redis for plain lookups, your systems
                of record, Debezium/Kafka and webhooks (they feed InputLayer), OPA for who may call, NeMo for content
                safety, Temporal for durable effects (its workflow id becomes the claim key), LangSmith/OTel.
              </p>
            </Card>
          </div>
        </Section>

        {/* ── Where it sits ──────────────────────────────────────────── */}
        <Section id="where" eyebrow="Where it sits" title="Beside your tools, not in their call path">
          <div className="space-y-4 text-muted-foreground max-w-3xl">
            <p>
              Facts arrive from your CDC feed and webhooks through a small adapter you run (recipe shipped for{" "}
              <Link href={INGESTION_DOCS_URL} className="text-primary hover:underline">
                Debezium Server and signed webhooks
              </Link>
              ), with per-key revisions, so a late or replayed event never overwrites a newer one.
            </p>
            <p>
              InputLayer is not in your tool&apos;s call path: your agent claims the action in InputLayer, the claim is
              recorded only if the rules hold at that instant, and your handler or Temporal runs the tool only if the claim
              won, with the claim as its idempotency key.
            </p>
            <p>
              If a supporting fact changes afterwards, the need is retracted and the running work is cancelled; an effect
              that already landed is yours to compensate, as it is today.
            </p>
            <p>
              How stale is too stale is a rule too: adapters renew a health lease, a claim can require that lease to be
              current at commit, and a watchdog marks a silent source stale, so a tool is refused while its source is
              stale.
            </p>
            <p>
              It is one node today: facts are durable in its{" "}
              <Link href={PERSISTENCE_DOCS_URL} className="text-primary hover:underline">
                write-ahead log
              </Link>
              , a restart resumes from it and the adapter&apos;s revisions make a replayed feed safe, and while it is
              unreachable no claim can win, so gated tools wait rather than run unchecked.
            </p>
          </div>
        </Section>

        {/* ── The code ───────────────────────────────────────────────── */}
        <Section id="how" eyebrow="How it works" title="Observe the rule, claim the action, act">
          <p className="text-muted-foreground max-w-3xl">
            A shipment runs late against its promise, so a carrier check is needed. The agent is woken, claims the check
            once, and runs it. A kill switch mid-run retracts the need, so the same loop cancels the check and releases
            the claim; lifting the switch starts it again. The comments name what each block deletes.
          </p>
          <div className="mt-8 space-y-6">
            <Card>
              <div className="flex flex-wrap gap-2">
                <Tag tone="ok">The agent loop</Tag>
                <Tag>Upcoming Python SDK: subscribe() and claim() ship with the next release</Tag>
              </div>
              <CodeBlock html={loopHtml} label="Agent loop that is woken by a rule, claims the check once and cancels it when the need is retracted" />
            </Card>
            <Card>
              <div className="flex flex-wrap gap-2">
                <Tag tone="ok">The facts, the policy and the rule</Tag>
                <Tag>Runs on today&apos;s Python SDK</Tag>
              </div>
              <CodeBlock html={rulesHtml} label="Relations, policy facts and the derived rule, declared in Python" />
            </Card>
          </div>
          <p className="mt-6 text-sm text-muted-foreground max-w-3xl">
            No model in this loop, on purpose: it shows the gate. Where a model proposes an action, the same claim decides
            whether it is recorded. Until the next release, the same standing query runs over the{" "}
            <Link href={WEBSOCKET_DOCS_URL} className="text-primary hover:underline">
              WebSocket API
            </Link>
            ; the{" "}
            <Link href={PYTHON_SDK_URL} className="text-primary hover:underline">
              Python SDK guide
            </Link>{" "}
            covers the declarations.
          </p>
        </Section>

        {/* ── Capabilities ───────────────────────────────────────────── */}
        <Section eyebrow="What you get" title="What the engine does that the glue did by hand">
          <div className="grid gap-6 md:grid-cols-2">
            {[
              {
                title: "Told what changed, including what stopped being true.",
                body: "When a fact changes, the conclusions that stop holding are retracted and the new ones pushed to every subscribed agent as exact deltas, with a proof on request.",
                replaces: "Replaces the trigger service, the re-check cron and the flags",
              },
              {
                title: "No duplicate starts, no action the rules do not support when it commits.",
                body: "One agent’s claim per need, checked at commit against the live policy view; twenty connections racing one claim gave one winner and no errors. The effect runs outside the engine with the claim key as its idempotency key. In the reference architecture a model’s output never authorizes an action by itself.",
                replaces: "Replaces the policy prose, the handler guards and the “already handled” rows",
              },
              {
                title: "Cancellation derived from state, not left to the model.",
                body: "The need leaves the view when the facts or the policy stop supporting it, and the same loop that started the work cancels it.",
                replaces: "Replaces the cron and the hand-written cancel paths",
              },
              {
                title: "Scope, then sharing.",
                body: "Measured today for tens of concurrent sessions per tenant knowledge graph with per-session subscriptions; many agents asking the same question share one evaluation (64-subscriber fan-out p99 13.8 ms); per-session questions fan out from one subscription per process until the engine-side change lands.",
                replaces: "Rules, transactions, standing queries and proofs in one engine",
              },
            ].map((cap, i) => (
              <Card key={cap.title} className="space-y-3">
                <p aria-hidden="true" className="font-mono text-sm font-medium text-primary">
                  {String(i + 1).padStart(2, "0")}
                </p>
                <h3 className="text-base font-semibold">{cap.title}</h3>
                <p className="text-sm text-muted-foreground">{cap.body}</p>
                <p className="text-xs font-mono text-muted-foreground/80">{cap.replaces}</p>
              </Card>
            ))}
          </div>
        </Section>

        {/* ── How it recovers ────────────────────────────────────────── */}
        <Section eyebrow="How it recovers" title="One node, a durable log, and tools that wait">
          <div className="grid gap-6 md:grid-cols-3">
            {[
              {
                title: "Durable",
                body: "Every committed transaction is in the write-ahead log; a restart replays it. The adapter’s per-key revisions make a replayed feed safe.",
              },
              {
                title: "Single writer",
                body: "One engine per data directory and one replica. There is no high availability today; back it up like any stateful service.",
              },
              {
                title: "Fails closed",
                body: "While the engine is unreachable no claim can win, so a tool gated by a claim waits instead of running unchecked.",
              },
            ].map((item) => (
              <Card key={item.title} className="space-y-2">
                <h3 className="text-base font-semibold">{item.title}</h3>
                <p className="text-sm text-muted-foreground">{item.body}</p>
              </Card>
            ))}
          </div>
          <p className="mt-6 text-sm text-muted-foreground max-w-3xl">
            See the{" "}
            <Link href={PERSISTENCE_DOCS_URL} className="text-primary hover:underline">
              persistence
            </Link>{" "}
            and{" "}
            <Link href={DEPLOYMENT_DOCS_URL} className="text-primary hover:underline">
              deployment
            </Link>{" "}
            guides.
          </p>
        </Section>

        {/* ── The stack line ─────────────────────────────────────────── */}
        <Section eyebrow="Beside your models" title="Decision models judge. Language models think. InputLayer knows.">
          <div className="grid gap-6 md:grid-cols-3">
            {[
              {
                tag: "Decision models",
                title: "Judge",
                body: "Small models that turn messy input into a typed choice: which question was asked, which command was meant.",
              },
              {
                tag: "Language models",
                title: "Think",
                body: "Language, planning and judgement in situations nobody wrote a rule for. They write the sentence and propose the plan.",
              },
              {
                tag: "InputLayer",
                title: "Knows",
                body: "The facts you supply and what your rules derive from them, kept current as facts change, with the evidence behind each answer.",
              },
            ].map((job) => (
              <Card key={job.tag} className="space-y-3">
                <Tag tone={job.tag === "InputLayer" ? "ok" : "neutral"}>{job.tag}</Tag>
                <h3 className="text-2xl font-bold">{job.title}</h3>
                <p className="text-sm text-muted-foreground">{job.body}</p>
              </Card>
            ))}
          </div>
          <p className="mt-6 text-muted-foreground max-w-3xl">
            Models keep the thinking. The knowledge graph is what the engine holds: the facts and rule-derived
            conclusions of one domain. The rules engine is what keeps it current and enforces it.
          </p>
        </Section>

        {/* ── What you delete ────────────────────────────────────────── */}
        <Section eyebrow="What you delete" title="The agent loop most teams run today">
          <div className="grid gap-6 lg:grid-cols-2">
            <Card>
              <Tag tone="bad">Today: rules in the prompt, state re-checked on a timer</Tag>
              <CodeBlock html={todayHtml} label="Agent loop that re-fetches, reads a cached flag, marks it handled and puts policy prose in the prompt" />
            </Card>
            <Card className="space-y-3">
              <Tag tone="ok">With InputLayer</Tag>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li>
                  <strong className="text-foreground">The re-check cron</strong> becomes a subscription to the rule: the
                  agent is woken when the answer changes, and an unchanged answer pushes nothing.
                </li>
                <li>
                  <strong className="text-foreground">The cached flag and its invalidation</strong> become a view the
                  engine keeps current, retracted when its facts stop supporting it.
                </li>
                <li>
                  <strong className="text-foreground">The policy prose</strong> becomes facts and a rule, enforced when the
                  claim commits rather than trusted to the model.
                </li>
                <li>
                  <strong className="text-foreground">The &ldquo;already handled&rdquo; row</strong> becomes a claim: one
                  winner per need, released when the need is retracted.
                </li>
              </ul>
              <p className="text-xs text-muted-foreground">
                One boundary: a cron that exists because time passes (an SLA that expires at 48 hours) stays as a one-line
                tick writer whose fact the rule reads, because rules do not read the clock.
              </p>
            </Card>
          </div>
        </Section>

        {/* ── Compared with what you'd build yourself ────────────────── */}
        <Section eyebrow="Compared with what you'd build yourself" title="What each option does when a fact changes">
          <ComparisonTable
            rowHeader=""
            align="left"
            highlightColumn="InputLayer"
            columns={["Re-query + cache + cron", "Kafka / CDC alone", "Vector store / memory layer", "InputLayer"]}
            rows={[
              {
                capability: "Knows something changed",
                values: {
                  "Re-query + cache + cron": "On the next run",
                  "Kafka / CDC alone": "Yes, raw events",
                  "Vector store / memory layer": "When re-indexed",
                  InputLayer: "Yes",
                },
              },
              {
                capability: "Knows which conclusions changed",
                values: {
                  "Re-query + cache + cron": "No, you recompute",
                  "Kafka / CDC alone": "No, you write the logic",
                  "Vector store / memory layer": "No",
                  InputLayer: "Yes, only affected ones",
                },
              },
              {
                capability: "Withdraws conclusions that stop holding",
                values: {
                  "Re-query + cache + cron": "Hand-written invalidation",
                  "Kafka / CDC alone": "Hand-written",
                  "Vector store / memory layer": "No",
                  InputLayer: "Built in",
                },
              },
              {
                capability: "Records an action only while the rules hold",
                values: {
                  "Re-query + cache + cron": "Rows and checks you write",
                  "Kafka / CDC alone": "No",
                  "Vector store / memory layer": "No",
                  InputLayer: "Yes, checked at commit",
                },
              },
              {
                capability: "Explains why an answer holds",
                values: {
                  "Re-query + cache + cron": "No",
                  "Kafka / CDC alone": "No",
                  "Vector store / memory layer": "Similarity scores",
                  InputLayer: "Facts and rules behind each row",
                },
              },
              {
                capability: "Where it wins",
                values: {
                  "Re-query + cache + cron": "Simple, already there",
                  "Kafka / CDC alone": "Durable event transport",
                  "Vector store / memory layer": "Unstructured text",
                  InputLayer: "Changing structured facts with chained rules",
                },
              },
            ]}
          />
        </Section>

        {/* ── Proof points ───────────────────────────────────────────── */}
        <Section eyebrow="Proof points" title="Measured, and labelled for what they measure">
          <div className="grid gap-6 lg:grid-cols-2">
            <Card className="space-y-4">
              <Tag>Recursive-query benchmark</Tag>
              <div className="flex flex-wrap gap-8">
                <div>
                  <span className="text-4xl font-extrabold text-primary">6.83ms</span>
                  <p className="text-xs text-muted-foreground mt-1">incremental update</p>
                </div>
                <div>
                  <span className="text-4xl font-extrabold text-muted-foreground/60">11.3s</span>
                  <p className="text-xs text-muted-foreground mt-1">full recompute</p>
                </div>
                <div>
                  <span className="text-4xl font-extrabold text-primary">1,652x</span>
                  <p className="text-xs text-muted-foreground mt-1">faster</p>
                </div>
              </div>
              <p className="text-sm text-muted-foreground">
                One new edge in a graph with 400,000 derived relationships: only the affected conclusions update. A
                recursive-query result, not an agent-latency figure.{" "}
                <Link href={BENCHMARK_URL} className="text-primary hover:underline">
                  Read the benchmark
                </Link>
                .
              </p>
            </Card>
            <Card className="space-y-4">
              <Tag>Commit gate</Tag>
              <div className="flex flex-wrap gap-8">
                <div>
                  <span className="text-4xl font-extrabold text-primary">20</span>
                  <p className="text-xs text-muted-foreground mt-1">connections racing one claim</p>
                </div>
                <div>
                  <span className="text-4xl font-extrabold text-primary">1</span>
                  <p className="text-xs text-muted-foreground mt-1">winner</p>
                </div>
                <div>
                  <span className="text-4xl font-extrabold text-primary">0</span>
                  <p className="text-xs text-muted-foreground mt-1">errors</p>
                </div>
              </div>
              <p className="text-sm text-muted-foreground">
                The others get a clean no-op, not a conflict to retry. The claim is a guarded insert the engine checks at
                commit against the live view.
              </p>
            </Card>
          </div>
        </Section>

        {/* ── Flagship demo ──────────────────────────────────────────── */}
        <Section eyebrow="Flagship demo" title="A voice agent built on the same rules">
          <ol className="grid gap-4 md:grid-cols-4">
            {[
              { tag: "1 · Ask", body: "\"Where's order 4821, can it still make Friday?\"" },
              { tag: "2 · Pick the question", body: "A small intent model maps it to a known question: ask_status(4821)." },
              { tag: "3 · Answer exactly", body: "InputLayer answers \"due Thursday\" from live facts, with the facts and rules behind it." },
              { tag: "4 · Speak", body: "A template speaks the answer. No generative model on this path." },
            ].map((step) => (
              <li key={step.tag} className="rounded-xl border border-border bg-card p-5 min-w-0 space-y-2">
                <Tag>{step.tag}</Tag>
                <p className="text-sm">{step.body}</p>
              </li>
            ))}
          </ol>

          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            <Card className="space-y-3">
              <Tag tone="ok">The world changes mid-sentence</Tag>
              <p className="text-sm text-muted-foreground">
                The carrier update lands while the agent is speaking. The old answer is withdrawn, the unplayed audio is
                dropped, and the agent says <strong className="text-foreground">&ldquo;Correction: Friday&rdquo;</strong>{" "}
                while a carrier check runs in parallel. No new turn, no new prompt.
              </p>
            </Card>
            <Card className="space-y-3">
              <Tag>Open questions go to the model</Tag>
              <p className="text-sm text-muted-foreground">
                &ldquo;Why is it late, and what would you do?&rdquo; goes to the LLM, with the current answers already in
                its context. The model proposes; an action it proposes is recorded only if the rules allow it.
              </p>
            </Card>
          </div>
          <Link
            href={ARTICLE_URL}
            className="group mt-6 block rounded-xl border border-border bg-card p-8 space-y-3 transition-colors hover:border-primary/30 hover:bg-card/80 max-w-3xl"
          >
            <h3 className="text-xl font-semibold group-hover:text-primary transition-colors">
              Building a Voice Agent That Knows: A Voice Pipeline with InputLayer at Its Heart
            </h3>
            <p className="text-sm text-muted-foreground leading-relaxed">
              The reference architecture, being built on today&apos;s standing queries: known questions answered without
              a model, and an agent that corrects itself when the world changes.
            </p>
            <span className="inline-flex items-center gap-1 text-sm text-primary font-medium pt-1">
              Read the article <ArrowRight className="h-3.5 w-3.5" aria-hidden="true" />
            </span>
          </Link>
        </Section>

        {/* ── Fits your stack ────────────────────────────────────────── */}
        <Section eyebrow="Fits your stack" title="Keep your models and framework">
          <ComparisonTable
            rowHeader="Integration"
            align="left"
            columns={["What it gives your agent"]}
            rows={[
              {
                capability: "Change triggers",
                values: {
                  "What it gives your agent": "Standing queries that tell your agent which answers were added and withdrawn, so it wakes on change",
                },
              },
              {
                capability: "LangGraph memory, state and checkpointer",
                values: {
                  "What it gives your agent": "Graph state, semantic memory and resumable checkpoints stored as facts, with routing by rules",
                },
              },
              {
                capability: "LangChain tool and retriever",
                values: {
                  "What it gives your agent": "Typed tools generated from your relations and a retriever over derived answers; the model never writes queries",
                },
              },
              {
                capability: "OpenAI-compatible fact-checking gateway",
                values: {
                  "What it gives your agent": "Point any OpenAI SDK at it; conversations become facts and your rules check them, with quoted evidence",
                },
              },
            ]}
          />
          <p className="mt-6 text-sm text-muted-foreground max-w-3xl">
            See the{" "}
            <Link href={WEBSOCKET_DOCS_URL} className="text-primary hover:underline">
              WebSocket API
            </Link>
            ,{" "}
            <Link href="/docs/guides/langgraph/" className="text-primary hover:underline">
              LangGraph
            </Link>
            ,{" "}
            <Link href="/docs/guides/langchain/" className="text-primary hover:underline">
              LangChain
            </Link>{" "}
            and{" "}
            <Link href="/docs/guides/verified-completions/" className="text-primary hover:underline">
              gateway
            </Link>{" "}
            guides.
          </p>
        </Section>

        {/* ── Why now ────────────────────────────────────────────────── */}
        <Section
          eyebrow="Why now"
          title={
            <>
              Your data stack went live years ago.
              <br className="hidden sm:block" /> Your agents are the last batch jobs left.
            </>
          }
        >
          <Card className="overflow-x-auto">
            <BatchVsLiveDiagram />
          </Card>
          <p className="mt-6 text-muted-foreground max-w-3xl">
            Nightly ETL became change data capture. Cron jobs became event-driven services. Full refreshes became
            incremental views. Agents are still built the old way: a trigger fires, the agent fetches the world, reasons
            over all of it, acts, and stops. Everything that changes before the next trigger is invisible. In the 2000s
            the rules engine took business rules out of application code; a live rules engine takes them out of the
            prompt and keeps them current while the agent works.
          </p>
          <p className="mt-6 text-sm text-muted-foreground max-w-3xl">
            A good fit when the agent acts on structured facts that change, the answer is derived through a chain of facts
            or rules, a stale answer has a real cost, and the agent lives long enough for the world to change under it.
            Document chat and one-shot Q&amp;A are better served by retrieval alone.
          </p>
        </Section>

        {/* ── Get started ────────────────────────────────────────────── */}
        <Section id="start" eyebrow="Get started" title="Pilot it on one flow">
          <div className="grid gap-6 lg:grid-cols-2">
            <Card className="space-y-3">
              <h3 className="text-base font-semibold">Run it yourself</h3>
              <p className="text-sm text-muted-foreground">
                Run the engine, declare a few facts and a rule, and watch conclusions change as facts do. Free to
                self-host; the SDKs install from source until the next release is published.
              </p>
              <div className="flex flex-wrap gap-3 pt-2">
                <Link href={QUICKSTART_URL} className={primaryButton}>
                  Quickstart
                  <ArrowRight className="h-4 w-4" aria-hidden="true" />
                </Link>
                <Link href="/docs/" className={secondaryButton}>
                  Read the docs
                </Link>
              </div>
            </Card>
            <Card className="space-y-3">
              <h3 className="text-base font-semibold">Design-partner evaluation</h3>
              <p className="text-sm text-muted-foreground">
                Talk to us about a four-week evaluation on your data: bring one agent flow, and together we move its rules
                out of the prompt and measure what that does for correctness, duplicate work and cost.
              </p>
              <div className="flex flex-wrap gap-3 pt-2">
                <a href={CONTACT_URL} className={secondaryButton}>
                  Talk to us
                </a>
              </div>
            </Card>
          </div>
        </Section>
      </main>

      <SiteFooter />
    </div>
  )
}
