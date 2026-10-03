import Link from "next/link"
import type { ReactNode } from "react"
import { ArrowRight } from "lucide-react"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"
import { StatCard } from "@/components/stat-card"
import { ComparisonTable } from "@/components/comparison-table"
import { BatchVsStreamingDiagram } from "@/components/batch-vs-streaming-diagram"
import { highlightToHtml } from "@/lib/syntax-highlight"
import { highlightGeneric } from "@/lib/generic-highlight"

const QUICKSTART_URL = "/docs/guides/quickstart/"
const WEBSOCKET_DOCS_URL = "/docs/guides/websocket-api/"
const CONTACT_URL = "mailto:sam@inputlayer.ai?subject=Design-partner%20evaluation"

// ── Code samples ────────────────────────────────────────────────────────

const batchAgentCode = `while ticket.open:                    # every turn / every 5 min
    order  = oms.get_order(id)        # re-fetch
    eta    = carrier.get_eta(order)   # re-fetch
    policy = policies.eligibility(c)  # re-fetch
    ok = eta > order.promised and policy.allows("expedite")
    ...
# + one cache-invalidation handler per upstream event
# + the handler nobody wrote for the rare case
# + a cron job that recomputes everything`

const streamingRulesCode = `// rules, written once
+late(O) <- shipment(O,S), eta(S,T), promised(O,P), T > P
+can_offer(O,C) <- late(O), customer(O,C),
                   eligible(C, "expedite")`

const streamingAgentCode = `# agent: react to what changed
for change in kg.watch("?can_offer(O, C)"):
    for row in change.added:   offer(row)
    for row in change.removed: withdraw(row)`

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
  const batchHtml = highlightGeneric(batchAgentCode, "python") ?? batchAgentCode
  const rulesHtml = highlightToHtml(streamingRulesCode)
  const agentHtml = highlightGeneric(streamingAgentCode, "python") ?? streamingAgentCode

  return (
    <div className="flex flex-col min-h-dvh">
      <SiteHeader />

      <main className="flex-1">
        {/* ── Hero ───────────────────────────────────────────────────── */}
        <section className="relative overflow-hidden border-b border-border/50">
          <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent" />
          <div className="relative mx-auto max-w-6xl px-6 py-24 lg:py-32">
            <p className="font-mono text-sm font-medium uppercase tracking-wider text-primary">
              The streaming engine for AI agents
            </p>
            <h1 className="mt-4 text-4xl sm:text-5xl lg:text-6xl font-extrabold tracking-tight leading-[1.08] max-w-4xl">
              Agents that react in milliseconds,
              <br className="hidden sm:block" /> <span className="text-primary">not on the next run.</span>
            </h1>
            <p className="mt-6 text-lg text-muted-foreground max-w-3xl">
              Today&apos;s agents are batch jobs. They wake up, re-read everything, reason, act and go back to sleep:
              stale between runs, slow to react, and paying to recompute what didn&apos;t change. InputLayer makes your
              agents streaming. Every change is reasoned over the moment it lands, only what changed is recomputed, and a
              conclusion that stops being true is withdrawn right away.
            </p>
            <div className="mt-8 flex flex-wrap gap-3">
              <a href="#start" className={primaryButton}>
                Turn one batch agent into a streaming agent
                <ArrowRight className="h-4 w-4" aria-hidden="true" />
              </a>
              <a href="#how" className={secondaryButton}>
                See how it works
              </a>
            </div>
            <p className="mt-6 text-xs text-muted-foreground">
              Self-hosted · source-available under the Elastic License 2.0 · Rust engine with Python and JS SDKs
            </p>
          </div>
        </section>

        {/* ── The problem ────────────────────────────────────────────── */}
        <Section
          eyebrow="The problem"
          title={
            <>
              Your data stack went streaming years ago.
              <br className="hidden sm:block" /> Your agents are the last batch jobs left.
            </>
          }
        >
          <Card className="overflow-x-auto">
            <BatchVsStreamingDiagram />
          </Card>
          <p className="mt-6 text-muted-foreground max-w-3xl">
            Nightly ETL became streaming. Cron jobs became event-driven services. Full refreshes became incremental
            views. Agents are still built the old way: a trigger fires, the agent fetches the world, reasons over all of
            it, acts, and stops. Everything that changes before the next trigger is invisible.
          </p>
        </Section>

        {/* ── Why streaming agents ───────────────────────────────────── */}
        <Section eyebrow="Why streaming agents" title="Three things batch agents can't do">
          <div className="grid gap-6 md:grid-cols-3">
            {[
              {
                mark: "ms",
                title: "React when it happens, not when the cron fires",
                body: "A batch agent learns about a change on its next run, seconds to hours later. A streaming agent knows within milliseconds and can act while it still matters.",
              },
              {
                mark: "−1",
                title: "Never act on something that stopped being true",
                body: "Batch agents see what's there and miss what disappeared: the cancelled order, the revoked approval, the delay that cleared. InputLayer withdraws a conclusion the moment its last reason goes away.",
              },
              {
                mark: "Δ",
                title: "Pay for change, not for re-reading the world",
                body: "Batch cost grows with data volume × how often you run. Streaming cost grows with what actually changed. Fewer recomputes, fewer fetches, fewer tokens spent re-deriving the same answer.",
              },
            ].map((reason) => (
              <Card key={reason.title} className="space-y-3">
                <p aria-hidden="true" className="font-mono text-3xl font-medium leading-none text-primary">
                  {reason.mark}
                </p>
                <h3 className="text-base font-semibold">{reason.title}</h3>
                <p className="text-sm text-muted-foreground">{reason.body}</p>
              </Card>
            ))}
          </div>
          <Card className="mt-6 space-y-3">
            <h3 className="text-base font-semibold">The math every lead runs in their head</h3>
            <p className="text-muted-foreground">
              10,000 open support tickets. The agent re-checks each one every 5 minutes. That is{" "}
              <strong className="text-foreground">2.9 million evaluations a day</strong>, almost all on tickets where
              nothing changed, and it is still up to 5 minutes late on the ones that did. A streaming agent works only on
              the tickets whose facts changed, at the moment they change.
            </p>
            <p className="text-xs text-muted-foreground">Illustrative arithmetic, not a benchmark.</p>
          </Card>
        </Section>

        {/* ── How it works ───────────────────────────────────────────── */}
        <Section id="how" eyebrow="How it works" title="Feed it facts. Write the rules once. React to what changes.">
          <ol className="grid gap-6 md:grid-cols-3">
            {[
              {
                tag: "1 · Stream facts in",
                title: "From the systems you already run",
                body: "Orders, carriers, permissions, inventory: write facts as they change, from CDC, webhooks or your app. Your systems of record stay where they are.",
              },
              {
                tag: "2 · Declare the reasoning",
                title: "Rules, not glue code",
                body: "\"An order is late if its ETA passed its promise.\" \"A replacement may be offered if the order is late and the customer is eligible.\" Rules chain, recurse and combine with vector similarity.",
              },
              {
                tag: "3 · React to changes",
                title: "Your agent gets only what changed",
                body: "When a fact changes, InputLayer updates just the affected conclusions and tells the agent what was added and what was withdrawn, with the facts and rules behind each one.",
              },
            ].map((step) => (
              <li key={step.tag} className="rounded-xl border border-border bg-card p-6 min-w-0 space-y-3">
                <Tag>{step.tag}</Tag>
                <h3 className="text-base font-semibold">{step.title}</h3>
                <p className="text-sm text-muted-foreground">{step.body}</p>
              </li>
            ))}
          </ol>

          <div className="mt-6 grid gap-6 lg:grid-cols-2">
            <Card>
              <Tag tone="bad">Batch agent today</Tag>
              <CodeBlock html={batchHtml} label="Batch agent loop that re-fetches every input on every turn" />
            </Card>
            <Card>
              <Tag tone="ok">Streaming agent with InputLayer</Tag>
              <CodeBlock html={rulesHtml} label="Rules written once" />
              <CodeBlock html={agentHtml} label="Agent loop that reacts to added and removed rows" />
              <p className="mt-3 text-xs text-muted-foreground">
                The SDK form shown is the upcoming release; standing queries run over the{" "}
                <Link href={WEBSOCKET_DOCS_URL} className="text-primary hover:underline">
                  WebSocket API
                </Link>{" "}
                today.
              </p>
            </Card>
          </div>

          <p className="mt-8 border-l-4 border-primary pl-4 text-lg font-semibold max-w-3xl">
            Deleted: the re-fetching, the invalidation handlers, the recompute job, the polling loop. Added: a few rules
            and one watch.
          </p>
        </Section>

        {/* ── Proof ──────────────────────────────────────────────────── */}
        <Section eyebrow="Proof" title="Measured, not promised">
          <div className="grid gap-6 md:grid-cols-3">
            <StatCard
              value="5 ms"
              description="typical time from a committed change to the agent receiving the update (13 ms worst case), warm, engine-side"
            />
            <StatCard
              value="96"
              description="on real retail data in our streaming-agents benchmark, checked after every change"
            />
            <StatCard
              value="100%"
              description="of live answers equal to a fresh full query at every checkpoint; every withdrawal check passed"
            />
          </div>
          <p className="mt-6 text-xs text-muted-foreground max-w-3xl">
            Preliminary results from a shared development machine; official numbers will be published from a dedicated
            benchmark host with raw samples. Model latency and ingestion are on top of engine time.
          </p>
        </Section>

        {/* ── Where it fits ──────────────────────────────────────────── */}
        <Section eyebrow="Where it fits" title="Built for agents that act on a world that keeps changing">
          <ComparisonTable
            rowHeader="Agent"
            align="left"
            columns={["What changes under it", "What streaming gives you"]}
            rows={[
              {
                capability: "Support & customer service",
                values: {
                  "What changes under it": "Order status, ETAs, refunds, eligibility",
                  "What streaming gives you": "Promises that stay true while the ticket is open",
                },
              },
              {
                capability: "Fulfilment & logistics",
                values: {
                  "What changes under it": "Carrier events, stock, capacity",
                  "What streaming gives you": "Re-plans the moment a shipment slips",
                },
              },
              {
                capability: "Risk, fraud & compliance",
                values: {
                  "What changes under it": "Transactions, sanctions, approvals",
                  "What streaming gives you": "Blocks and releases as soon as the facts change",
                },
              },
              {
                capability: "Operations & monitoring",
                values: {
                  "What changes under it": "Metrics, incidents, dependencies",
                  "What streaming gives you": "Root cause and impact recomputed per event, not per sweep",
                },
              },
              {
                capability: "Voice & real-time assistants",
                values: {
                  "What changes under it": "What the user just said, what the world just did",
                  "What streaming gives you": "Corrects itself mid-sentence instead of on the next turn",
                },
              },
            ]}
          />
          <p className="mt-6 text-sm text-muted-foreground max-w-3xl">
            Keep your LLM, your vector store for documents and your systems of record. InputLayer is the streaming layer
            between your data and your agent&apos;s decisions.
          </p>
        </Section>

        {/* ── Compared with what you'd build yourself ────────────────── */}
        <Section eyebrow="Compared with what you'd build yourself" title="The streaming layer agents never had">
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

        {/* ── Get started ────────────────────────────────────────────── */}
        <Section id="start" eyebrow="Get started" title="Turn one batch agent into a streaming agent">
          <div className="grid gap-6 lg:grid-cols-2">
            <Card className="space-y-3">
              <h3 className="text-base font-semibold">Run it yourself</h3>
              <p className="text-sm text-muted-foreground">
                Install the engine, load a sample, and watch an agent react to changes in a few minutes. Free to
                self-host.
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
                Bring one batch agent. In four weeks we turn it into a streaming agent on your data and measure the
                difference in freshness, correctness and cost.
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
