import Link from "next/link"
import type { ReactNode } from "react"
import { ArrowRight } from "lucide-react"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"
import { ComparisonTable } from "@/components/comparison-table"
import { BatchVsLiveDiagram } from "@/components/batch-vs-live-diagram"
import { highlightToHtml } from "@/lib/syntax-highlight"
import { highlightGeneric } from "@/lib/generic-highlight"

const QUICKSTART_URL = "/docs/guides/quickstart/"
const WEBSOCKET_DOCS_URL = "/docs/guides/websocket-api/"
const CONTACT_URL = "mailto:sam@inputlayer.ai?subject=Design-partner%20evaluation"

// ── Code samples ────────────────────────────────────────────────────────

const todayAgentCode = `while ticket.open:                    # every turn / every 5 min
    order  = oms.get_order(id)        # re-fetch
    eta    = carrier.get_eta(order)   # re-fetch
    policy = policies.eligibility(c)  # re-fetch
    prompt = render(order, eta, policy)  # stuff it, again
    ok     = llm.decide(prompt)       # the model re-derives it
    ...
# + one cache-invalidation handler per upstream event
# + the handler nobody wrote for the rare case
# + a cron job that recomputes everything`

const rulesCode = `// rules, written once
+late(O) <- shipment(O,S), eta(S,T), promised(O,P), T > P
+can_offer(O,C) <- late(O), customer(O,C),
                   eligible(C, "expedite")`

const agentCode = `# agent: told what changed, no re-reading
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
  const todayHtml = highlightGeneric(todayAgentCode, "python") ?? todayAgentCode
  const rulesHtml = highlightToHtml(rulesCode)
  const agentHtml = highlightGeneric(agentCode, "python") ?? agentCode

  return (
    <div className="flex flex-col min-h-dvh">
      <SiteHeader />

      <main className="flex-1">
        {/* ── Hero ───────────────────────────────────────────────────── */}
        <section className="relative overflow-hidden border-b border-border/50">
          <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent" />
          <div className="relative mx-auto max-w-6xl px-6 py-24 lg:py-32">
            <p className="font-mono text-sm font-medium uppercase tracking-wider text-primary">
              The live knowledge graph for AI agents
            </p>
            <h1 className="mt-4 text-5xl sm:text-6xl lg:text-7xl font-extrabold tracking-tight leading-[1.05] max-w-4xl">
              Models think.
              <br className="hidden sm:block" /> <span className="text-primary">InputLayer knows.</span>
            </h1>
            <p className="mt-6 text-xl sm:text-2xl font-semibold max-w-3xl">
              A fact changes. InputLayer derives what it means for your agent, without another prompt.
            </p>
            <p className="mt-4 text-lg text-muted-foreground max-w-3xl">
              InputLayer applies your rules as facts change, updating what your agent should say or do, even while other
              work continues. Keep your models and framework; connect them to current results and evidence.
            </p>
            <div className="mt-8 flex flex-wrap gap-3">
              <Link href={QUICKSTART_URL} className={primaryButton}>
                Quickstart
                <ArrowRight className="h-4 w-4" aria-hidden="true" />
              </Link>
              <a href="#how" className={secondaryButton}>
                See how it works
              </a>
            </div>
            <p className="mt-6 text-xs text-muted-foreground max-w-3xl">
              &ldquo;Knows&rdquo; means accepted facts plus rule-derived conclusions; source freshness and delivery
              still apply.
            </p>
            <p className="mt-2 text-xs text-muted-foreground">
              Self-hosted · source-available under the Elastic License 2.0 · Rust engine with Python and JS SDKs
            </p>
          </div>
        </section>

        {/* ── The stack line ─────────────────────────────────────────── */}
        <Section eyebrow="Where it sits" title="Decision models judge. Language models think. InputLayer knows.">
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
                body: "Language, planning and judgement in situations nobody wrote a rule for. They write the sentence and the plan.",
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
            Three jobs, three measures. Models interpret, rules derive, and your application acts.
          </p>
        </Section>

        {/* ── The shift ──────────────────────────────────────────────── */}
        <Section eyebrow="The shift" title="Separate knowing from thinking">
          <p className="text-muted-foreground max-w-3xl">
            Every turn, agents ask the model things the system already knows: is this order late, is this customer
            eligible, what else is affected. With InputLayer, facts and rules live outside the prompt, so the agent is
            not limited by the context window. Facts stream in, your rules derive the answers, and only the answers
            enter the prompt. The prompt becomes a view, not the store.
          </p>

          <div className="mt-8 grid gap-6 lg:grid-cols-2">
            <Card>
              <Tag tone="bad">Today: the model knows nothing until told, every turn</Tag>
              <CodeBlock html={todayHtml} label="Agent loop that re-fetches every input and re-derives the answer on every turn" />
            </Card>
            <Card>
              <Tag tone="ok">With InputLayer: the graph knows, the model thinks</Tag>
              <CodeBlock html={rulesHtml} label="Rules written once" />
              <CodeBlock html={agentHtml} label="Agent loop that is told which answers were added and withdrawn" />
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
            Deleted: the re-fetching, the prompt stuffing, the invalidation handlers, the recompute job. Added: a few
            rules and one watch.
          </p>
        </Section>

        {/* ── Why engineers adopt it ─────────────────────────────────── */}
        <Section eyebrow="Why engineers adopt it" title="Four reasons to move knowing out of the prompt">
          <div className="grid gap-6 md:grid-cols-2">
            {[
              {
                title: "Current, exact answers",
                body: "A fact changes and the affected conclusions update, including the ones that stop being true, with the facts and rules behind each.",
              },
              {
                title: "Not limited by the context window",
                body: "Facts and rules live outside the prompt; only the derived answers go in. Fewer tokens, nothing lost in the middle.",
              },
              {
                title: "A deterministic fast path",
                body: "A small intent model picks which known question was asked; the engine answers it exactly from live facts, with no generative model on that path. Open questions still go to the LLM.",
              },
              {
                title: "Fits the stack you have",
                body: "LangGraph memory, state and checkpointer; LangChain tool and retriever; an OpenAI-compatible fact-checking gateway; and change triggers your agent can wake on.",
              },
            ].map((reason, i) => (
              <Card key={reason.title} className="space-y-3">
                <p aria-hidden="true" className="font-mono text-sm font-medium text-primary">
                  {String(i + 1).padStart(2, "0")}
                </p>
                <h3 className="text-base font-semibold">{reason.title}</h3>
                <p className="text-sm text-muted-foreground">{reason.body}</p>
              </Card>
            ))}
          </div>
        </Section>

        {/* ── How it works: the fast path ────────────────────────────── */}
        <Section id="how" eyebrow="How it works" title="A fast path for what your system already knows">
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
              <Tag>Open questions take the slow path</Tag>
              <p className="text-sm text-muted-foreground">
                &ldquo;Why is it late, and what would you do?&rdquo; goes to the LLM, with the current answers already in
                its context. Only open questions go to the LLM; known ones never wait for it.
              </p>
            </Card>
          </div>
          <p className="mt-6 text-xs text-muted-foreground max-w-3xl">
            The voice agent is our flagship demo and is being built on today&apos;s standing queries. On the fast path a
            wrong answer can only come from a wrong fact or a wrong route, and both are inspectable.
          </p>
        </Section>

        {/* ── Fits your stack ────────────────────────────────────────── */}
        <Section eyebrow="Fits your stack" title="Keep your models and framework">
          <ComparisonTable
            rowHeader="Integration"
            align="left"
            columns={["What it gives your agent"]}
            rows={[
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
              {
                capability: "Change triggers",
                values: {
                  "What it gives your agent": "Standing queries that tell your agent which answers were added and withdrawn, so it wakes on change",
                },
              },
            ]}
          />
          <p className="mt-6 text-sm text-muted-foreground max-w-3xl">
            Keep your LLM, your vector store for documents and your systems of record. See the{" "}
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
            over all of it, acts, and stops. Everything that changes before the next trigger is invisible. A live
            knowledge graph closes that gap.
          </p>
        </Section>

        {/* ── Who it is for ──────────────────────────────────────────── */}
        <Section eyebrow="Who it is for" title="Built for agents that act on a world that keeps changing">
          <ComparisonTable
            rowHeader="Agent"
            align="left"
            columns={["What changes under it", "What InputLayer knows for it"]}
            rows={[
              {
                capability: "Support & customer service",
                values: {
                  "What changes under it": "Order status, ETAs, refunds, eligibility",
                  "What InputLayer knows for it": "Which promises still hold while the ticket is open",
                },
              },
              {
                capability: "Fulfilment & logistics",
                values: {
                  "What changes under it": "Carrier events, stock, capacity",
                  "What InputLayer knows for it": "Which shipments slipped and what they affect",
                },
              },
              {
                capability: "Risk, fraud & compliance",
                values: {
                  "What changes under it": "Transactions, sanctions, approvals",
                  "What InputLayer knows for it": "Which flags hold now, and which were withdrawn",
                },
              },
              {
                capability: "Operations & monitoring",
                values: {
                  "What changes under it": "Metrics, incidents, dependencies",
                  "What InputLayer knows for it": "Root cause and impact, updated per event, not per sweep",
                },
              },
              {
                capability: "Voice & live assistants",
                values: {
                  "What changes under it": "What the user just said, what the world just did",
                  "What InputLayer knows for it": "The current answer, so the agent can correct itself mid-sentence",
                },
              },
            ]}
          />
          <p className="mt-6 text-sm text-muted-foreground max-w-3xl">
            A good fit when the agent acts on structured facts that change, the answer is derived through a chain of facts
            or rules, a stale answer has a real cost, and the agent lives long enough for the world to change under it.
            Document chat and one-shot Q&amp;A are better served by retrieval alone.
          </p>
        </Section>

        {/* ── Compared with what you'd build yourself ────────────────── */}
        <Section eyebrow="Compared with what you'd build yourself" title="What each option knows when a fact changes">
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
        <Section id="start" eyebrow="Get started" title="Give your agent a live knowledge graph">
          <div className="grid gap-6 lg:grid-cols-2">
            <Card className="space-y-3">
              <h3 className="text-base font-semibold">Run it yourself</h3>
              <p className="text-sm text-muted-foreground">
                Install the engine, load a sample, and watch conclusions change as facts do. Free to self-host.
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
                Bring one agent. In four weeks we move what it knows into InputLayer on your data and measure the
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
