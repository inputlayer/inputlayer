"use client"

import Link from "next/link"
import { useState } from "react"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"
import { EmbeddingDiagram, DiamondDiagram, WaterfallDiagram, ProvenanceTreeDiagram, VisualCodeTabs, HeroVisualization } from "@/components/landing-diagrams"
import { RotatingHero } from "@/components/rotating-hero"
import { ComparisonTable } from "@/components/comparison-table"
import {
  ArrowRight,
  ExternalLink,
  Zap,
  Shield,
  Brain,
  GitBranch,
  Copy,
  Check,
  Terminal,
  BookOpen,
  Server,
  FileText,
  Play,
} from "lucide-react"

const DEMO_BASE_URL = "https://demo.inputlayer.ai"

// ── Syntax-highlighted code blocks ──────────────────────────────────────

const rulesVectorsCode = `// Multi-hop: payment -> account standing -> eligibility
+paid_invoice[("cust_1", "inv_99")]
+dispute_resolved[("cust_1", "d_42")]
+product[("pro_a", "Widget Pro", [0.82, 0.44]),
         ("pro_b", "Widget Lite", [0.79, 0.41])]

// Hop 1: account in good standing if paid AND disputes resolved
+good_standing(C) <- paid_invoice(C, _), dispute_resolved(C, _)

// Hop 2: eligible to buy if in good standing
+eligible(C, P) <- good_standing(C), product(P, _, _)

// Query: rules derive eligibility, vectors rank by relevance
?eligible("cust_1", Pid),
 product(Pid, Name, Emb),
 Dist = cosine(Emb, [0.80, 0.43]),
 Dist < 0.1
// -> pro_a  "Widget Pro"   Dist: 0.001
// -> pro_b  "Widget Lite"  Dist: 0.003
// 3 hops of reasoning, then vector ranking - one query`

const retractionCode = `// Two reasons to block a customer
+unpaid_bill[("customer_42")]
+unverified_card[("customer_42")]

// Either reason blocks purchasing
+blocked(C) <- unpaid_bill(C)
+blocked(C) <- unverified_card(C)

?blocked("customer_42")
// -> "customer_42"         (blocked)

// Customer pays the bill:
-unpaid_bill("customer_42")
?blocked("customer_42")
// -> "customer_42"         (still blocked - card unverified)

// Customer verifies their card:
-unverified_card("customer_42")
?blocked("customer_42")
// -> No results.           (correctly unblocked)`

const incrementalCode = `+manages[("alice", "bob"), ("bob", "charlie")]

+authority(X, Y) <- manages(X, Y)
+authority(X, Z) <- authority(X, Y), manages(Y, Z)

?authority("alice", Who)
// -> "alice" | "bob"
// -> "alice" | "charlie"    (2 rows)

// Add one new edge:
+manages("charlie", "diana")

?authority("alice", Who)
// -> "alice" | "bob"
// -> "alice" | "charlie"
// -> "alice" | "diana"      (3 rows, only diana recomputed)`

const provenanceCode = `// Agent decided to purchase from Acme Supplies
+budget_remaining[("team_a", 5000)]
+approved_vendor[("acme_supplies")]
+order[("team_a", "acme_supplies", 3200)]

+purchase_ok(T, V, Amt) <- order(T, V, Amt),
  approved_vendor(V), budget_remaining(T, B), Amt <= B

// Why was this purchase approved?
.why ?purchase_ok("team_a", "acme_supplies", 3200)
// [rule] purchase_ok (clause 0)
//   bindings: T="team_a", V="acme_supplies", Amt=3200
//   [base] order("team_a", "acme_supplies", 3200)
//   [base] approved_vendor("acme_supplies")
//   [base] budget_remaining("team_a", 5000)
//   [eval] 3200 <= 5000 -> true

// Why wasn't Globex Corp approved?
.why_not purchase_ok("team_a", "globex_corp", 1500)
// Rule: purchase_ok (clause 0)
//   Blocker: approved_vendor("globex_corp") - No matching tuples`

const dockerCommand = "docker run -p 8080:8080 ghcr.io/inputlayer/inputlayer"

// ── Helper components ───────────────────────────────────────────────────

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      }}
      className="absolute top-3 right-3 inline-flex items-center gap-1.5 rounded-md border border-border bg-background/80 px-2.5 py-1.5 text-xs text-muted-foreground backdrop-blur transition-colors hover:text-foreground hover:bg-background"
    >
      {copied ? (
        <>
          <Check className="h-3.5 w-3.5 text-emerald-500" />
          Copied
        </>
      ) : (
        <>
          <Copy className="h-3.5 w-3.5" />
          Copy
        </>
      )}
    </button>
  )
}

// ── Page ─────────────────────────────────────────────────────────────────

export default function LandingPage() {
  return (
    <div className="flex flex-col min-h-dvh">
      <SiteHeader />

      {/* ── Hero ───────────────────────────────────────────────────── */}
      <section className="relative overflow-hidden border-b border-border/50">
        <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent" />
        <div className="relative mx-auto max-w-6xl px-6 py-24 lg:py-32">
          <div className="grid gap-12 lg:grid-cols-2 lg:gap-16 items-center">
            <div className="space-y-8">
              <RotatingHero />
              <div className="flex flex-wrap gap-3 pt-2">
                <a
                  href={`${DEMO_BASE_URL}/demo/request-access?kg=flights`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-2 rounded-md bg-primary px-5 py-2.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
                >
                  Try the demo
                  <ArrowRight className="h-4 w-4" />
                </a>
                <Link
                  href="/docs/"
                  className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-5 py-2.5 text-sm font-medium hover:bg-secondary transition-colors"
                >
                  Read the docs
                  <ArrowRight className="h-3.5 w-3.5" />
                </Link>
              </div>
            </div>
            <div className="space-y-4">
              <HeroVisualization />
              <div className="flex justify-center">
                <a
                  href={`${DEMO_BASE_URL}/demo/request-access?kg=flights`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-2 text-sm text-primary font-medium hover:underline"
                >
                  <Play className="h-3.5 w-3.5" />
                  Open in Studio
                </a>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ── Rules + Vectors ────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="grid gap-12 lg:grid-cols-2 items-start">
            <div className="space-y-6">
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Rules + vector search</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Vector search was never built for logic.
              </h2>
              <p className="text-muted-foreground">
                Example: A customer asks "what can I buy?" Simple filtering won't cut it - their eligibility depends on account status, which depends on payment history, which depends on dispute resolutions. That's three hops of reasoning before you even get to ranking products. A vector store can filter on metadata it already has. It can't derive new facts from chains of rules.
              </p>
              <p className="text-sm font-semibold text-primary uppercase tracking-wider pt-2">Best of both worlds</p>
              <p className="text-muted-foreground">
                InputLayer evaluates rules and ranks by vector similarity, all in a <strong>single query</strong>. Rules derive conclusions through multi-hop reasoning - like whether a customer is eligible based on a chain of conditions. Vector search ranks what's left by relevance. It's the best of both worlds, vector search powered with reasoning.
              </p>
              <a
                href={`${DEMO_BASE_URL}/demo/request-access?kg=rules_vectors`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 text-sm text-primary font-medium hover:underline pt-2"
              >
                <Play className="h-3.5 w-3.5" />
                Open in Studio
              </a>
            </div>
            <VisualCodeTabs visual={<EmbeddingDiagram />} code={rulesVectorsCode} />
          </div>
        </div>
      </section>

      {/* ── Correct Retraction ─────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="grid gap-12 lg:grid-cols-2 items-start">
            <div className="space-y-6">
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Correct retraction</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Multiple reasons, one conclusion. Remove one - does the conclusion hold?
              </h2>
              <p className="text-muted-foreground">
                Your customer didn't pay a bill and gets temporarily blocked from buying anything. Once that bill is paid, they should be unblocked - an easy problem to solve. But what if there are multiple facts that concluded the ban? Imagine, they also failed to confirm their credit card information. Multiple retraction paths, a harder problem to solve.
              </p>
              <p className="text-muted-foreground">
                With InputLayer, conclusions only retract when every path is cleared. The customer stays blocked until the bill is paid and the card is verified. No premature unblocking, no manual checks.
              </p>
              <a
                href={`${DEMO_BASE_URL}/demo/request-access?kg=retraction`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 text-sm text-primary font-medium hover:underline pt-2"
              >
                <Play className="h-3.5 w-3.5" />
                Open in Studio
              </a>
            </div>
            <VisualCodeTabs visual={<DiamondDiagram />} code={retractionCode} />
          </div>
        </div>
      </section>

      {/* ── Incremental Updates ─────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="grid gap-12 lg:grid-cols-2 items-start">
            <div className="space-y-6">
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Incremental updates</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Computing conclusions in record time and at any scale.
              </h2>
              <p className="text-muted-foreground">
                Imagine a 2,000-node dependency graph, common for business applications. If one fact changes (a record expires, a supplier is suspended, ownership changes etc.) every conclusion built on that fact needs to be updated. Without InputLayer, recomputing everything takes 11.3 seconds. InputLayer traces the impact forward and updates only what's affected, making it record-fast at any scale.
              </p>
              <div className="flex gap-8 pt-2">
                <div>
                  <span className="text-4xl font-extrabold text-primary">6.83ms</span>
                  <p className="text-xs text-muted-foreground mt-1">incremental update</p>
                </div>
                <div>
                  <span className="text-4xl font-extrabold text-muted-foreground/30">11.3s</span>
                  <p className="text-xs text-muted-foreground mt-1">full recompute</p>
                </div>
                <div>
                  <span className="text-4xl font-extrabold text-primary">1,652x</span>
                  <p className="text-xs text-muted-foreground mt-1">faster</p>
                </div>
              </div>
              <a
                href={`${DEMO_BASE_URL}/demo/request-access?kg=incremental`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 text-sm text-primary font-medium hover:underline pt-2"
              >
                <Play className="h-3.5 w-3.5" />
                Open in Studio
              </a>
            </div>
            <VisualCodeTabs visual={<WaterfallDiagram />} code={incrementalCode} />
          </div>
        </div>
      </section>

      {/* ── Provenance ─────────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="grid gap-12 lg:grid-cols-2 items-start">
            <div className="space-y-6">
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Provenance</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Always know "why" a decision was made by AI
              </h2>
              <p className="text-muted-foreground">
                Your AI agent decides to purchase supplies and makes a mistake. Understanding the "why" becomes crucial for fixing the issue. A simple "the model predicted this decision" is not auditable, actionable or trustworthy. With InputLayer you can simply run <code className="text-xs bg-muted/50 px-1.5 py-0.5 rounded">.why</code> and get a structured proof tree: facts, rules, and the chain of reasoning that produced the decision. Or run <code className="text-xs bg-muted/50 px-1.5 py-0.5 rounded">.why_not</code> to see exactly which condition blocked something.
              </p>
              <div className="flex justify-center gap-8 pt-2">
                <div className="text-center">
                  <span className="text-5xl font-extrabold text-primary">100%</span>
                  <p className="text-xs text-muted-foreground mt-1">of results fully traceable</p>
                </div>
                <div className="text-center">
                  <span className="text-5xl font-extrabold text-primary">100%</span>
                  <p className="text-xs text-muted-foreground mt-1">of results fully verifiable</p>
                </div>
              </div>
              <a
                href={`${DEMO_BASE_URL}/demo/request-access?kg=provenance`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 text-sm text-primary font-medium hover:underline pt-2"
              >
                <Play className="h-3.5 w-3.5" />
                Open in Studio
              </a>
            </div>
            <VisualCodeTabs visual={<ProvenanceTreeDiagram />} code={provenanceCode} />
          </div>
        </div>
      </section>

      {/* ── Who Builds With InputLayer ─────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">The reasoning layer for AI agents</p>
            <h2 className="text-3xl font-bold tracking-tight">
              Derive what's true from what's stored, before the LLM ever sees it
            </h2>
            <p className="text-muted-foreground mt-4">
              InputLayer is the reasoning layer for AI agents. It derives conclusions from facts and rules so your agent works with what's actually true - not what a model approximates.
            </p>
          </div>

          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
            {[
              { icon: <Brain className="h-6 w-6 text-primary" />, title: "Agent memory that reasons", desc: "Your agent stores facts. InputLayer derives what follows - before the LLM prompt is built." },
              { icon: <Zap className="h-6 w-6 text-primary" />, title: "Beyond vector retrieval", desc: "Similarity search finds context. Rules filter to what's actually true. One query, both." },
              { icon: <Shield className="h-6 w-6 text-primary" />, title: "Auditable decisions", desc: "Every conclusion traces back to the facts and rules that produced it. Ask .why on any result." },
              { icon: <GitBranch className="h-6 w-6 text-primary" />, title: "Memory that stays current", desc: "One fact changes, derived conclusions update in milliseconds. No stale context, no recomputation." },
            ].map((persona) => (
              <div
                key={persona.title}
                className="rounded-xl border border-border bg-card p-6 space-y-3"
              >
                {persona.icon}
                <h3 className="text-sm font-semibold">{persona.title}</h3>
                <p className="text-sm text-muted-foreground">{persona.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Comparison ──────────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="space-y-6 mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider">Comparison</p>
            <h2 className="text-3xl font-bold tracking-tight">
              The reasoning layer your stack is missing
            </h2>
            <p className="text-muted-foreground max-w-2xl">
              InputLayer is not a replacement for your data stack or your AI platform. It is the streaming reasoning layer that sits between them - filling the gap that neither vector search nor graph traversal can cover alone.
            </p>
          </div>

          <ComparisonTable
            columns={["Vector DBs", "Graph DBs", "SQL", "InputLayer"]}
            highlightColumn="InputLayer"
            rows={[
              { capability: "Vector similarity", values: { "Vector DBs": "native", "Graph DBs": "plugin", "SQL": "none", "InputLayer": "native" } },
              { capability: "Graph traversal", values: { "Vector DBs": "none", "Graph DBs": "native", "SQL": "partial", "InputLayer": "native" } },
              { capability: "Rule-based inference", values: { "Vector DBs": "none", "Graph DBs": "none", "SQL": "none", "InputLayer": "native" } },
              { capability: "Recursive reasoning", values: { "Vector DBs": "none", "Graph DBs": "partial", "SQL": "partial", "InputLayer": "native" } },
              { capability: "Incremental updates", values: { "Vector DBs": "none", "Graph DBs": "none", "SQL": "partial", "InputLayer": "native" } },
              { capability: "Correct retraction", values: { "Vector DBs": "none", "Graph DBs": "none", "SQL": "none", "InputLayer": "native" } },
              { capability: "Explainable retrieval", values: { "Vector DBs": "none", "Graph DBs": "partial", "SQL": "none", "InputLayer": "native" } },
            ]}
          />
        </div>
      </section>

      {/* ── Deep Dives ─────────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="flex items-center justify-between mb-12">
            <div>
              <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">Go deeper</p>
              <h2 className="text-3xl font-bold tracking-tight">The problems behind the features</h2>
            </div>
            <Link
              href="/blog/"
              className="hidden sm:inline-flex items-center gap-1 text-sm text-primary font-medium hover:underline"
            >
              All posts <ArrowRight className="h-3.5 w-3.5" />
            </Link>
          </div>

          <div className="grid gap-6 md:grid-cols-2">
            {[
              {
                slug: "why-vector-search-alone-fails",
                context: "Extends: Rules + vector search",
                title: "Why Vector Search Alone Fails Your AI Agent",
                desc: "Similarity scores can't encode business rules. This post walks through the ink cartridge problem and shows how rules and vectors work together in a single query.",
              },
              {
                slug: "correct-retraction-why-delete-should-actually-delete",
                context: "Extends: Correct conclusion retraction",
                title: "Correct Retraction: Why Delete Should Actually Delete",
                desc: "When an entity is cleared from a sanctions list, which flags should retract? The diamond problem is subtle, and most systems get it wrong.",
              },
            ].map((post) => (
              <Link
                key={post.slug}
                href={`/blog/${post.slug}/`}
                className="group rounded-xl border border-border bg-card p-8 space-y-3 transition-colors hover:border-primary/30 hover:bg-card/80"
              >
                <span className="text-[10px] font-semibold text-primary uppercase tracking-wider">{post.context}</span>
                <h3 className="text-xl font-semibold group-hover:text-primary transition-colors">{post.title}</h3>
                <p className="text-sm text-muted-foreground leading-relaxed">{post.desc}</p>
                <span className="inline-flex items-center gap-1 text-sm text-primary font-medium pt-1">
                  Read the deep dive <ArrowRight className="h-3.5 w-3.5" />
                </span>
              </Link>
            ))}
          </div>

          <div className="mt-8 text-center sm:hidden">
            <Link
              href="/blog/"
              className="inline-flex items-center gap-1 text-sm text-primary font-medium hover:underline"
            >
              All posts <ArrowRight className="h-3.5 w-3.5" />
            </Link>
          </div>
        </div>
      </section>

      {/* ── Get Started ───────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="relative rounded-2xl border border-border bg-gradient-to-br from-primary/10 via-transparent to-primary/5 p-12 space-y-10">
            <div className="text-center space-y-3">
              <h2 className="text-3xl font-bold tracking-tight">
                Open source. Run it yourself.
              </h2>
              <p className="text-muted-foreground text-lg max-w-xl mx-auto">
                No account, no API key, no vendor lock-in. From first query to production in four steps.
              </p>
            </div>

            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              {[
                {
                  step: "1",
                  icon: <Terminal className="h-5 w-5 text-primary" />,
                  title: "Try it in 30 seconds",
                  desc: "One Docker command, instant local instance.",
                  href: "https://demo.inputlayer.ai",
                  external: true,
                  label: "Launch demo",
                },
                {
                  step: "2",
                  icon: <BookOpen className="h-5 w-5 text-primary" />,
                  title: "Learn the syntax",
                  desc: "Datalog rules, facts, queries - the full language reference.",
                  href: "/docs/",
                  external: false,
                  label: "Read the docs",
                },
                {
                  step: "3",
                  icon: <Server className="h-5 w-5 text-primary" />,
                  title: "Deploy with your stack",
                  desc: "Self-hosted Docker or Kubernetes. Your infra, your data.",
                  href: "/docs/guides/configuration/",
                  external: false,
                  label: "Deployment guide",
                },
                {
                  step: "4",
                  icon: <FileText className="h-5 w-5 text-primary" />,
                  title: "Go to production",
                  desc: "Apache 2.0 + Commons Clause. Commercial license when you need it.",
                  href: "/commercial/",
                  external: false,
                  label: "View license",
                },
              ].map((s) => (
                <div key={s.step} className="relative rounded-xl border border-border bg-background/50 p-5 space-y-3">
                  <div className="flex items-center gap-3">
                    <span className="flex items-center justify-center w-6 h-6 rounded-full bg-primary/10 text-xs font-bold text-primary">{s.step}</span>
                    {s.icon}
                  </div>
                  <h3 className="text-sm font-semibold">{s.title}</h3>
                  <p className="text-xs text-muted-foreground leading-relaxed">{s.desc}</p>
                  {s.external ? (
                    <a href={s.href} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-xs text-primary font-medium hover:underline">
                      {s.label} <ExternalLink className="h-3 w-3" />
                    </a>
                  ) : (
                    <Link href={s.href} className="inline-flex items-center gap-1 text-xs text-primary font-medium hover:underline">
                      {s.label} <ArrowRight className="h-3 w-3" />
                    </Link>
                  )}
                </div>
              ))}
            </div>

            <div className="mx-auto max-w-lg">
              <div className="relative">
                <pre className="rounded-lg bg-[var(--code-bg)] py-4 px-12 overflow-x-auto text-sm font-mono text-center">
                  <code>
                    <span className="syn-builtin">docker</span> run -p 8080:8080 ghcr.io/inputlayer/inputlayer
                  </code>
                </pre>
                <CopyButton text={dockerCommand} />
              </div>
            </div>
          </div>
        </div>
      </section>

      <SiteFooter />
    </div>
  )
}
