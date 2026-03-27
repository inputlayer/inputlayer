"use client"

import Link from "next/link"
import { useState } from "react"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"
import { BlogCard } from "@/components/blog-card"
import { highlightToHtml } from "@/lib/syntax-highlight"
import { blogPosts } from "@/lib/content-bundle"
import {
  ArrowRight,
  ExternalLink,
  Zap,
  Shield,
  Brain,
  Factory,
  Truck,
  GitBranch,
  ShoppingBag,
  CheckCircle,
  XCircle,
  Minus,
  Copy,
  Check,
  Star,
} from "lucide-react"

// ── Syntax-highlighted code blocks ──────────────────────────────────────

const heroCode = `// Facts: current supplier status
+supplier("sup_01", "status", "active")
+supplier("sup_02", "status", "suspended")

// Rule: order is blocked if any supplier is suspended
+order_blocked(Order, Sup, "suspended") <-
    required_supplier(Order, Sup),
    supplier(Sup, "status", "suspended")

// Query: why can't order 2847 ship?
?order_blocked("order_2847", Supplier, Reason)`

const policySearchCode = `// Compatibility rules + vector search in one pass
?recommendable("shopper_42", ProductId),
 product(ProductId, Desc, Price, Embedding),
 Similarity = cosine(Embedding, QueryVec),
 Similarity > 0.6`

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

function CodeBlock({ code, className }: { code: string; className?: string }) {
  const html = highlightToHtml(code)
  return (
    <pre className={`rounded-lg bg-[var(--code-bg)] p-4 overflow-x-auto text-sm font-mono ${className ?? ""}`}>
      <code dangerouslySetInnerHTML={{ __html: html }} />
    </pre>
  )
}

function FeatureBadge({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex items-center rounded-full border border-border bg-secondary/50 px-3 py-1 text-sm text-secondary-foreground">
      {children}
    </span>
  )
}

function ComparisonIcon({ value }: { value: "native" | "plugin" | "partial" | "none" }) {
  switch (value) {
    case "native":
      return <CheckCircle className="h-4 w-4 text-emerald-500" />
    case "plugin":
      return <CheckCircle className="h-4 w-4 text-yellow-500" />
    case "partial":
      return <Minus className="h-4 w-4 text-yellow-500" />
    case "none":
      return <XCircle className="h-4 w-4 text-muted-foreground/40" />
  }
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
            <div className="space-y-6">
              <h1 className="text-4xl font-extrabold tracking-tight sm:text-5xl lg:text-6xl">
                Streaming reasoning layer
                <br />
                <span className="text-primary">for AI systems</span>
              </h1>
              <p className="text-lg text-muted-foreground max-w-lg">
                Incremental rules engine with vector search, graph traversal, and explainable derivation traces. Sits between your data and your AI - keeping context live, correct, and auditable as facts change.
              </p>
              <div className="flex flex-wrap gap-3 pt-2">
                <Link
                  href="/docs/"
                  className="inline-flex items-center gap-2 rounded-md bg-primary px-5 py-2.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
                >
                  Read the docs
                  <ArrowRight className="h-4 w-4" />
                </Link>
                <a
                  href="https://github.com/inputlayer/inputlayer"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-5 py-2.5 text-sm font-medium hover:bg-secondary transition-colors"
                >
                  <Star className="h-4 w-4" />
                  Star on GitHub
                </a>
                <Link
                  href="/use-cases/"
                  className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-5 py-2.5 text-sm font-medium hover:bg-secondary transition-colors"
                >
                  See use cases
                  <ArrowRight className="h-3.5 w-3.5" />
                </Link>
              </div>
            </div>
            <div>
              <CodeBlock code={heroCode} />
            </div>
          </div>
        </div>
      </section>

      {/* ── Problem ────────────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">The problem</p>
            <h2 className="text-3xl font-bold tracking-tight mb-4">
              Vector search <span className="underline">only</span> finds things that look like the answer
            </h2>
            <p className="text-muted-foreground text-lg">
              The standard RAG pipeline retrieves documents by similarity. That fails when the answer is connected through a chain of facts - not surface-level similarity.
            </p>
          </div>

          <div className="grid gap-6 md:grid-cols-3">
            <div className="rounded-xl border border-border bg-card p-6 space-y-3">
              <div className="flex items-center gap-2 text-destructive">
                <Factory className="h-5 w-5" />
                <span className="font-semibold">Manufacturing Operations</span>
              </div>
              <p className="text-sm text-muted-foreground">
                A production planning agent asks: <em>&ldquo;Can Line 4 run the night shift?&rdquo;</em> The answer depends on active equipment holds, maintenance schedules, parts availability, and which operators are certified for the current job spec.
              </p>
              <p className="text-xs text-muted-foreground/70">
                The connection - job spec &rarr; required parts &rarr; parts on hold &rarr; hold reason &rarr; expected release date - does not exist in embedding space. It exists as a chain of operational facts.
              </p>
            </div>

            <div className="rounded-xl border border-border bg-card p-6 space-y-3">
              <div className="flex items-center gap-2 text-warning-foreground">
                <Truck className="h-5 w-5" />
                <span className="font-semibold">Supply Chain</span>
              </div>
              <p className="text-sm text-muted-foreground">
                An AI system is asked: <em>&ldquo;Can order #2847 ship by Friday?&rdquo;</em> The answer requires knowing current supplier status, active suspensions, lead times, and whether any required supplier is under sanctions review.
              </p>
              <p className="text-xs text-muted-foreground/70">
                None of those facts are semantically similar to &ldquo;can this order ship by Friday.&rdquo; They are connected through operational relationships. A rules engine follows them. Vector search cannot.
              </p>
            </div>

            <div className="rounded-xl border border-border bg-card p-6 space-y-3">
              <div className="flex items-center gap-2 text-accent">
                <GitBranch className="h-5 w-5" />
                <span className="font-semibold">Financial Risk</span>
              </div>
              <p className="text-sm text-muted-foreground">
                Compliance asks: <em>&ldquo;Is this transaction suspicious?&rdquo;</em> Entity A paid Entity B. B is a subsidiary of C. C is on a sanctions list.
              </p>
              <p className="text-xs text-muted-foreground/70">
                Pattern matching finds similar transactions. It does not traverse the ownership graph to reach the sanctions hit three hops away. Graph traversal plus rule evaluation does.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* ── Solution ───────────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">The solution</p>
            <h2 className="text-3xl font-bold tracking-tight mb-4">
              Reasoning, not just similarity
            </h2>
          </div>

          <div className="grid gap-12 lg:grid-cols-2 items-start">
            {/* Left: benefits */}
            <div className="space-y-6">
              <p className="text-muted-foreground text-lg">
                InputLayer adds an incremental rules engine to your AI stack. You define the relationships and policies that matter - compatibility rules, ownership chains, operational constraints. The engine derives what follows and keeps those derivations live as your data changes.
              </p>
              <p className="text-muted-foreground">
                Every result is traceable. The Provenance API returns a complete derivation proof per result - the exact chain of rules and facts that produced it. When a regulator, auditor, or downstream system asks why the AI made that decision, the answer is a structured artifact, not a log.
              </p>
              <p className="text-muted-foreground">
                This is not a replacement for your orchestration platform, your data warehouse, or your vector database. It is the reasoning layer that makes them work for decisions that require following consequence, not matching surface similarity.
              </p>
            </div>

            {/* Right: example */}
            <div className="space-y-3">
              <p className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">Example</p>
              <CodeBlock code={policySearchCode} />
              <p className="text-sm text-muted-foreground">
                This query evaluates compatibility rules against purchase history, joins with live inventory and product embeddings, and ranks results by semantic similarity - all in a single pass. No glue code, no separate round trips.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* ── How it works ───────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">Under the hood</p>
            <h2 className="text-3xl font-bold tracking-tight mb-4">
              How it works
            </h2>
            <p className="text-muted-foreground text-lg">
              InputLayer is built on an incremental computation engine. This gives it three properties that matter for production use.
            </p>
          </div>

          <div className="grid gap-6 md:grid-cols-3">
            <div className="rounded-xl border border-border bg-card p-6 space-y-4">
              <Zap className="h-8 w-8 text-primary" />
              <h3 className="text-lg font-semibold">Incremental maintenance</h3>
              <p className="text-sm text-muted-foreground">
                When a fact changes, only the affected derivations recompute. Insert one new edge into a 2,000-node graph: 6.83ms to re-derive transitive closure. Full recompute: 11.3 seconds. Production AI systems cannot wait for full recomputes.
              </p>
              <div className="text-center pt-2">
                <span className="text-5xl font-extrabold text-primary">1,652x</span>
                <p className="text-xs text-muted-foreground mt-1">faster than full recompute</p>
              </div>
            </div>

            <div className="rounded-xl border border-border bg-card p-6 space-y-4">
              <Brain className="h-8 w-8 text-primary" />
              <h3 className="text-lg font-semibold">Explainable results</h3>
              <p className="text-sm text-muted-foreground">
                Every derived fact traces back to the rules and base facts that produced it. Not &ldquo;the vector was close&rdquo; - a complete derivation chain exposed via the Provenance API. Auditable, storable, and regulatorily defensible.
              </p>
              <div className="text-center pt-2">
                <span className="text-5xl font-extrabold text-primary">100%</span>
                <p className="text-xs text-muted-foreground mt-1">of results fully traceable</p>
              </div>
            </div>

            <div className="rounded-xl border border-border bg-card p-6 space-y-4">
              <Shield className="h-8 w-8 text-primary" />
              <h3 className="text-lg font-semibold">Correct retraction</h3>
              <p className="text-sm text-muted-foreground">
                Delete a fact and every conclusion that depended on it disappears automatically - but only if no other derivation path still supports it. No phantom flags. No stale recommendations. No manual cache invalidation.
              </p>
              <div className="text-center pt-2">
                <span className="text-5xl font-extrabold text-primary">0</span>
                <p className="text-xs text-muted-foreground mt-1">stale results</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ── Comparison Table ───────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">Comparison</p>
            <h2 className="text-3xl font-bold tracking-tight mb-4">
              The reasoning layer your stack is missing
            </h2>
            <p className="text-muted-foreground text-lg">
              InputLayer is not a replacement for your data stack or your AI platform. It is the streaming reasoning layer that sits between them - filling the gap that neither vector search nor graph traversal can cover alone.
            </p>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 font-semibold">Capability</th>
                  <th className="text-center py-3 px-4 font-semibold text-muted-foreground">Vector DBs</th>
                  <th className="text-center py-3 px-4 font-semibold text-muted-foreground">Graph DBs</th>
                  <th className="text-center py-3 px-4 font-semibold text-muted-foreground">SQL</th>
                  <th className="text-center py-3 px-4 font-semibold text-primary">InputLayer</th>
                </tr>
              </thead>
              <tbody>
                {[
                  { cap: "Vector similarity", vec: "native", graph: "plugin", sql: "none", il: "native" },
                  { cap: "Graph traversal", vec: "none", graph: "native", sql: "partial", il: "native" },
                  { cap: "Rule-based inference", vec: "none", graph: "none", sql: "none", il: "native" },
                  { cap: "Recursive reasoning", vec: "none", graph: "partial", sql: "partial", il: "native" },
                  { cap: "Incremental updates", vec: "none", graph: "none", sql: "partial", il: "native" },
                  { cap: "Correct retraction", vec: "none", graph: "none", sql: "none", il: "native" },
                  { cap: "Explainable retrieval", vec: "none", graph: "partial", sql: "none", il: "native" },
                ].map((row) => (
                  <tr key={row.cap} className="border-b border-border/50">
                    <td className="py-3 px-4">{row.cap}</td>
                    <td className="py-3 px-4 text-center"><span className="inline-flex justify-center w-full"><ComparisonIcon value={row.vec as "native" | "plugin" | "partial" | "none"} /></span></td>
                    <td className="py-3 px-4 text-center"><span className="inline-flex justify-center w-full"><ComparisonIcon value={row.graph as "native" | "plugin" | "partial" | "none"} /></span></td>
                    <td className="py-3 px-4 text-center"><span className="inline-flex justify-center w-full"><ComparisonIcon value={row.sql as "native" | "plugin" | "partial" | "none"} /></span></td>
                    <td className="py-3 px-4 text-center"><span className="inline-flex justify-center w-full"><ComparisonIcon value={row.il as "native" | "plugin" | "partial" | "none"} /></span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* ── Use Cases ─────────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">Use cases</p>
            <h2 className="text-3xl font-bold tracking-tight mb-4">
              Built for reasoning-intensive applications
            </h2>
            <p className="text-muted-foreground text-lg">
              From manufacturing operations to financial compliance, InputLayer powers applications where the answer requires following chains of facts - not just matching surface similarity.
            </p>
          </div>

          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
            {[
              { title: "Manufacturing Operations", icon: <Factory className="h-8 w-8 text-primary" />, desc: "Live reasoning over equipment holds, job specs, parts availability, and operator certifications. When a hold is lifted, every dependent production plan updates in milliseconds.", href: "/use-cases/manufacturing/" },
              { title: "Supply Chain", icon: <Truck className="h-8 w-8 text-primary" />, desc: "Supplier status, sanctions exposure, and order fulfillment reasoning over live entity graphs. One supplier status change propagates through every affected order automatically.", href: "/use-cases/supply-chain/" },
              { title: "Financial Risk and Compliance", icon: <Shield className="h-8 w-8 text-primary" />, desc: "Beneficial ownership traversal, sanctions screening, and policy enforcement through entity relationship chains. Auditable derivation proof for every flag.", href: "/use-cases/financial-risk/" },
              { title: "Conversational Commerce", icon: <ShoppingBag className="h-8 w-8 text-primary" />, desc: "Compatible product recommendations from purchase history and live inventory, in one query. No glue code. No stale results.", href: "/use-cases/commerce/" },
            ].map((uc) => (
              <Link
                key={uc.title}
                href={uc.href}
                className="group rounded-xl border border-border bg-card p-6 space-y-4 transition-colors hover:border-primary/30 hover:bg-card/80"
              >
                {uc.icon}
                <h3 className="text-lg font-semibold group-hover:text-primary transition-colors">{uc.title}</h3>
                <p className="text-sm text-muted-foreground">{uc.desc}</p>
                <span className="inline-flex items-center gap-1 text-sm text-primary font-medium">
                  Learn more <ArrowRight className="h-3.5 w-3.5" />
                </span>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* ── Features ───────────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">Features</p>
            <h2 className="text-3xl font-bold tracking-tight mb-4">
              Everything in the box
            </h2>
          </div>

          <div className="flex flex-wrap gap-3">
            {[
              "55 built-in functions",
              "HNSW vector indexes",
              "Cosine, Euclidean, Dot Product, Manhattan",
              "Recursive queries",
              "Magic Sets optimization",
              "Incremental computation engine",
              "Persistent storage (Parquet + WAL)",
              "Multi-tenancy",
              "WebSocket API",
              "Streaming transport",
              "Python SDK",
              "Object-logic mapper",
              "REST API",
              "Interactive REPL",
              "Session rules",
              "Conditional deletion",
              "Schema validation",
              "Aggregations",
              "Temporal functions",
              "String functions",
              "Math functions",
              "Vector operations",
              "LSH bucketing",
            ].map((f) => (
              <FeatureBadge key={f}>{f}</FeatureBadge>
            ))}
          </div>
        </div>
      </section>

      {/* ── Blog Preview ────────────────────────────────────────────── */}
      {blogPosts.length > 0 && (
        <section className="border-b border-border/50">
          <div className="mx-auto max-w-6xl px-6 py-20">
            <div className="flex items-center justify-between mb-12">
              <div>
                <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">From the blog</p>
                <h2 className="text-3xl font-bold tracking-tight">Latest posts</h2>
              </div>
              <Link
                href="/blog/"
                className="hidden sm:inline-flex items-center gap-1 text-sm text-primary font-medium hover:underline"
              >
                View all <ArrowRight className="h-3.5 w-3.5" />
              </Link>
            </div>

            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              {blogPosts.slice(0, 3).map((post) => (
                <BlogCard
                  key={post.slug}
                  slug={post.slug}
                  title={post.title}
                  date={post.date}
                  author={post.author}
                  excerpt={post.excerpt}
                  category={post.category}
                />
              ))}
            </div>

            <div className="mt-8 text-center sm:hidden">
              <Link
                href="/blog/"
                className="inline-flex items-center gap-1 text-sm text-primary font-medium hover:underline"
              >
                View all posts <ArrowRight className="h-3.5 w-3.5" />
              </Link>
            </div>
          </div>
        </section>
      )}

      {/* ── Getting Started ────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mx-auto text-center space-y-6">
            <h2 className="text-3xl font-bold tracking-tight">Get started</h2>
            <p className="text-muted-foreground text-lg">
              Pull the Docker image and start querying in seconds. The query language is intuitive - if you know SQL, the basics take about 10 minutes.
            </p>

            <div className="text-left mx-auto max-w-2xl">
              <div className="relative">
                <pre className="rounded-lg bg-[var(--code-bg)] py-4 px-24 overflow-x-auto text-sm font-mono text-center">
                  <code>
                    <span className="syn-builtin">docker</span> run -p 8080:8080 ghcr.io/inputlayer/inputlayer
                  </code>
                </pre>
                <CopyButton text={dockerCommand} />
              </div>
            </div>

            <div className="flex flex-wrap justify-center gap-3 pt-4">
              <Link
                href="/docs/"
                className="inline-flex items-center gap-2 rounded-md bg-primary px-5 py-2.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
              >
                Read the docs
                <ArrowRight className="h-4 w-4" />
              </Link>
              <a
                href="https://demo.inputlayer.ai"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-5 py-2.5 text-sm font-medium hover:bg-secondary transition-colors"
              >
                Try the demo
                <ExternalLink className="h-3.5 w-3.5" />
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* ── Bottom CTA ───────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="relative rounded-2xl border border-border bg-gradient-to-br from-primary/10 via-transparent to-primary/5 p-12 text-center space-y-6">
            <h2 className="text-3xl font-bold tracking-tight">
              See it in action
            </h2>
            <p className="text-muted-foreground text-lg max-w-xl mx-auto">
              Try InputLayer in your browser. Load a knowledge graph, write rules, and query - no installation required.
            </p>
            <div className="flex flex-wrap justify-center gap-3 pt-2">
              <a
                href="https://demo.inputlayer.ai"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 rounded-md bg-primary px-6 py-3 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
              >
                Launch demo
                <ArrowRight className="h-4 w-4" />
              </a>
              <a
                href="https://github.com/inputlayer/inputlayer"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-6 py-3 text-sm font-medium hover:bg-secondary transition-colors"
              >
                View on GitHub
                <ExternalLink className="h-3.5 w-3.5" />
              </a>
            </div>
          </div>
        </div>
      </section>

      <SiteFooter />
    </div>
  )
}
