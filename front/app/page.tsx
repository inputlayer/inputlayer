"use client"

import Link from "next/link"
import { useState } from "react"
import { SiteHeader } from "@/components/site-header"
import { Logo } from "@/components/logo"
import { highlightToHtml } from "@/lib/syntax-highlight"
import {
  ArrowRight,
  ExternalLink,
  Zap,
  Shield,
  Brain,
  Database,
  GitBranch,
  CheckCircle,
  XCircle,
  Minus,
  Copy,
  Check,
} from "lucide-react"

// ── Syntax-highlighted code blocks ──────────────────────────────────────

const heroCode = `// Facts: who manages whom
+manages("alice", "bob")
+manages("bob", "charlie")
+manages("bob", "diana")

// Rule: transitive authority (recursive)
+authority(X, Y) <- manages(X, Y)
+authority(X, Z) <- manages(X, Y), authority(Y, Z)

// Query: who does Alice have authority over?
?authority("alice", Person)`

const policySearchCode = `// Policy-filtered semantic search - one query
?authority("alice", Author),
 document(DocId, Author, Embedding),
 Similarity = cosine(Embedding, QueryVec),
 Similarity > 0.7`

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
                A reasoning engine
                <br />
                <span className="text-primary">for AI agents</span>
              </h1>
              <p className="text-lg text-muted-foreground max-w-lg">
                InputLayer is a modern open-source database built on three key concepts:
              </p>
              <ul className="space-y-2 text-muted-foreground">
                <li className="flex items-start gap-3">
                  <Database className="h-5 w-5 text-primary mt-0.5 shrink-0" />
                  <span><strong className="text-foreground">Knowledge graph</strong> - data is stored as facts and relationships, not flat documents</span>
                </li>
                <li className="flex items-start gap-3">
                  <Brain className="h-5 w-5 text-primary mt-0.5 shrink-0" />
                  <span><strong className="text-foreground">Deductive</strong> - you define rules, and the system derives everything that logically follows</span>
                </li>
                <li className="flex items-start gap-3">
                  <Zap className="h-5 w-5 text-primary mt-0.5 shrink-0" />
                  <span><strong className="text-foreground">Streaming</strong> - when facts change, all derived conclusions update instantly</span>
                </li>
              </ul>
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
                  GitHub
                  <ExternalLink className="h-3.5 w-3.5" />
                </a>
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
                <Shield className="h-5 w-5" />
                <span className="font-semibold">Healthcare</span>
              </div>
              <p className="text-sm text-muted-foreground">
                Patient asks <em>&ldquo;Can I eat shrimp tonight?&rdquo;</em> System finds recipes. Misses the allergy record three hops away: patient takes Drug X &rarr; interacts with iodine &rarr; shrimp is high in iodine.
              </p>
              <p className="text-xs text-muted-foreground/70">
                Drug &rarr; interaction &rarr; ingredient &rarr; food has zero vector similarity to &ldquo;shrimp dinner.&rdquo;
              </p>
            </div>

            <div className="rounded-xl border border-border bg-card p-6 space-y-3">
              <div className="flex items-center gap-2 text-warning-foreground">
                <Database className="h-5 w-5" />
                <span className="font-semibold">Enterprise</span>
              </div>
              <p className="text-sm text-muted-foreground">
                Employee asks for Q3 revenue reports. Vector DB returns 40 matching documents. Cannot check whether this employee, in this role, in this department, has permission to see any of them.
              </p>
              <p className="text-xs text-muted-foreground/70">
                Access control is a logical question, not a similarity question.
              </p>
            </div>

            <div className="rounded-xl border border-border bg-card p-6 space-y-3">
              <div className="flex items-center gap-2 text-accent">
                <GitBranch className="h-5 w-5" />
                <span className="font-semibold">Financial Services</span>
              </div>
              <p className="text-sm text-muted-foreground">
                Compliance asks <em>&ldquo;Is this transaction suspicious?&rdquo;</em> System finds similar transactions. Misses: Entity A paid Entity B, B is a subsidiary of C, C is on a sanctions list.
              </p>
              <p className="text-xs text-muted-foreground/70">
                Graph traversal + rule evaluation - not pattern matching.
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
                <span className="text-primary font-semibold">InputLayer&apos;s query language</span> combines graph traversal, logical rules, and vector search in a single query.
              </p>
              <ul className="space-y-4">
                <li className="flex items-start gap-3">
                  <CheckCircle className="h-5 w-5 text-emerald-500 mt-0.5 shrink-0" />
                  <div>
                    <p className="font-medium">Recursive rule evaluation</p>
                    <p className="text-sm text-muted-foreground">Define rules like transitive authority. The engine recursively derives all conclusions - including things you never explicitly stored.</p>
                  </div>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle className="h-5 w-5 text-emerald-500 mt-0.5 shrink-0" />
                  <div>
                    <p className="font-medium">Policy-filtered search</p>
                    <p className="text-sm text-muted-foreground">Logical access control and vector similarity in one pass. Permission-checked and semantically ranked results without glue code.</p>
                  </div>
                </li>
                <li className="flex items-start gap-3">
                  <CheckCircle className="h-5 w-5 text-emerald-500 mt-0.5 shrink-0" />
                  <div>
                    <p className="font-medium">One query replaces three systems</p>
                    <p className="text-sm text-muted-foreground">A single query replaces a vector DB call, a graph traversal, and a policy engine.</p>
                  </div>
                </li>
              </ul>
            </div>

            {/* Right: example */}
            <div className="space-y-3">
              <p className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">Example</p>
              <CodeBlock code={policySearchCode} />
              <p className="text-sm text-muted-foreground">
                This query resolves the <code className="bg-muted rounded px-1.5 py-0.5 text-xs font-mono">authority</code> rule recursively, runs vector search in the same pass, and returns only documents that Alice has permission to see and that are semantically relevant to her question.
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
                When a fact changes, only the affected derivations recompute. Insert one new edge into a 2,000-node graph and re-query transitive closure: 6.83ms instead of 11.3 seconds.
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
                Every derived fact traces back to the rules and base facts that produced it. Not &ldquo;the vector was close&rdquo; - a full derivation chain you can audit and explain.
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
                Delete a fact and every conclusion derived through it disappears automatically - even through chains of recursive rules. No phantom permissions, no manual cache invalidation.
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
              One system, not four
            </h2>
            <p className="text-muted-foreground text-lg">
              Replace the duct-tape architecture where you run a vector DB, a graph DB, a rules engine, and a batch pipeline - and try to keep them in sync.
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
                href="https://github.com/inputlayer/inputlayer"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-5 py-2.5 text-sm font-medium hover:bg-secondary transition-colors"
              >
                View on GitHub
                <ExternalLink className="h-3.5 w-3.5" />
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* ── Footer ─────────────────────────────────────────────────── */}
      <footer className="border-t border-border/50 bg-card/50">
        <div className="mx-auto max-w-6xl px-6 py-10">
          <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
            <div className="flex items-center gap-4">
              <Logo size="sm" />
              <span className="text-sm text-muted-foreground">
                AGPL-3.0 License
              </span>
            </div>
            <nav className="flex items-center gap-6 text-sm text-muted-foreground">
              <Link href="/docs/" className="hover:text-foreground transition-colors">
                Documentation
              </Link>
              <a
                href="https://github.com/inputlayer/inputlayer"
                target="_blank"
                rel="noopener noreferrer"
                className="hover:text-foreground transition-colors"
              >
                GitHub
              </a>
            </nav>
          </div>
        </div>
      </footer>
    </div>
  )
}
