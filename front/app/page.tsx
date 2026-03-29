"use client"

import Link from "next/link"
import { useState } from "react"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"
import { BlogCard } from "@/components/blog-card"
import { EmbeddingDiagram, DiamondDiagram, WaterfallDiagram, ProvenanceTreeDiagram, VisualCodeTabs, HeroVisualization } from "@/components/landing-diagrams"
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
  Copy,
  Check,
} from "lucide-react"

// ── Syntax-highlighted code blocks ──────────────────────────────────────

const rulesVectorsCode = `// Facts: products with embeddings
+product[
  ("pg245", "Canon PG-245 Black Ink", 14.99, [0.82, 0.15, 0.91, 0.44]),
  ("cl246", "Canon CL-246 Color Ink", 16.99, [0.79, 0.18, 0.88, 0.41]),
  ("ep202", "Epson 202 Black Ink",    12.99, [0.83, 0.14, 0.90, 0.43])
]

// Compatibility facts (ep202 is NOT compatible with Canon)
+compatible[("canon_mg3620", "pg245"), ("canon_mg3620", "cl246")]
+owns[("shopper_42", "canon_mg3620")]
+in_stock[("pg245"), ("cl246"), ("ep202")]

// Rule: recommendable if compatible and in stock
+recommendable(S, P) <- owns(S, Dev), compatible(Dev, P), in_stock(P)

// Query: rules filter, cosine distance ranks (lower = more similar)
?recommendable("shopper_42", Pid),
 product(Pid, Desc, Price, Emb),
 Dist = cosine(Emb, [0.81, 0.16, 0.89, 0.42]),
 Dist < 0.05
// -> pg245  "Canon PG-245 Black Ink"  $14.99  Dist: 0.0001
// -> cl246  "Canon CL-246 Color Ink"  $16.99  Dist: 0.0002
// ep202 excluded by rule: similar vectors, incompatible printer`

const retractionCode = `// Two paths to the same conclusion
+owns[("alpha","beta"), ("alpha","delta"), ("beta","gamma"), ("delta","gamma")]
+sanctions_list[("gamma")]

+exposed(E, S) <- owns(E, S), sanctions_list(S)
+exposed(E, S) <- owns(E, Mid), exposed(Mid, S)

?exposed("alpha", Who)
// -> "alpha" | "gamma"    (1 row)

// Remove one path:
-owns("beta", "gamma")
?exposed("alpha", Who)
// -> "alpha" | "gamma"    (still exposed via delta)

// Remove second path:
-owns("delta", "gamma")
?exposed("alpha", Who)
// -> No results.           (correctly retracted)`

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

const provenanceCode = `+owns[("alpha", "beta"), ("beta", "gamma")]
+sanctions_list[("gamma")]

+exposed(E, S) <- owns(E, S), sanctions_list(S)
+exposed(E, S) <- owns(E, Mid), exposed(Mid, S)

// Why is alpha exposed?
.why ?exposed("alpha", Who)
// [rule] exposed (clause 1)
//   exposed(E, S) <- owns(E, Mid), exposed(Mid, S)
//   bindings: E="alpha", Mid="beta", S="gamma"
//   [base] owns("alpha", "beta")
//   [rule] exposed (clause 0)
//     exposed(E, S) <- owns(E, S), sanctions_list(S)
//     bindings: E="beta", S="gamma"
//     [base] owns("beta", "gamma")
//     [base] sanctions_list("gamma")

// Why is delta NOT exposed?
.why_not exposed("delta", "gamma")
// Rule: exposed (clause 0)
//   Blocker: owns("delta", _) - No matching tuples
// Rule: exposed (clause 1)
//   Blocker: owns("delta", _) - No matching tuples`

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
            <div className="space-y-6">
              <h1 className="text-4xl font-extrabold tracking-tight sm:text-5xl lg:text-6xl">
                Streaming reasoning layer
                <br />
                <span className="text-primary">for AI systems</span>
              </h1>
              <p className="text-lg text-muted-foreground max-w-lg">
                Store facts. Define rules. InputLayer derives the conclusions, keeps them current as data changes, and explains every result with a proof tree. Combine recursive reasoning with vector search in a single query. Open source.
              </p>
              <div className="flex flex-wrap gap-3 pt-2">
                <a
                  href="https://demo.inputlayer.ai"
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
            <div>
              <HeroVisualization />
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
                Similarity search finds things that look right. Rules find things that are right.
              </h2>
              <p className="text-muted-foreground">
                A shopper asks for printer ink. Vector search returns every ink cartridge with a high similarity score - Canon, Epson, Brother, all nearly identical in embedding space. But only one brand fits their printer. Recommending the wrong one means a return, a support ticket, and a customer who doesn't come back.
              </p>
              <p className="text-muted-foreground">
                InputLayer evaluates compatibility rules and ranks by vector similarity in a single query. The rule filters to what actually fits. The vector search ranks what's left by relevance.
              </p>
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
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Correct conclusion retraction</p>
              <h2 className="text-3xl font-bold tracking-tight">
                When a fact is deleted, every conclusion built on it needs to update. Correctly.
              </h2>
              <p className="text-muted-foreground">
                An entity is cleared from a sanctions list. The compliance flags derived through it need to retract. But what if the same entity is also flagged through a second, independent ownership path? Retract too aggressively and you miss real exposure. Don't retract at all and your team drowns in phantom alerts.
              </p>
              <p className="text-muted-foreground">
                InputLayer tracks every derivation path independently. A conclusion only retracts when every path supporting it is gone. This is the diamond problem - and getting it wrong has real consequences.
              </p>
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
                An operator's certification expires. How long until the planning agent knows?
              </h2>
              <p className="text-muted-foreground">
                In a 2,000-node dependency graph, one fact changes at the edge - a training record expires, a supplier is suspended, an ownership stake is sold. Every derived conclusion built on that fact needs to update. Recomputing everything takes 11.3 seconds. InputLayer traces the impact forward and updates only what's affected.
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
                Your agent flags a customer as churn risk. Can it show its work?
              </h2>
              <p className="text-muted-foreground">
                A VP asks "why was this flagged?" and the answer is "the model predicted it" - that's not auditable, not actionable, and not trustworthy. Run <code className="text-xs bg-muted/50 px-1.5 py-0.5 rounded">.why</code> on any InputLayer result and get a structured proof tree: which facts, which rules, which chain of reasoning produced the conclusion. Run <code className="text-xs bg-muted/50 px-1.5 py-0.5 rounded">.why_not</code> to see exactly which condition blocked a derivation.
              </p>
              <div className="text-center pt-2">
                <span className="text-5xl font-extrabold text-primary">100%</span>
                <p className="text-xs text-muted-foreground mt-1">of results fully traceable</p>
              </div>
            </div>
            <VisualCodeTabs visual={<ProvenanceTreeDiagram />} code={provenanceCode} />
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
              From financial compliance to conversational commerce, InputLayer powers applications where answers require following chains of connected facts.
            </p>
          </div>

          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
            {[
              { title: "Financial Risk", icon: <Shield className="h-8 w-8 text-primary" />, desc: "Trace ownership chains to any depth. When an entity is cleared from a sanctions list, downstream flags retract - but only if no second path still supports them.", href: "/use-cases/financial-risk/", tags: ["Correct conclusion retraction", "Provenance"] },
              { title: "Commerce", icon: <ShoppingBag className="h-8 w-8 text-primary" />, desc: "Every ink cartridge looks the same in embedding space. Compatibility rules filter to what actually fits the shopper's printer, then vectors rank by relevance.", href: "/use-cases/commerce/", tags: ["Rules + vectors", "Incremental"] },
              { title: "Manufacturing", icon: <Factory className="h-8 w-8 text-primary" />, desc: "An operator's training expires. The certification, qualification, and production line availability retract through a four-level dependency chain - in milliseconds.", href: "/use-cases/manufacturing/", tags: ["Incremental", "Provenance"] },
              { title: "Supply Chain", icon: <Truck className="h-8 w-8 text-primary" />, desc: "A port closes. Within milliseconds, every affected supplier, order, and SLA penalty is identified across the entire supply graph.", href: "/use-cases/supply-chain/", tags: ["Incremental"] },
            ].map((uc) => (
              <Link
                key={uc.title}
                href={uc.href}
                className="group rounded-xl border border-border bg-card p-6 space-y-4 transition-colors hover:border-primary/30 hover:bg-card/80"
              >
                {uc.icon}
                <h3 className="text-lg font-semibold group-hover:text-primary transition-colors">{uc.title}</h3>
                <p className="text-sm text-muted-foreground">{uc.desc}</p>
                <div className="flex flex-wrap gap-1.5">
                  {uc.tags.map((tag) => (
                    <span key={tag} className="inline-flex items-center rounded-full border border-primary/20 bg-primary/5 px-2 py-0.5 text-[10px] text-primary">
                      {tag}
                    </span>
                  ))}
                </div>
                <span className="inline-flex items-center gap-1 text-sm text-primary font-medium">
                  Learn more <ArrowRight className="h-3.5 w-3.5" />
                </span>
              </Link>
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

      {/* ── Get Started ───────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="relative rounded-2xl border border-border bg-gradient-to-br from-primary/10 via-transparent to-primary/5 p-12 text-center space-y-6">
            <h2 className="text-3xl font-bold tracking-tight">
              Open source. Run it yourself.
            </h2>
            <p className="text-muted-foreground text-lg max-w-xl mx-auto">
              One Docker command. No account, no API key, no vendor lock-in. Try it in your browser or pull the image and run locally.
            </p>

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

            <div className="flex flex-wrap justify-center gap-3 pt-2">
              <a
                href="https://demo.inputlayer.ai"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-2 rounded-md bg-primary px-6 py-3 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
              >
                Try the demo
                <ArrowRight className="h-4 w-4" />
              </a>
              <Link
                href="/docs/"
                className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-6 py-3 text-sm font-medium hover:bg-secondary transition-colors"
              >
                Read the docs
                <ArrowRight className="h-3.5 w-3.5" />
              </Link>
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
