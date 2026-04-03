"use client"

import Link from "next/link"
import { useState, useEffect } from "react"
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

const rulesVectorsCode = `// Flight network
+direct_flight[("new_york", "london", 7.0),
               ("london", "paris", 1.5),
               ("paris", "tokyo", 12.0),
               ("tokyo", "sydney", 9.5)]

// What each destination is like (culture, beach, food, nightlife)
+destination[("london", [0.82, 0.15, 0.71, 0.68]),
             ("paris",  [0.88, 0.12, 0.63, 0.95]),
             ("tokyo",  [0.76, 0.22, 0.85, 0.94]),
             ("sydney", [0.31, 0.91, 0.42, 0.67])]

// If you can get there (even with connections), you can reach it
+can_reach(A, B) <- direct_flight(A, B, _)
+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)

// Where can I fly from NYC that feels like a beach vacation?
?can_reach("new_york", Dest),
 destination(Dest, Emb),
 Dist = cosine(Emb, [0.25, 0.95, 0.40, 0.55])
// -> sydney  0.08  (best match - beaches & outdoors)
// -> tokyo   0.52
// -> london  0.61
// -> paris   0.65
// Logic finds where you CAN go, search ranks by what you WANT`

const retractionCode = `// Two independent routes to Sydney
+direct_flight[("new_york", "london", 7.0),
               ("london", "paris", 1.5),
               ("paris", "tokyo", 12.0),
               ("tokyo", "sydney", 9.5),
               ("london", "dubai", 7.0),
               ("dubai", "sydney", 11.0)]

+can_reach(A, B) <- direct_flight(A, B, _)
+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)

?can_reach("new_york", "sydney")
// -> reachable              (via Tokyo AND via Dubai)

// Dubai route cancelled:
-direct_flight("london", "dubai", 7.0)
?can_reach("new_york", "sydney")
// -> reachable              (still reachable via Tokyo)

// Tokyo route also cancelled:
-direct_flight("tokyo", "sydney", 9.5)
?can_reach("new_york", "sydney")
// -> No results.            (correctly unreachable)`

const incrementalCode = `+direct_flight[("new_york", "london", 7.0),
               ("london", "paris", 1.5),
               ("paris", "tokyo", 12.0),
               ("tokyo", "sydney", 9.5)]

+can_reach(A, B) <- direct_flight(A, B, _)
+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)

?can_reach("new_york", Dest)
// -> london, paris, tokyo, sydney   (4 destinations)

// Add one new route:
+direct_flight("london", "dubai", 7.0)

?can_reach("new_york", Dest)
// -> london, paris, tokyo, sydney, dubai
// Only new connections calculated - not everything from scratch`

const provenanceCode = `+direct_flight[("new_york", "london", 7.0),
               ("london", "paris", 1.5),
               ("paris", "tokyo", 12.0),
               ("tokyo", "sydney", 9.5)]

+can_reach(A, B) <- direct_flight(A, B, _)
+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)

// How can I get from New York to Sydney?
.why ?can_reach("new_york", "sydney")
// [rule] can_reach (clause 1)
//   [base] direct_flight("new_york", "london", 7.0)
//   [rule] can_reach("london", "sydney")
//     [base] direct_flight("london", "paris", 1.5)
//     [rule] can_reach("paris", "sydney")
//       [base] direct_flight("paris", "tokyo", 12.0)
//       [base] direct_flight("tokyo", "sydney", 9.5)

// Why can't I reach São Paulo?
.why_not can_reach("new_york", "sao_paulo")
// No flights to "sao_paulo" from anywhere in the network`

const dockerCommand = "docker run -p 8080:8080 ghcr.io/inputlayer/inputlayer"

// ── Helper components ───────────────────────────────────────────────────

function RotatingWord({ words }: { words: string[] }) {
  const [index, setIndex] = useState(0)
  useEffect(() => {
    const timer = setInterval(() => setIndex((i) => (i + 1) % words.length), 2000)
    return () => clearInterval(timer)
  }, [words.length])
  return (
    <span className="inline-block min-w-[80px] text-primary font-semibold transition-opacity duration-300">
      {words[index]}
    </span>
  )
}

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
              <p className="text-lg text-muted-foreground max-w-[540px]">
                Store facts. Write rules. InputLayer draws conclusions, keeps them up to date as things change, and can show exactly how it got every conclusion.
              </p>
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
            <div>
              <HeroVisualization />
            </div>
          </div>
        </div>
      </section>

      {/* ── Who Builds With InputLayer ─────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="max-w-2xl mb-12">
            <p className="text-sm font-semibold text-primary uppercase tracking-wider mb-2">How it works</p>
            <h2 className="text-3xl font-bold tracking-tight">
              Give your AI conclusions it can trust
            </h2>
            <p className="text-muted-foreground mt-4">
              InputLayer figures out what&apos;s true from the facts and rules you give it. Your AI gets reliable, up-to-date conclusions instead of guessing, and every conclusion can be traced back to the facts behind it.
            </p>
          </div>

          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
            {[
              { icon: <Brain className="h-6 w-6 text-primary" />, title: "Thinks, not just stores", desc: "Your AI stores facts. InputLayer connects the dots and draws conclusions automatically." },
              { icon: <Zap className="h-6 w-6 text-primary" />, title: "Search + logic together", desc: "Find things by similarity, then filter by what's actually true. One query, both at once." },
              { icon: <Shield className="h-6 w-6 text-primary" />, title: "Shows its work", desc: "Every conclusion traces back to the facts and rules that produced it. Ask \"why?\" on any result." },
              { icon: <GitBranch className="h-6 w-6 text-primary" />, title: "Always up to date", desc: "When a fact changes, every affected conclusion updates in milliseconds. No waiting, no rebuilding." },
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
              What you get that other tools don&apos;t do
            </h2>
            <p className="text-muted-foreground max-w-2xl">
              Databases store data. InputLayer thinks about it, combining search, logic, and live updates in one place.
            </p>
          </div>

          <ComparisonTable
            columns={["Vector DBs", "Graph DBs", "SQL", "InputLayer"]}
            highlightColumn="InputLayer"
            rows={[
              { capability: "Find by similarity", values: { "Vector DBs": "native", "Graph DBs": "plugin", "SQL": "plugin", "InputLayer": "native" } },
              { capability: "Follow connections", values: { "Vector DBs": "none", "Graph DBs": "native", "SQL": "partial", "InputLayer": "native" } },
              { capability: "Apply rules", values: { "Vector DBs": "none", "Graph DBs": "partial", "SQL": "partial", "InputLayer": "native" } },
              { capability: "Chain logic together", values: { "Vector DBs": "none", "Graph DBs": "native", "SQL": "native", "InputLayer": "native" } },
              { capability: "Live updates", values: { "Vector DBs": "none", "Graph DBs": "none", "SQL": "partial", "InputLayer": "native" } },
              { capability: "Undo when facts change", values: { "Vector DBs": "none", "Graph DBs": "recompute", "SQL": "recompute", "InputLayer": "native" } },
              { capability: "Explain every conclusion", values: { "Vector DBs": "none", "Graph DBs": "partial", "SQL": "partial", "InputLayer": "native" } },
            ]}
          />
        </div>
      </section>

      {/* ── Rules + Vectors ────────────────────────────────────────── */}
      <section className="border-b border-border/50">
        <div className="mx-auto max-w-6xl px-6 py-20">
          <div className="grid gap-12 lg:grid-cols-2 items-start">
            <div className="space-y-6">
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Search + logic</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Where can I fly that feels like a beach vacation?
              </h2>
              <p className="text-muted-foreground">
                You're in New York and want beach destinations, but only ones you can actually get to. First, InputLayer follows the flight network to figure out which cities are reachable (even with connecting flights). Then it ranks those cities by how well they match "beach vacation."
              </p>
              <p className="text-sm font-semibold text-primary uppercase tracking-wider pt-2">One query does both</p>
              <p className="text-muted-foreground">
                Rules figure out where you can go. Search ranks by what you want. InputLayer does both in a <strong>single query</strong>. No stitching things together, no extra steps.
              </p>
              <a
                href={`${DEMO_BASE_URL}/demo/request-access?kg=flights`}
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
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Smart undo</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Cancel a route. Does the destination stay reachable?
              </h2>
              <p className="text-muted-foreground">
                Sydney is reachable from New York two ways, through Tokyo and through Dubai. Cancel the Dubai route, and most systems would just wipe the conclusion. But the Tokyo route still works.
              </p>
              <p className="text-muted-foreground">
                InputLayer tracks both paths. It only removes a conclusion when every way to reach it is gone. Sydney stays reachable until both routes are cancelled. No wrong removals, no stale conclusions.
              </p>
              <a
                href={`${DEMO_BASE_URL}/demo/request-access?kg=flights`}
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
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Instant updates</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Add a route. Only the affected conclusions update.
              </h2>
              <p className="text-muted-foreground">
                Your flight network has 2,000 airports and hundreds of thousands of possible connections. Add one new route, say London to Dubai, and most systems recalculate everything from scratch. InputLayer only updates the conclusions that actually changed.
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
                href={`${DEMO_BASE_URL}/demo/request-access?kg=flights`}
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
              <p className="text-sm font-semibold text-primary uppercase tracking-wider">Explainability</p>
              <h2 className="text-3xl font-bold tracking-tight">
                Ask &quot;why?&quot; and get the full reasoning chain
              </h2>
              <p className="text-muted-foreground">
                How does New York connect to Sydney? Ask <code className="text-xs bg-muted/50 px-1.5 py-0.5 rounded">.why</code> and see every step: NY to London, London to Paris, Paris to Tokyo, Tokyo to Sydney. Or ask <code className="text-xs bg-muted/50 px-1.5 py-0.5 rounded">.why_not</code> to see why São Paulo can&apos;t be reached. Every conclusion comes with the receipts.
              </p>
              <div className="flex justify-center pt-2">
                <div className="text-center">
                  <span className="text-5xl font-extrabold text-primary">100%</span>
                  <p className="text-xs text-muted-foreground mt-1">
                    of results fully <RotatingWord words={["explainable", "traceable", "verifiable", "reproducible"]} />
                  </p>
                </div>
              </div>
              <a
                href={`${DEMO_BASE_URL}/demo/request-access?kg=flights`}
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
                context: "Extends: Search + logic",
                title: "Why Search Alone Isn't Enough for Your AI Agent",
                desc: "Finding similar items is useful, but it can't tell you which destinations are actually reachable. See how rules and search work together in one query.",
              },
              {
                slug: "correct-retraction-why-delete-should-actually-delete",
                context: "Extends: Smart undo",
                title: "Why Delete Should Actually Delete",
                desc: "When a route gets cancelled, which destinations become unreachable? Getting the right conclusion is harder than it sounds, and getting it wrong can break everything downstream.",
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
                  desc: "How to write facts, rules, and queries. The full reference.",
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
