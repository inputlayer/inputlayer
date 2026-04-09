"use client"

import { useEffect, useRef, useState, type ReactNode } from "react"
import { highlightToHtml } from "@/lib/syntax-highlight"
import { Eye, Code } from "lucide-react"

/* ── Shared: Intersection observer hook for scroll-triggered animation ── */

function useInView(threshold = 0.3) {
  const ref = useRef<HTMLDivElement>(null)
  const [visible, setVisible] = useState(false)
  useEffect(() => {
    const el = ref.current
    if (!el) return
    const obs = new IntersectionObserver(([e]) => { if (e.isIntersecting) { setVisible(true); obs.disconnect() } }, { threshold })
    obs.observe(el)
    return () => obs.disconnect()
  }, [threshold])
  return { ref, visible }
}

/* ═══════════════════════════════════════════════════════════════════════ */
/*  1. EMBEDDING SPACE — Rules + Vectors                                  */
/*  2D scatter showing destinations clustered by similarity,              */
/*  with a rule boundary that separates reachable from unreachable        */
/* ═══════════════════════════════════════════════════════════════════════ */

const embDestinations = [
  { id: "sydney", label: "Sydney", x: 160, y: 85, sim: 0.08, ok: true },
  { id: "tokyo", label: "Tokyo", x: 310, y: 80, sim: 0.52, ok: true },
  { id: "london", label: "London", x: 360, y: 155, sim: 0.61, ok: true },
  { id: "paris", label: "Paris", x: 330, y: 130, sim: 0.65, ok: true },
  { id: "rio", label: "Rio", x: 180, y: 150, sim: 0.12, ok: false },
  { id: "bali", label: "Bali", x: 230, y: 100, sim: 0.15, ok: false },
]

export function EmbeddingDiagram() {
  const { ref, visible } = useInView()
  const w = 520, h = 260

  return (
    <div ref={ref} className="w-full">
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full" style={{ maxHeight: 340 }}>
        <defs>
          <radialGradient id="emb-glow-ok" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="oklch(0.7 0.18 160)" stopOpacity="0.5" />
            <stop offset="100%" stopColor="oklch(0.7 0.18 160)" stopOpacity="0" />
          </radialGradient>
          <radialGradient id="emb-glow-fail" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="oklch(0.65 0.15 25)" stopOpacity="0.4" />
            <stop offset="100%" stopColor="oklch(0.65 0.15 25)" stopOpacity="0" />
          </radialGradient>
          <linearGradient id="emb-rule-line" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--primary)" stopOpacity="0" />
            <stop offset="50%" stopColor="var(--primary)" stopOpacity="0.6" />
            <stop offset="100%" stopColor="var(--primary)" stopOpacity="0" />
          </linearGradient>
        </defs>

        {/* Grid */}
        {[80, 160, 240, 320, 400].map((x) => (
          <line key={`gx${x}`} x1={x} y1={10} x2={x} y2={h - 10} style={{ stroke: "var(--border)", strokeWidth: 0.5, opacity: 0.3 }} />
        ))}
        {[50, 100, 150, 200].map((y) => (
          <line key={`gy${y}`} x1={40} y1={y} x2={w - 20} y2={y} style={{ stroke: "var(--border)", strokeWidth: 0.5, opacity: 0.3 }} />
        ))}

        {/* Axis labels */}
        <text x={w / 2} y={h - 2} textAnchor="middle" style={{ fill: "var(--muted-foreground)", fontSize: 9, opacity: 0.5 }}>embedding dimension 1</text>
        <text x={12} y={h / 2} textAnchor="middle" transform={`rotate(-90, 12, ${h / 2})`} style={{ fill: "var(--muted-foreground)", fontSize: 9, opacity: 0.5 }}>embedding dimension 2</text>

        {/* Cluster label */}
        <text x={270} y={228} textAnchor="middle" style={{ fill: "var(--muted-foreground)", fontSize: 10, opacity: visible ? 0.5 : 0 , transition: "opacity 0.8s 0.3s" }}>
          ranked by similarity to &quot;beach vacation&quot;
        </text>

        {/* Rule boundary line */}
        <line
          x1={270} y1={10} x2={270} y2={h - 30}
          style={{
            stroke: "url(#emb-rule-line)",
            strokeWidth: 2,
            strokeDasharray: "6 4",
            opacity: visible ? 1 : 0,
            transition: "opacity 0.6s 1.5s",
          }}
        />
        <text x={400} y={25} textAnchor="middle" style={{ fill: "var(--primary)", fontSize: 9, fontWeight: 600, opacity: visible ? 0.8 : 0, transition: "opacity 0.5s 1.8s" }}>
          REACHABLE
        </text>
        <text x={140} y={25} textAnchor="middle" style={{ fill: "var(--muted-foreground)", fontSize: 9, fontWeight: 600, opacity: visible ? 0.5 : 0, transition: "opacity 0.5s 1.8s" }}>
          UNREACHABLE
        </text>

        {/* Destination dots */}
        {embDestinations.map((p, i) => {
          const delay = 0.3 + i * 0.12
          const ruleDelay = 1.6 + i * 0.08
          return (
            <g key={p.id} style={{ opacity: visible ? 1 : 0, transition: `opacity 0.5s ${delay}s`, transform: visible ? "scale(1)" : "scale(0)", transformOrigin: `${p.x}px ${p.y}px`, transitionProperty: "opacity, transform", transitionDuration: "0.5s", transitionDelay: `${delay}s` }}>
              {/* Glow */}
              <circle cx={p.x} cy={p.y} r={28} fill={p.ok ? "url(#emb-glow-ok)" : "url(#emb-glow-fail)"}
                style={{ opacity: visible ? 1 : 0, transition: `opacity 0.5s ${ruleDelay}s` }} />
              {/* Dot */}
              <circle cx={p.x} cy={p.y} r={6}
                style={{
                  fill: p.ok ? "oklch(0.7 0.18 160)" : "oklch(0.6 0.12 25)",
                  stroke: p.ok ? "oklch(0.8 0.18 160)" : "oklch(0.7 0.12 25)",
                  strokeWidth: 1.5,
                  opacity: visible ? (p.ok ? 1 : 0.35) : 1,
                  transition: `opacity 0.5s ${ruleDelay}s`,
                }}
              />
              {/* Label */}
              <text x={p.x} y={p.y - 12} textAnchor="middle"
                style={{
                  fill: p.ok ? "oklch(0.85 0.12 160)" : "var(--muted-foreground)",
                  fontSize: 9,
                  fontWeight: 500,
                  opacity: visible ? (p.ok ? 1 : 0.3) : 0.8,
                  transition: `opacity 0.5s ${ruleDelay}s`,
                }}>
                {p.label}
              </text>
              {/* Similarity score */}
              <text x={p.x} y={p.y + 18} textAnchor="middle"
                style={{
                  fill: "var(--muted-foreground)",
                  fontSize: 8,
                  fontFamily: "var(--font-mono, monospace)",
                  opacity: visible ? 0.5 : 0,
                  transition: `opacity 0.4s ${delay + 0.3}s`,
                }}>
                {p.sim.toFixed(2)}
              </text>
            </g>
          )
        })}

        {/* Query vector */}
        <g style={{ opacity: visible ? 1 : 0, transition: "opacity 0.5s 0.1s" }}>
          <circle cx={100} cy={100} r={4} style={{ fill: "var(--primary)", stroke: "var(--primary)", strokeWidth: 1.5, opacity: 0.8 }} />
          <text x={100} y={90} textAnchor="middle" style={{ fill: "var(--primary)", fontSize: 9, fontWeight: 600 }}>query</text>
          <text x={100} y={115} textAnchor="middle" style={{ fill: "var(--primary)", fontSize: 8, opacity: 0.7 }}>&quot;beach vacation&quot;</text>
        </g>
      </svg>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════════════ */
/*  2. DIAMOND RETRACTION — Correct retraction                            */
/*  Two independent flight paths to Sydney,                               */
/*  sequentially removing them to demonstrate correct retraction          */
/* ═══════════════════════════════════════════════════════════════════════ */

export function DiamondDiagram() {
  const { ref, visible } = useInView()
  const w = 480, h = 300

  const nodes = [
    { id: "Sydney", x: 240, y: 45 },
    { id: "via Tokyo", x: 110, y: 165 },
    { id: "via Dubai", x: 370, y: 165 },
  ]

  return (
    <div ref={ref} className="w-full">
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full" style={{ maxHeight: 360 }}>
        <defs>
          <marker id="dia-arrow" viewBox="0 0 10 8" refX="10" refY="4" markerWidth="8" markerHeight="6" orient="auto">
            <path d="M0,0 L10,4 L0,8z" style={{ fill: "var(--border)" }} />
          </marker>
          <radialGradient id="dia-glow" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="oklch(0.7 0.18 160)" stopOpacity="0.25" />
            <stop offset="100%" stopColor="oklch(0.7 0.18 160)" stopOpacity="0" />
          </radialGradient>
          <filter id="dia-shadow">
            <feDropShadow dx="0" dy="1" stdDeviation="3" floodColor="black" floodOpacity="0.2" />
          </filter>
        </defs>

        <style>{`
          @keyframes dia-phase {
            0%, 25% { opacity: 1; }
            30%, 55% { opacity: 0.12; }
            60%, 100% { opacity: 0.12; }
          }
          @keyframes dia-phase2 {
            0%, 55% { opacity: 1; }
            60%, 85% { opacity: 0.12; }
            90%, 100% { opacity: 0.12; }
          }
          @keyframes dia-reachable {
            0%, 25% { opacity: 1; }
            30%, 55% { opacity: 1; }
            60%, 85% { opacity: 0; }
            90%, 100% { opacity: 0; }
          }
          @keyframes dia-label1 {
            0%, 25% { opacity: 0; }
            30%, 55% { opacity: 1; }
            60%, 100% { opacity: 0; }
          }
          @keyframes dia-unreachable {
            0%, 55% { opacity: 0; }
            65%, 85% { opacity: 1; }
            90%, 100% { opacity: 0; }
          }
        `}</style>

        {/* Edge: Sydney <- via Dubai (path 1 - cancelled first) */}
        <g style={{ animation: visible ? "dia-phase 10s ease-in-out 1s infinite" : "none" }}>
          <line x1={340} y1={142} x2={275} y2={62} style={{ stroke: "var(--border)", strokeWidth: 1.5 }} markerEnd="url(#dia-arrow)" />
          <text x={330} y={100} textAnchor="middle" style={{ fill: "var(--muted-foreground)", fontSize: 9, opacity: 0.5 }}>derives</text>
        </g>

        {/* Edge: Sydney <- via Tokyo (path 2 - cancelled second) */}
        <g style={{ animation: visible ? "dia-phase2 10s ease-in-out 1s infinite" : "none" }}>
          <line x1={140} y1={142} x2={205} y2={62} style={{ stroke: "var(--border)", strokeWidth: 1.5 }} markerEnd="url(#dia-arrow)" />
          <text x={150} y={100} textAnchor="middle" style={{ fill: "var(--muted-foreground)", fontSize: 9, opacity: 0.5 }}>derives</text>
        </g>

        {/* Nodes */}
        {nodes.map((n, i) => (
          <g key={n.id} style={{ opacity: visible ? 1 : 0, transition: `opacity 0.5s ${0.2 + i * 0.15}s`, transform: visible ? "scale(1)" : "scale(0.8)", transformOrigin: `${n.x}px ${n.y}px`, transitionProperty: "opacity, transform", transitionDuration: "0.5s, 0.5s", transitionDelay: `${0.2 + i * 0.15}s` }}>
            <rect x={n.x - 55} y={n.y - 18} width={110} height={36} rx={8} filter="url(#dia-shadow)"
              style={{ fill: "var(--card)", stroke: "var(--border)", strokeWidth: 1 }} />
            <text x={n.x} y={n.y + 5} textAnchor="middle"
              style={{ fill: "var(--foreground)", fontSize: 12, fontWeight: 600, fontFamily: "inherit" }}>
              {n.id}
            </text>
          </g>
        ))}

        {/* REACHABLE badge on Sydney */}
        <g style={{ animation: visible ? "dia-reachable 10s ease-in-out 1s infinite" : "none" }}>
          <circle cx={240} cy={45} r={55} fill="url(#dia-glow)" />
          <rect x={298} y={30} width={75} height={20} rx={10} style={{ fill: "oklch(0.6 0.18 160 / 0.25)", stroke: "oklch(0.7 0.18 160 / 0.6)", strokeWidth: 0.5 }} />
          <text x={335} y={43} textAnchor="middle" style={{ fill: "oklch(0.85 0.15 160)", fontSize: 8, fontWeight: 700, letterSpacing: "0.06em" }}>REACHABLE</text>
        </g>

        {/* Phase labels */}
        <text x={240} y={225} textAnchor="middle" style={{ fill: "oklch(0.7 0.18 160)", fontSize: 10, fontWeight: 500, animation: visible ? "dia-label1 10s ease-in-out 1s infinite" : "none" }}>
          Dubai cancelled - still reachable via Tokyo
        </text>
        <text x={240} y={225} textAnchor="middle" style={{ fill: "oklch(0.7 0.15 25)", fontSize: 10, fontWeight: 500, animation: visible ? "dia-unreachable 10s ease-in-out 1s infinite" : "none" }}>
          both cancelled - correctly unreachable
        </text>
      </svg>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════════════ */
/*  3. WATERFALL — Incremental updates                                    */
/*  Dramatic bar comparison: 11,280ms vs 6.83ms                           */
/* ═══════════════════════════════════════════════════════════════════════ */

export function WaterfallDiagram() {
  const { ref, visible } = useInView()

  return (
    <div ref={ref} className="w-full space-y-6">
      {/* Full recompute bar */}
      <div>
        <div className="flex items-baseline justify-between mb-2">
          <span className="text-sm text-muted-foreground">Full recompute</span>
          <span className="text-sm font-mono text-foreground" style={{ opacity: visible ? 1 : 0, transition: "opacity 0.5s 1.8s" }}>11.3s</span>
        </div>
        <div className="h-12 rounded-lg relative overflow-hidden" style={{ background: "oklch(0.2 0.01 260)" }}>
          <div
            className="h-full rounded-lg"
            style={{
              width: visible ? "100%" : "0%",
              transition: "width 1.5s cubic-bezier(0.22, 1, 0.36, 1) 0.5s",
              background: "linear-gradient(90deg, oklch(0.35 0.04 260 / 0.4), oklch(0.45 0.06 260 / 0.6))",
              borderRight: "2px solid oklch(0.5 0.06 260 / 0.4)",
            }}
          />
          <div className="absolute inset-0 flex items-center justify-center" style={{ opacity: visible ? 1 : 0, transition: "opacity 0.4s 2s" }}>
            <span className="text-xs font-mono" style={{ color: "white" }}>recomputes all 400,000 reachable pairs</span>
          </div>
        </div>
      </div>

      {/* Incremental bar */}
      <div>
        <div className="flex items-baseline justify-between mb-2">
          <span className="text-sm font-semibold" style={{ color: "var(--primary)" }}>Incremental update</span>
          <span className="text-sm font-mono font-bold" style={{ color: "var(--primary)", opacity: visible ? 1 : 0, transition: "opacity 0.5s 2.2s" }}>6.83 ms</span>
        </div>
        <div className="h-12 rounded-lg relative overflow-hidden" style={{ background: "oklch(0.2 0.01 260)" }}>
          <div
            className="h-full rounded-lg relative"
            style={{
              width: visible ? "0.06%" : "0%",
              minWidth: visible ? "8px" : "0px",
              transition: "width 0.8s cubic-bezier(0.22, 1, 0.36, 1) 1.2s, min-width 0.8s cubic-bezier(0.22, 1, 0.36, 1) 1.2s",
              background: "linear-gradient(90deg, oklch(0.55 0.18 160 / 0.6), oklch(0.65 0.22 160 / 0.9))",
              boxShadow: visible ? "0 0 20px oklch(0.6 0.2 160 / 0.4), 0 0 40px oklch(0.6 0.2 160 / 0.2)" : "none",
              transitionProperty: "width, min-width, box-shadow",
            }}
          />
        </div>
      </div>

      {/* Speedup callout */}
      <div className="flex items-center gap-4" style={{ opacity: visible ? 1 : 0, transition: "opacity 0.6s 2.5s" }}>
        <div className="h-px flex-1" style={{ background: "linear-gradient(90deg, transparent, var(--border))" }} />
        <span className="text-2xl font-extrabold tracking-tight" style={{ color: "var(--primary)" }}>1,652x faster</span>
        <div className="h-px flex-1" style={{ background: "linear-gradient(90deg, var(--border), transparent)" }} />
      </div>
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════════════ */
/*  4. PROOF TREE — Provenance                                      */
/*  Derivation tree matching IL Studio's proof tree panel style     */
/* ═══════════════════════════════════════════════════════════════════════ */

interface PNode {
  label: string
  type: "conclusion" | "rule" | "derived" | "fact"
  children?: PNode[]
}

const proofData: PNode = {
  label: 'can_reach("new_york", "sydney")',
  type: "conclusion",
  children: [
    {
      label: "can_reach(A,C) <- direct_flight(A,B,_), can_reach(B,C)",
      type: "rule",
      children: [
        { label: 'direct_flight("new_york", "london", 7.0)', type: "fact" },
        {
          label: 'can_reach("london", "sydney")',
          type: "derived",
          children: [
            {
              label: "can_reach(A,C) <- direct_flight(A,B,_), can_reach(B,C)",
              type: "rule",
              children: [
                { label: 'direct_flight("london", "paris", 1.5)', type: "fact" },
                {
                  label: 'can_reach("paris", "sydney")',
                  type: "derived",
                  children: [
                    {
                      label: "can_reach(A,C) <- direct_flight(A,B,_), can_reach(B,C)",
                      type: "rule",
                      children: [
                        { label: 'direct_flight("paris", "tokyo", 12.0)', type: "fact" },
                        { label: 'direct_flight("tokyo", "sydney", 9.5)', type: "fact" },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
    },
  ],
}

const typeColors: Record<string, { dot: string; text: string; bg: string; label: string }> = {
  conclusion: { dot: "var(--primary)", text: "var(--primary)", bg: "oklch(0.55 0.15 260 / 0.12)", label: "conclusion" },
  rule: { dot: "oklch(0.6 0.15 250)", text: "oklch(0.7 0.12 250)", bg: "oklch(0.55 0.12 250 / 0.1)", label: "rule" },
  derived: { dot: "oklch(0.65 0.15 80)", text: "oklch(0.75 0.12 80)", bg: "oklch(0.6 0.12 80 / 0.1)", label: "derived" },
  fact: { dot: "oklch(0.6 0.18 160)", text: "oklch(0.75 0.14 160)", bg: "oklch(0.55 0.15 160 / 0.1)", label: "fact" },
}

function ProofNode({ node, depth, index, visible }: { node: PNode; depth: number; index: number; visible: boolean }) {
  const c = typeColors[node.type]
  const delay = 0.3 + (depth * 0.15) + (index * 0.1)

  return (
    <div>
      <div
        className="flex items-center gap-2.5 py-1.5 px-2 rounded-md"
        style={{
          opacity: visible ? 1 : 0,
          transform: visible ? "translateX(0)" : "translateX(-16px)",
          transition: `opacity 0.5s ${delay}s, transform 0.5s ${delay}s`,
          background: c.bg,
        }}
      >
        <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ background: c.dot, boxShadow: `0 0 8px ${c.dot}` }} />
        <span className="text-xs font-mono leading-tight" style={{ color: c.text }}>{node.label}</span>
        <span
          className="text-[9px] px-2 py-0.5 rounded-full font-semibold uppercase tracking-wider shrink-0"
          style={{ color: c.dot, background: c.bg, border: `1px solid ${c.dot}33` }}
        >
          {c.label}
        </span>
      </div>

      {node.children && (
        <div className="ml-4 border-l-2 pl-4 space-y-1 py-1" style={{ borderColor: `${c.dot}30` }}>
          {node.children.map((child, i) => (
            <ProofNode key={i} node={child} depth={depth + 1} index={i} visible={visible} />
          ))}
        </div>
      )}
    </div>
  )
}

export function ProvenanceTreeDiagram() {
  const { ref, visible } = useInView()

  return (
    <div ref={ref} className="w-full space-y-1">
      <ProofNode node={proofData} depth={0} index={0} visible={visible} />
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════════════ */
/*  VISUAL / CODE TABS                                                    */
/*  Toggle between animated diagram and syntax-highlighted code           */
/* ═══════════════════════════════════════════════════════════════════════ */

export function VisualCodeTabs({ visual, code }: { visual: ReactNode; code: string }) {
  const [tab, setTab] = useState<"visual" | "code">("visual")
  const html = highlightToHtml(code)

  return (
    <div className="space-y-3">
      <div className="flex gap-0 rounded-lg border border-border overflow-hidden w-fit">
        <button
          onClick={() => setTab("visual")}
          className={`inline-flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-medium transition-colors border-r border-border ${
            tab === "visual"
              ? "bg-primary text-primary-foreground"
              : "bg-card text-muted-foreground hover:text-foreground hover:bg-secondary/50"
          }`}
        >
          <Eye className="h-3 w-3" />
          Visual
        </button>
        <button
          onClick={() => setTab("code")}
          className={`inline-flex items-center gap-1.5 px-3.5 py-1.5 text-xs font-medium transition-colors ${
            tab === "code"
              ? "bg-primary text-primary-foreground"
              : "bg-card text-muted-foreground hover:text-foreground hover:bg-secondary/50"
          }`}
        >
          <Code className="h-3 w-3" />
          Code
        </button>
      </div>
      {tab === "visual" ? (
        <div>{visual}</div>
      ) : (
        <pre className="rounded-lg bg-[var(--code-bg)] p-4 overflow-x-auto text-sm font-mono">
          <code dangerouslySetInnerHTML={{ __html: html }} />
        </pre>
      )}
    </div>
  )
}

/* ═══════════════════════════════════════════════════════════════════════ */
/*  HERO VISUALIZATION                                                    */
/*  Animated architecture diagram: Data -> Facts -> Rules -> Derived -> AI */
/* ═══════════════════════════════════════════════════════════════════════ */

const heroFacts = [
  { text: 'direct_flight("New York", "London")', delay: 0.3 },
  { text: 'direct_flight("London", "Paris")', delay: 0.5 },
  { text: 'direct_flight("Paris", "Tokyo")', delay: 0.7 },
  { text: 'direct_flight("Tokyo", "Sydney")', delay: 0.9 },
]

const heroRules = [
  { text: "can_reach(A, B) <- direct_flight(A, B)", delay: 1.2 },
  { text: "can_reach(A, C) <- direct_flight(A, B), can_reach(B, C)", delay: 1.4 },
]

const heroDerived = [
  { text: 'can_reach("New York", "Paris")', label: "live", delay: 1.8 },
  { text: 'can_reach("New York", "Tokyo")', label: "live", delay: 2.0 },
  { text: 'can_reach("New York", "Sydney")', label: "live", delay: 2.2 },
  { text: 'can_reach("London", "Sydney")', label: "live", delay: 2.4 },
]

export function HeroVisualization() {
  const { ref, visible } = useInView(0.2)

  return (
    <div ref={ref} className="space-y-5">
      <style>{`
        @keyframes hero-pulse { 0%, 100% { opacity: 0.4; } 50% { opacity: 1; } }
        @keyframes hero-flow { from { transform: translateX(-4px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
      `}</style>

      {/* Facts layer */}
      <div>
        <div className="flex items-center gap-2 mb-2">
          <span className="w-2 h-2 rounded-full" style={{ background: "oklch(0.6 0.18 160)" }} />
          <span className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: "oklch(0.7 0.14 160)" }}>Facts</span>
          <div className="h-px flex-1" style={{ background: "oklch(0.6 0.18 160 / 0.2)" }} />
        </div>
        <div className="grid grid-cols-2 gap-1.5">
          {heroFacts.map((f) => (
            <div
              key={f.text}
              className="rounded-md px-2.5 py-1.5 text-[10px] font-mono truncate"
              style={{
                background: "oklch(0.55 0.12 160 / 0.08)",
                border: "1px solid oklch(0.6 0.15 160 / 0.15)",
                color: "oklch(0.75 0.12 160)",
                opacity: visible ? 1 : 0,
                transform: visible ? "translateX(0)" : "translateX(-8px)",
                transition: `opacity 0.4s ${f.delay}s, transform 0.4s ${f.delay}s`,
              }}
            >
              {f.text}
            </div>
          ))}
        </div>
      </div>

      {/* Arrow */}
      <div className="flex justify-center" style={{ opacity: visible ? 1 : 0, transition: "opacity 0.4s 1.1s" }}>
        <svg width="24" height="20" viewBox="0 0 24 20">
          <path d="M12 0 L12 14 M6 10 L12 16 L18 10" stroke="var(--border)" strokeWidth="1.5" fill="none" />
        </svg>
      </div>

      {/* Rules layer */}
      <div>
        <div className="flex items-center gap-2 mb-2">
          <span className="w-2 h-2 rounded-full" style={{ background: "oklch(0.6 0.15 250)" }} />
          <span className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: "oklch(0.7 0.12 250)" }}>Rules</span>
          <div className="h-px flex-1" style={{ background: "oklch(0.6 0.15 250 / 0.2)" }} />
        </div>
        <div className="space-y-1.5">
          {heroRules.map((r) => (
            <div
              key={r.text}
              className="rounded-md px-2.5 py-1.5 text-[10px] font-mono"
              style={{
                background: "oklch(0.5 0.1 250 / 0.08)",
                border: "1px solid oklch(0.55 0.12 250 / 0.15)",
                color: "oklch(0.7 0.1 250)",
                opacity: visible ? 1 : 0,
                transform: visible ? "translateX(0)" : "translateX(-8px)",
                transition: `opacity 0.4s ${r.delay}s, transform 0.4s ${r.delay}s`,
              }}
            >
              {r.text}
            </div>
          ))}
        </div>
      </div>

      {/* Arrow */}
      <div className="flex justify-center" style={{ opacity: visible ? 1 : 0, transition: "opacity 0.4s 1.6s" }}>
        <svg width="24" height="20" viewBox="0 0 24 20">
          <path d="M12 0 L12 14 M6 10 L12 16 L18 10" stroke="var(--border)" strokeWidth="1.5" fill="none" />
        </svg>
      </div>

      {/* Derived state layer */}
      <div>
        <div className="flex items-center gap-2 mb-2">
          <span className="w-2 h-2 rounded-full" style={{ background: "var(--primary)", boxShadow: "0 0 8px var(--primary)" }} />
          <span className="text-[10px] font-semibold uppercase tracking-wider text-primary">Conclusions (computed automatically)</span>
          <div className="h-px flex-1 bg-primary/20" />
        </div>
        <div className="space-y-1.5">
          {heroDerived.map((d) => (
            <div
              key={d.text}
              className="rounded-md px-2.5 py-1.5 flex items-center justify-between"
              style={{
                background: "oklch(0.5 0.12 260 / 0.1)",
                border: "1px solid oklch(0.55 0.15 260 / 0.2)",
                opacity: visible ? 1 : 0,
                transform: visible ? "translateX(0)" : "translateX(-8px)",
                transition: `opacity 0.4s ${d.delay}s, transform 0.4s ${d.delay}s`,
              }}
            >
              <span className="text-[10px] font-mono text-primary truncate">{d.text}</span>
              <span
                className="text-[8px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded-full shrink-0 ml-2"
                style={{
                  background: "oklch(0.6 0.18 160 / 0.15)",
                  color: "oklch(0.75 0.15 160)",
                  animation: visible ? `hero-pulse 2s ease-in-out ${d.delay + 0.5}s infinite` : "none",
                }}
              >
                {d.label}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Badge strip */}
      <div className="flex justify-center pt-1" style={{ opacity: visible ? 1 : 0, transition: "opacity 0.5s 2.6s" }}>
        <div className="inline-flex items-center gap-2 border border-primary/20 rounded-full px-3.5 py-1.5">
          <div className="w-[5px] h-[5px] rounded-full bg-primary animate-pulse flex-shrink-0" />
          <span className="font-mono text-[11px] text-primary uppercase tracking-[0.09em]">
            Live · Reliable · Traceable · Explainable
          </span>
        </div>
      </div>

    </div>
  )
}
