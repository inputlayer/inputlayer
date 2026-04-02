"use client"

import { useState } from "react"
import type { View } from "@/lib/datalog-store"
import { useDatalogStore } from "@/lib/datalog-store"
import {
  TimingDisplay, MiniWaterfall, formatUs, Tip, PIPELINE_STAGES, EXECUTION_COLOR,
} from "@/components/timing-display"
import {
  GitBranch,
  Code,
  Eye,
  FileText,
  ChevronDown,
  ChevronRight,
  Scan,
  Merge,
  Filter,
  Layers,
  Sparkles,
  Cpu,
  Zap,
  Clock,
  Rows3,
  ArrowRight,
  Database,
  Activity,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

// ── Local helpers ───────────────────────────────────────────────────────────

function timeAgo(date: Date): string {
  const seconds = Math.floor((Date.now() - date.getTime()) / 1000)
  if (seconds < 5) return "just now"
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  return `${Math.floor(seconds / 3600)}h ago`
}

// ── Sub-tab button ──────────────────────────────────────────────────────────

function SubTab({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "px-3 py-1.5 text-xs font-medium rounded-md transition-colors",
        active ? "bg-foreground/10 text-foreground" : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
      )}
    >
      {children}
    </button>
  )
}

// ── Sub-tab: Timing ─────────────────────────────────────────────────────────

function TimingSection({ view }: { view: View }) {
  const bm = view.benchmark

  if (!bm) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <Clock className="h-8 w-8 text-muted-foreground/30 mb-3" />
        <p className="text-sm text-muted-foreground">No timing data available</p>
        <p className="text-xs text-muted-foreground/60 mt-1 max-w-xs">
          Timing is captured when this view is loaded. Make sure timing_mode is enabled in server config.
        </p>
      </div>
    )
  }

  return (
    <div className="p-4">
      <TimingDisplay tb={bm.timingBreakdown} executionTimeMs={bm.executionTimeMs} rowCount={bm.rowCount} />
    </div>
  )
}

// ── Sub-tab: Inputs ─────────────────────────────────────────────────────────

function InputsSection({ view }: { view: View }) {
  const relations = useDatalogStore((s) => s.relations)
  const recentMutations = useDatalogStore((s) => s.recentMutations)
  const isRecursive = view.dependencies.includes(view.name)
  const bm = view.benchmark

  const depInfo = view.dependencies
    .filter((d) => d !== view.name)
    .map((depName) => {
      const rel = relations.find((r) => r.name === depName)
      const mutation = recentMutations.get(depName)
      return { name: depName, tupleCount: rel?.tupleCount ?? 0, lastMutation: mutation }
    })

  if (depInfo.length === 0 && !isRecursive) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <Database className="h-8 w-8 text-muted-foreground/30 mb-3" />
        <p className="text-sm text-muted-foreground">No input dependencies</p>
      </div>
    )
  }

  return (
    <div className="p-4 space-y-4">
      {/* Data flow visualization */}
      <div className="flex items-center gap-1.5 flex-wrap">
        {depInfo.map((dep, i) => (
          <span key={dep.name} className="flex items-center gap-1.5">
            {i > 0 && <span className="text-muted-foreground/30 text-xs">+</span>}
            <span className="inline-flex items-center gap-1 rounded-md border border-border/60 bg-muted/30 px-2 py-0.5 font-mono text-[11px]">
              <Database className="h-3 w-3 text-muted-foreground/60" />
              {dep.name}
              <span className="text-muted-foreground/50">({dep.tupleCount.toLocaleString()})</span>
            </span>
          </span>
        ))}
        {isRecursive && (
          <span className="flex items-center gap-1.5">
            {depInfo.length > 0 && <span className="text-muted-foreground/30 text-xs">+</span>}
            <span className="inline-flex items-center gap-1 rounded-md border border-amber-500/40 bg-amber-500/5 px-2 py-0.5 font-mono text-[11px] text-amber-700 dark:text-amber-400">
              <Activity className="h-3 w-3" />{view.name}<span className="opacity-50">(self)</span>
            </span>
          </span>
        )}
        <ArrowRight className="h-3.5 w-3.5 text-muted-foreground/40 mx-1" />
        <span className="inline-flex items-center gap-1 rounded-md border border-chart-1/40 bg-chart-1/5 px-2 py-0.5 font-mono text-[11px] font-medium">
          {view.name}
          {bm && <span className="text-muted-foreground/50">({bm.rowCount.toLocaleString()})</span>}
        </span>
      </div>

      {/* Detail list */}
      <div className="rounded-lg border border-border/50 overflow-hidden divide-y divide-border/30">
        {depInfo.map((dep) => (
          <div key={dep.name} className="flex items-center justify-between px-4 py-2.5 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2 min-w-0">
              <div className="h-2 w-2 rounded-full bg-chart-1 flex-shrink-0" />
              <span className="font-mono text-xs">{dep.name}</span>
              <span className="text-[10px] text-muted-foreground">{dep.tupleCount.toLocaleString()} facts</span>
            </div>
            {dep.lastMutation ? (
              <span className={cn(
                "flex items-center gap-1 text-[10px] flex-shrink-0 ml-2",
                dep.lastMutation.operation === "insert" ? "text-emerald-600 dark:text-emerald-400" : "text-red-500"
              )}>
                <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: dep.lastMutation.operation === "insert" ? "#10b981" : "#ef4444" }} />
                {dep.lastMutation.operation === "insert" ? "+" : "\u2212"}{dep.lastMutation.count} {timeAgo(dep.lastMutation.timestamp)}
              </span>
            ) : (
              <span className="text-[10px] text-muted-foreground/40 flex-shrink-0 ml-2">no recent changes</span>
            )}
          </div>
        ))}
      </div>

      {isRecursive && (
        <div className="flex items-start gap-2 rounded-md border border-amber-500/30 bg-amber-500/5 px-3 py-2">
          <Activity className="h-3.5 w-3.5 text-amber-500 flex-shrink-0 mt-0.5" />
          <p className="text-[11px] text-muted-foreground">
            <span className="font-medium text-amber-700 dark:text-amber-400">Recursive</span> - {view.name} depends on itself.
            Evaluation iterates until no new facts are derived (fixpoint).
          </p>
        </div>
      )}
    </div>
  )
}

// ── Sub-tab: Query Plan ─────────────────────────────────────────────────────

interface PlanSection { title: string; content: string[] }
interface IRNodeDef { type: string; text: string; children: string[] }

function parsePipelineTrace(plan: string): PlanSection[] {
  const sections: PlanSection[] = []
  const lines = plan.split("\n")
  let current: PlanSection | null = null
  for (const line of lines) {
    const t = line.trim()
    if (t.startsWith("| ") && t.endsWith("|")) {
      const title = t.replace(/^\|\s*/, "").replace(/\s*\|$/, "").trim()
      if (title && !title.startsWith("-") && !title.startsWith("`")) { current = { title, content: [] }; sections.push(current); continue }
    }
    if (t.startsWith("===") || t.startsWith("---") || t.startsWith("\u250c") || t.startsWith("`") || t === "PIPELINE TRACE" || t === "") continue
    if (current) current.content.push(line)
  }
  return sections
}

function parseIRNodes(lines: string[]): IRNodeDef[] {
  const nodes: IRNodeDef[] = []
  let current: IRNodeDef | null = null
  const pfx = ["Scan","Map","Filter","Join","Antijoin","Distinct","Union","Aggregate","Compute","FlatMap","JoinFlatMap","HnswScan"]
  for (const line of lines) {
    const t = line.trim()
    if (!t) continue
    if (t.match(/^Rule \d+/)) { if (current) nodes.push(current); current = { type: "rule_header", text: t.replace(/:$/, ""), children: [] }; continue }
    if (current && pfx.some(p => t.startsWith(p))) { current.children.push(t); continue }
    if (current) current.children.push(t); else nodes.push({ type: "info", text: t, children: [] })
  }
  if (current) nodes.push(current)
  return nodes
}

function irNodeIcon(text: string) {
  if (text.startsWith("Scan")) return <Scan className="h-3 w-3 text-emerald-500" />
  if (text.startsWith("Join") || text.startsWith("JoinFlatMap")) return <Merge className="h-3 w-3 text-blue-500" />
  if (text.startsWith("Filter")) return <Filter className="h-3 w-3 text-amber-500" />
  if (text.startsWith("Aggregate")) return <Layers className="h-3 w-3 text-purple-500" />
  if (text.startsWith("Antijoin")) return <Merge className="h-3 w-3 text-red-400" />
  if (text.startsWith("Distinct")) return <Sparkles className="h-3 w-3 text-cyan-500" />
  if (text.startsWith("HnswScan")) return <Zap className="h-3 w-3 text-purple-500" />
  return <Cpu className="h-3 w-3 text-muted-foreground" />
}

function irNodeColor(text: string): string {
  if (text.startsWith("Scan")) return "emerald"
  if (text.startsWith("Join") || text.startsWith("JoinFlatMap")) return "blue"
  if (text.startsWith("Filter")) return "amber"
  if (text.startsWith("Aggregate")) return "purple"
  if (text.startsWith("Antijoin")) return "red"
  if (text.startsWith("HnswScan")) return "purple"
  return "slate"
}

function QueryPlanSection({ view }: { view: View }) {
  const planSections = view.debugPlan ? parsePipelineTrace(view.debugPlan) : []

  if (!view.debugPlan) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <Code className="h-8 w-8 text-muted-foreground/30 mb-3" />
        <p className="text-sm text-muted-foreground">No query plan available</p>
      </div>
    )
  }

  if (planSections.length === 0) {
    return (
      <div className="p-4">
        <pre className="rounded-md bg-muted/30 p-3 font-mono text-xs text-foreground overflow-x-auto whitespace-pre-wrap">{view.debugPlan}</pre>
      </div>
    )
  }

  return (
    <div className="p-4 space-y-2">
      {planSections.map((section, i) => {
        const [open, setOpen] = useState(true)
        const irNodes = parseIRNodes(section.content)
        const isOpt = section.title === "OPTIMIZATION"
        const isParsing = section.title === "PARSING"
        return (
          <div key={i} className="rounded-lg border border-border/50 overflow-hidden">
            <div className="flex items-center gap-2 border-b border-border/40 bg-muted/30 px-3 py-2 cursor-pointer hover:bg-muted/50 transition-colors" onClick={() => setOpen(!open)}>
              {open ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground/60" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/60" />}
              <span className="text-xs font-medium tracking-wider text-foreground/80">{section.title}</span>
              {isOpt && <Badge variant="secondary" className="h-4 px-1.5 text-[10px]"><Sparkles className="mr-1 h-2.5 w-2.5" />optimized</Badge>}
            </div>
            {open && (
              <div className="p-3 space-y-1">
                {irNodes.map((node, ni) => (
                  <div key={ni}>
                    {node.type === "rule_header" ? <p className="text-xs font-medium text-foreground/70 mb-1 mt-2 first:mt-0">{node.text}</p>
                     : node.type === "info" ? <p className="font-mono text-xs text-muted-foreground">{node.text}</p> : null}
                    {node.children.map((child, ci) => {
                      const color = irNodeColor(child)
                      return (
                        <div key={ci} className="flex items-center gap-2 rounded-md border-l-2 px-2.5 py-1 ml-2"
                          style={{ borderLeftColor: `var(--color-${color}-500, oklch(0.7 0.15 160))`, backgroundColor: `color-mix(in oklch, var(--color-${color}-500, oklch(0.7 0.15 160)) 3%, transparent)` }}>
                          {irNodeIcon(child)}
                          <span className="font-mono text-xs text-foreground/85">{child}</span>
                        </div>
                      )
                    })}
                  </div>
                ))}
                {isParsing && irNodes.length === 0 && section.content.length > 0 && (
                  <div className="space-y-0.5">{section.content.filter(l => l.trim()).map((line, li) => (
                    <p key={li} className="font-mono text-xs text-foreground/80 pl-2">{line.trim()}</p>
                  ))}</div>
                )}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ── Sub-tab: Rule Definition ────────────────────────────────────────────────

function RuleSection({ view }: { view: View }) {
  if (!view.definition) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <FileText className="h-8 w-8 text-muted-foreground/30 mb-3" />
        <p className="text-sm text-muted-foreground">No definition available</p>
      </div>
    )
  }

  return (
    <div className="p-4 space-y-1">
      {view.definition.split("\n").filter(l => l.trim()).map((line, i) => (
        <div key={i} className="flex items-center gap-2 rounded-md border-l-2 border-l-blue-500/40 px-2.5 py-1.5"
          style={{ backgroundColor: "color-mix(in oklch, var(--color-blue-500, oklch(0.7 0.15 260)) 3%, transparent)" }}>
          <GitBranch className="h-3 w-3 text-blue-500 flex-shrink-0" />
          <span className="font-mono text-xs text-foreground/90">{line.trim()}</span>
        </div>
      ))}
    </div>
  )
}

// ── Main component ──────────────────────────────────────────────────────────

export function ViewPerformanceTab({ view }: { view: View }) {
  const [subTab, setSubTab] = useState<"overview" | "timing" | "inputs" | "plan" | "rule">("overview")
  const isRecursive = view.dependencies.includes(view.name)
  const clauseCount = view.computationSteps.length
  const depCount = view.dependencies.filter(d => d !== view.name).length
  const bm = view.benchmark

  return (
    <div className="flex h-full flex-col overflow-hidden">
      {/* Sub-tab bar */}
      <div className="flex items-center gap-1 px-4 py-2 border-b border-border/50 flex-shrink-0 bg-muted/10">
        <SubTab active={subTab === "overview"} onClick={() => setSubTab("overview")}>Overview</SubTab>
        <SubTab active={subTab === "timing"} onClick={() => setSubTab("timing")}>Timing</SubTab>
        <SubTab active={subTab === "inputs"} onClick={() => setSubTab("inputs")}>Inputs</SubTab>
        <SubTab active={subTab === "plan"} onClick={() => setSubTab("plan")}>Query Plan</SubTab>
        <SubTab active={subTab === "rule"} onClick={() => setSubTab("rule")}>Rule</SubTab>
      </div>

      {/* Sub-tab content */}
      <div className="flex-1 overflow-auto">
        {subTab === "overview" && (
          <div className="p-4 space-y-4">
            {/* Characteristics */}
            <div className="grid gap-3 grid-cols-3">
              <div className="rounded-lg border border-border/50 bg-muted/20 p-3">
                <div className="flex items-center gap-1.5 text-muted-foreground mb-1">
                  <Database className="h-3.5 w-3.5" />
                  <span className="text-[10px] font-medium uppercase tracking-wider">Inputs</span>
                </div>
                <p className="text-xl font-bold tabular-nums">{depCount}{isRecursive ? "+self" : ""}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">{depCount === 1 ? "relation" : "relations"}</p>
              </div>
              <div className="rounded-lg border border-border/50 bg-muted/20 p-3">
                <div className="flex items-center gap-1.5 text-muted-foreground mb-1">
                  <Code className="h-3.5 w-3.5" />
                  <span className="text-[10px] font-medium uppercase tracking-wider">Clauses</span>
                </div>
                <p className="text-xl font-bold tabular-nums">{clauseCount}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">{clauseCount === 1 ? "clause" : "clauses"}</p>
              </div>
              <div className="rounded-lg border border-border/50 bg-muted/20 p-3">
                <div className="flex items-center gap-1.5 text-muted-foreground mb-1">
                  <Eye className="h-3.5 w-3.5" />
                  <span className="text-[10px] font-medium uppercase tracking-wider">Evaluation</span>
                </div>
                <p className="text-xl font-bold">{isRecursive ? "Fixpoint" : "Single-pass"}</p>
                <p className="text-[10px] text-muted-foreground mt-0.5">{isRecursive ? "iterative" : "direct"}</p>
              </div>
            </div>

            {/* Last evaluation timing (compact) */}
            {bm && (
              <div className="rounded-lg border border-border/50 p-3">
                <div className="flex items-center gap-2 mb-2">
                  <Clock className="h-3.5 w-3.5 text-muted-foreground" />
                  <span className="text-xs font-medium">Last Evaluation</span>
                  <span className="text-[10px] text-muted-foreground ml-auto">{timeAgo(bm.benchmarkedAt)}</span>
                </div>
                <div className="flex items-baseline gap-4">
                  <span className="text-lg font-bold font-mono tabular-nums">{formatUs(bm.timingBreakdown.total_us)}</span>
                  <span className="text-xs text-muted-foreground"><span className="font-mono">{bm.rowCount.toLocaleString()}</span> rows</span>
                  <span className="text-xs text-muted-foreground"><span className="font-mono">{bm.executionTimeMs}ms</span> wall clock</span>
                </div>
                <div className="mt-2">
                  <MiniWaterfall tb={bm.timingBreakdown} />
                </div>
                <button onClick={() => setSubTab("timing")} className="text-[10px] text-chart-2 hover:underline mt-1">
                  View full breakdown
                </button>
              </div>
            )}

            {/* Rule definition preview */}
            {view.definition && (
              <div className="rounded-lg border border-border/50 p-3">
                <div className="flex items-center gap-2 mb-2">
                  <FileText className="h-3.5 w-3.5 text-muted-foreground" />
                  <span className="text-xs font-medium">Rule Definition</span>
                </div>
                <pre className="font-mono text-xs text-foreground/80 whitespace-pre-wrap leading-relaxed">{view.definition}</pre>
              </div>
            )}
          </div>
        )}

        {subTab === "timing" && <TimingSection view={view} />}
        {subTab === "inputs" && <InputsSection view={view} />}
        {subTab === "plan" && <QueryPlanSection view={view} />}
        {subTab === "rule" && <RuleSection view={view} />}
      </div>
    </div>
  )
}
