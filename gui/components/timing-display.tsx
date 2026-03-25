"use client"

import { useState } from "react"
import type { WsTimingBreakdown } from "@/lib/ws-types"
import { ChevronDown, Info, Activity } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

// ── Shared constants ────────────────────────────────────────────────────────

export const PIPELINE_STAGES = [
  { key: "parse_us" as const, label: "Parse", color: "#10b981" },
  { key: "sip_us" as const, label: "SIP", color: "#3b82f6" },
  { key: "magic_sets_us" as const, label: "Magic Sets", color: "#8b5cf6" },
  { key: "ir_build_us" as const, label: "IR Build", color: "#f59e0b" },
  { key: "optimize_us" as const, label: "Optimize", color: "#06b6d4" },
  { key: "shared_views_us" as const, label: "Shared Views", color: "#ec4899" },
] as const

export const EXECUTION_COLOR = "#f97316"

// ── Shared formatters ───────────────────────────────────────────────────────

export function formatUs(us: number): string {
  if (us >= 1_000_000) return `${(us / 1_000_000).toFixed(1)}s`
  if (us >= 1000) return `${(us / 1000).toFixed(1)}ms`
  return `${us}us`
}

export function formatTimingSummary(tb: WsTimingBreakdown): string {
  const parts: string[] = []
  if (tb.parse_us > 0) parts.push(`parse: ${formatUs(tb.parse_us)}`)
  if (tb.sip_us > 0) parts.push(`sip: ${formatUs(tb.sip_us)}`)
  if (tb.magic_sets_us > 0) parts.push(`magic: ${formatUs(tb.magic_sets_us)}`)
  if (tb.ir_build_us > 0) parts.push(`ir: ${formatUs(tb.ir_build_us)}`)
  if (tb.optimize_us > 0) parts.push(`opt: ${formatUs(tb.optimize_us)}`)
  if (tb.shared_views_us > 0) parts.push(`shared: ${formatUs(tb.shared_views_us)}`)
  const ruleTotal = tb.rules?.reduce((sum, r) => sum + r.execution_us, 0) ?? 0
  if (ruleTotal > 0) parts.push(`exec: ${formatUs(ruleTotal)}`)
  return parts.join(", ")
}

// ── Tooltip ─────────────────────────────────────────────────────────────────

export function Tip({ children }: { children: React.ReactNode }) {
  const [show, setShow] = useState(false)
  return (
    <span className="relative inline-flex items-center ml-1.5">
      <Info className="h-3 w-3 text-muted-foreground/40 hover:text-muted-foreground cursor-help translate-y-px"
        onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)} />
      {show && (
        <span className="fixed z-[99999] w-60 rounded-md border border-border bg-popover px-3 py-2 text-[11px] text-popover-foreground shadow-lg leading-relaxed pointer-events-none"
          ref={(el) => {
            if (!el) return
            const icon = el.parentElement?.querySelector("svg")
            if (!icon) return
            const r = icon.getBoundingClientRect()
            el.style.left = `${Math.max(8, Math.min(r.left + r.width / 2 - 120, window.innerWidth - 248))}px`
            el.style.top = `${r.top - el.offsetHeight - 6}px`
          }}>
          {children}
        </span>
      )}
    </span>
  )
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function buildStages(tb: WsTimingBreakdown) {
  const ruleTotal = tb.rules?.reduce((sum, r) => sum + r.execution_us, 0) ?? 0
  const stages = [
    ...PIPELINE_STAGES.map((s) => ({ label: s.label, color: s.color, us: tb[s.key] })),
    { label: "Execution", color: EXECUTION_COLOR, us: ruleTotal },
  ]
  const totalUs = tb.total_us || stages.reduce((sum, s) => sum + s.us, 0)
  return { stages, totalUs }
}

// ── Waterfall bar ───────────────────────────────────────────────────────────

export function WaterfallBar({ tb, height = "h-6" }: { tb: WsTimingBreakdown; height?: string }) {
  const { stages, totalUs } = buildStages(tb)
  return (
    <div>
      <div className={cn(height, "rounded-lg overflow-hidden flex bg-muted/30 border border-border/50")}>
        {stages.filter((s) => s.us > 0).map((s) => (
          <div key={s.label}
            title={`${s.label}: ${formatUs(s.us)} (${((s.us / totalUs) * 100).toFixed(1)}%)`}
            style={{ width: `${(s.us / totalUs) * 100}%`, minWidth: "2px", backgroundColor: s.color }}
            className="h-full hover:opacity-80 transition-opacity" />
        ))}
      </div>
      <div className="flex flex-wrap gap-2 mt-1.5">
        {stages.filter((s) => s.us > 0).map((s) => (
          <span key={s.label} className="flex items-center gap-1">
            <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: s.color }} />
            <span className="text-[10px] text-muted-foreground">{s.label}</span>
          </span>
        ))}
      </div>
    </div>
  )
}

// ── Mini waterfall (for inline summaries) ───────────────────────────────────

export function MiniWaterfall({ tb }: { tb: WsTimingBreakdown }) {
  const { stages, totalUs } = buildStages(tb)
  return (
    <div className="h-2 rounded overflow-hidden flex bg-muted/30">
      {stages.filter((s) => s.us > 0).map((s, i) => (
        <div key={i} style={{ width: `${(s.us / totalUs) * 100}%`, minWidth: "1px", backgroundColor: s.color }} className="h-full" />
      ))}
    </div>
  )
}

// ── Stage table ─────────────────────────────────────────────────────────────

export function StageTable({ tb }: { tb: WsTimingBreakdown }) {
  const { stages, totalUs } = buildStages(tb)
  const nonZero = stages.filter((s) => s.us > 0).sort((a, b) => b.us - a.us)

  return (
    <div className="rounded-lg border border-border/50 overflow-hidden">
      <table className="w-full text-xs">
        <thead>
          <tr className="bg-muted/40">
            <th className="px-3 py-1.5 text-left text-[10px] font-medium uppercase tracking-wider text-muted-foreground">Stage</th>
            <th className="px-3 py-1.5 text-right text-[10px] font-medium uppercase tracking-wider text-muted-foreground">Duration</th>
            <th className="px-3 py-1.5 text-right text-[10px] font-medium uppercase tracking-wider text-muted-foreground">%</th>
          </tr>
        </thead>
        <tbody>
          {nonZero.map((s) => (
            <tr key={s.label} className="border-t border-border/20 hover:bg-muted/20 transition-colors">
              <td className="px-3 py-1"><span className="flex items-center gap-1.5"><span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: s.color }} />{s.label}</span></td>
              <td className="px-3 py-1 text-right font-mono">{formatUs(s.us)}</td>
              <td className="px-3 py-1 text-right font-mono text-muted-foreground">{((s.us / totalUs) * 100).toFixed(1)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Per-rule breakdown ──────────────────────────────────────────────────────

export function RuleBreakdown({ tb }: { tb: WsTimingBreakdown }) {
  const [open, setOpen] = useState(false)
  const sortedRules = tb.rules ? [...tb.rules].sort((a, b) => b.execution_us - a.execution_us) : []
  const totalUs = tb.total_us || 1

  if (sortedRules.length === 0) return null

  return (
    <div className="rounded-lg border border-border/50 overflow-hidden">
      <button onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-2 px-3 py-2 bg-muted/30 hover:bg-muted/50 transition-colors text-xs font-medium">
        <ChevronDown className={cn("h-3 w-3 text-muted-foreground transition-transform", !open && "-rotate-90")} />
        <span>Per-Rule Breakdown ({sortedRules.length} rules)</span>
      </button>
      {open && (
        <div className="divide-y divide-border/20">
          {sortedRules.map((rule, i) => (
            <div key={i} className="flex items-center justify-between px-3 py-1.5 hover:bg-muted/20 transition-colors text-xs">
              <span className="flex items-center gap-1.5 min-w-0">
                <span className="font-mono truncate">{rule.rule_head}</span>
                {rule.is_recursive && <Badge variant="outline" className="text-[9px] px-1 py-0 border-amber-500/50 text-amber-600 dark:text-amber-400">rec</Badge>}
                {rule.workers > 1 && <Badge variant="outline" className="text-[9px] px-1 py-0 border-blue-500/50 text-blue-600 dark:text-blue-400">{rule.workers}w</Badge>}
              </span>
              <div className="flex items-center gap-3 flex-shrink-0 ml-2">
                <span className="font-mono">{formatUs(rule.execution_us)}</span>
                <span className="font-mono text-muted-foreground w-12 text-right">{((rule.execution_us / totalUs) * 100).toFixed(1)}%</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Recursive callout ───────────────────────────────────────────────────────

export function RecursiveCallout({ tb }: { tb: WsTimingBreakdown }) {
  const recursiveRules = tb.rules?.filter(r => r.is_recursive) ?? []
  if (recursiveRules.length === 0) return null
  const cost = recursiveRules.reduce((s, r) => s + r.execution_us, 0)

  return (
    <div className="flex items-center gap-2 rounded-md border border-amber-500/30 bg-amber-500/5 px-3 py-2">
      <Activity className="h-3.5 w-3.5 text-amber-500 flex-shrink-0" />
      <p className="text-[11px] text-muted-foreground">
        <span className="font-medium text-amber-700 dark:text-amber-400">Fixpoint iteration</span> - recursive execution took {formatUs(cost)}
      </p>
    </div>
  )
}

// ── Full timing display (used by both query results and view performance) ───

export function TimingDisplay({ tb, executionTimeMs, rowCount }: {
  tb: WsTimingBreakdown
  executionTimeMs?: number
  rowCount?: number
}) {
  return (
    <div className="space-y-4">
      {/* Summary */}
      <div className="flex items-baseline gap-6 flex-wrap">
        <div>
          <span className="text-2xl font-bold font-mono tabular-nums">{formatUs(tb.total_us)}</span>
          <span className="text-xs text-muted-foreground ml-1.5">
            engine
            <Tip>Time inside the Datalog engine: parse, rewrite, optimize, and execute.</Tip>
          </span>
        </div>
        {rowCount !== undefined && (
          <span className="text-xs text-muted-foreground"><span className="font-mono">{rowCount.toLocaleString()}</span> rows</span>
        )}
        {executionTimeMs !== undefined && (
          <span className="text-xs text-muted-foreground">
            <span className="font-mono">{executionTimeMs}ms</span> wall clock
            <Tip>Includes handler overhead and result serialization beyond engine computation.</Tip>
          </span>
        )}
      </div>

      <RecursiveCallout tb={tb} />
      <WaterfallBar tb={tb} />
      <StageTable tb={tb} />
      <RuleBreakdown tb={tb} />
    </div>
  )
}
