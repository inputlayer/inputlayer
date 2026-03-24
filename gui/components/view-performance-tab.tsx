"use client"

import { useState } from "react"
import type { View } from "@/lib/datalog-store"
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
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

interface ViewPerformanceTabProps {
  view: View
}

interface PlanSection {
  title: string
  content: string[]
}

interface IRNode {
  type: string
  text: string
  children: string[]
}

function parsePipelineTrace(plan: string): PlanSection[] {
  const sections: PlanSection[] = []
  const lines = plan.split("\n")
  let current: PlanSection | null = null

  for (const line of lines) {
    const trimmed = line.trim()

    // Section header detection (box-drawn headers)
    if (trimmed.startsWith("| ") && trimmed.endsWith("|")) {
      const title = trimmed.replace(/^\|\s*/, "").replace(/\s*\|$/, "").trim()
      if (title && !title.startsWith("-") && !title.startsWith("`")) {
        current = { title, content: [] }
        sections.push(current)
        continue
      }
    }

    // Skip decorative lines
    if (
      trimmed.startsWith("===") ||
      trimmed.startsWith("---") ||
      trimmed.startsWith("┌") ||
      trimmed.startsWith("`") ||
      trimmed === "PIPELINE TRACE" ||
      trimmed === ""
    ) {
      continue
    }

    if (current) {
      current.content.push(line)
    }
  }

  return sections
}

function parseIRNodes(lines: string[]): IRNode[] {
  const nodes: IRNode[] = []
  let current: IRNode | null = null

  for (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed) continue

    // Rule header: "Rule N IR:" or "Rule N Optimized IR:"
    if (trimmed.match(/^Rule \d+/)) {
      if (current) nodes.push(current)
      current = { type: "rule_header", text: trimmed.replace(/:$/, ""), children: [] }
      continue
    }

    // IR node lines
    if (current && (trimmed.startsWith("Scan") || trimmed.startsWith("Map") ||
        trimmed.startsWith("Filter") || trimmed.startsWith("Join") ||
        trimmed.startsWith("Antijoin") || trimmed.startsWith("Distinct") ||
        trimmed.startsWith("Union") || trimmed.startsWith("Aggregate") ||
        trimmed.startsWith("Compute") || trimmed.startsWith("FlatMap") ||
        trimmed.startsWith("JoinFlatMap") || trimmed.startsWith("HnswScan"))) {
      current.children.push(trimmed)
      continue
    }

    // Other content lines
    if (current) {
      current.children.push(trimmed)
    } else {
      nodes.push({ type: "info", text: trimmed, children: [] })
    }
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

function PlanSectionView({ section }: { section: PlanSection }) {
  const [open, setOpen] = useState(true)
  const irNodes = parseIRNodes(section.content)
  const isOptimization = section.title === "OPTIMIZATION"
  const isParsing = section.title === "PARSING"

  return (
    <div className="rounded-lg border border-border/50 overflow-hidden">
      <div
        className="flex items-center gap-2 border-b border-border/40 bg-muted/30 px-3 py-2 cursor-pointer hover:bg-muted/50 transition-colors"
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground/60" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/60" />
        )}
        <span className="text-xs font-medium tracking-wider text-foreground/80">
          {section.title}
        </span>
        {isOptimization && (
          <Badge variant="secondary" className="h-4 px-1.5 text-[10px]">
            <Sparkles className="mr-1 h-2.5 w-2.5" />
            optimized
          </Badge>
        )}
      </div>
      {open && (
        <div className="p-3 space-y-1">
          {irNodes.map((node, i) => (
            <div key={i}>
              {node.type === "rule_header" ? (
                <p className="text-xs font-medium text-foreground/70 mb-1 mt-2 first:mt-0">{node.text}</p>
              ) : node.type === "info" ? (
                <p className="font-mono text-xs text-muted-foreground">{node.text}</p>
              ) : null}
              {node.children.map((child, ci) => {
                const color = irNodeColor(child)
                return (
                  <div
                    key={ci}
                    className="flex items-center gap-2 rounded-md border-l-2 px-2.5 py-1 ml-2"
                    style={{
                      borderLeftColor: `var(--color-${color}-500, oklch(0.7 0.15 160))`,
                      backgroundColor: `color-mix(in oklch, var(--color-${color}-500, oklch(0.7 0.15 160)) 3%, transparent)`,
                    }}
                  >
                    {irNodeIcon(child)}
                    <span className="font-mono text-xs text-foreground/85">{child}</span>
                  </div>
                )
              })}
            </div>
          ))}
          {isParsing && irNodes.length === 0 && section.content.length > 0 && (
            <div className="space-y-0.5">
              {section.content.filter(l => l.trim()).map((line, i) => (
                <p key={i} className="font-mono text-xs text-foreground/80 pl-2">{line.trim()}</p>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export function ViewPerformanceTab({ view }: ViewPerformanceTabProps) {
  const clauseCount = view.computationSteps.length
  const isRecursive = view.dependencies.includes(view.name)
  const dependencyCount = view.dependencies.length
  const planSections = view.explainPlan ? parsePipelineTrace(view.explainPlan) : []

  return (
    <div className="h-full overflow-auto p-4 space-y-6">
      {/* Metric cards */}
      <div className="grid gap-4 md:grid-cols-3">
        <div className="rounded-lg border border-border/50 bg-muted/20 p-4">
          <div className="flex items-center gap-2 text-muted-foreground mb-2">
            <GitBranch className="h-4 w-4" />
            <span className="text-xs font-medium uppercase tracking-wider">Dependencies</span>
          </div>
          <p className="text-2xl font-bold tabular-nums">{dependencyCount}</p>
          <p className="text-xs text-muted-foreground mt-1">
            {dependencyCount === 0 ? "No dependencies" : dependencyCount === 1 ? "relation" : "relations"}
          </p>
        </div>

        <div className="rounded-lg border border-border/50 bg-muted/20 p-4">
          <div className="flex items-center gap-2 text-muted-foreground mb-2">
            <Code className="h-4 w-4" />
            <span className="text-xs font-medium uppercase tracking-wider">Clauses</span>
          </div>
          <p className="text-2xl font-bold tabular-nums">{clauseCount}</p>
          <p className="text-xs text-muted-foreground mt-1">{clauseCount === 1 ? "clause" : "clauses"}</p>
        </div>

        <div className="rounded-lg border border-border/50 bg-muted/20 p-4">
          <div className="flex items-center gap-2 text-muted-foreground mb-2">
            <Eye className="h-4 w-4" />
            <span className="text-xs font-medium uppercase tracking-wider">Type</span>
          </div>
          <p className="text-2xl font-bold">{isRecursive ? "Recursive" : "Standard"}</p>
          <p className="text-xs text-muted-foreground mt-1">
            {isRecursive ? "Self-referential rule" : "Non-recursive rule"}
          </p>
        </div>
      </div>

      {/* Dependencies */}
      {dependencyCount > 0 && (
        <div className="rounded-lg border border-border/50">
          <div className="border-b border-border/50 px-4 py-3">
            <h3 className="text-sm font-medium">Dependency Analysis</h3>
            <p className="text-xs text-muted-foreground mt-0.5">Relations this rule depends on</p>
          </div>
          <div className="p-4">
            <div className="flex flex-wrap gap-2">
              {view.dependencies.map((dep) => (
                <div
                  key={dep}
                  className="flex items-center gap-2 rounded-md border border-border/50 bg-muted/30 px-3 py-1.5"
                >
                  <div className={cn("h-2 w-2 rounded-full", dep === view.name ? "bg-amber-500" : "bg-chart-1")} />
                  <span className="font-mono text-xs">{dep}</span>
                  {dep === view.name && <span className="text-[10px] text-amber-500">(self)</span>}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Query Plan - structured */}
      {planSections.length > 0 && (
        <div>
          <div className="mb-3">
            <h3 className="text-sm font-medium">Query Plan</h3>
            <p className="text-xs text-muted-foreground mt-0.5">Pipeline stages from the query optimizer</p>
          </div>
          <div className="space-y-2">
            {planSections.map((section, i) => (
              <PlanSectionView key={i} section={section} />
            ))}
          </div>
        </div>
      )}

      {/* Fallback: raw plan if parsing produced no sections */}
      {view.explainPlan && planSections.length === 0 && (
        <div className="rounded-lg border border-border/50">
          <div className="border-b border-border/50 px-4 py-3">
            <h3 className="text-sm font-medium">Query Plan</h3>
          </div>
          <div className="p-4">
            <pre className="rounded-md bg-muted/30 p-3 font-mono text-xs text-foreground overflow-x-auto whitespace-pre-wrap">
              {view.explainPlan}
            </pre>
          </div>
        </div>
      )}

      {/* Rule definition */}
      <div className="rounded-lg border border-border/50">
        <div className="border-b border-border/50 px-4 py-3">
          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4 text-muted-foreground" />
            <h3 className="text-sm font-medium">Rule Definition</h3>
          </div>
          <p className="text-xs text-muted-foreground mt-0.5">Clauses defining this rule</p>
        </div>
        <div className="p-4">
          {view.definition ? (
            <div className="space-y-1">
              {view.definition.split("\n").filter(l => l.trim()).map((line, i) => (
                <div
                  key={i}
                  className="flex items-center gap-2 rounded-md border-l-2 border-l-blue-500/40 px-2.5 py-1.5"
                  style={{
                    backgroundColor: "color-mix(in oklch, var(--color-blue-500, oklch(0.7 0.15 260)) 3%, transparent)",
                  }}
                >
                  <GitBranch className="h-3 w-3 text-blue-500 flex-shrink-0" />
                  <span className="font-mono text-xs text-foreground/90">{line.trim()}</span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-xs text-muted-foreground italic">No definition available</p>
          )}
        </div>
      </div>
    </div>
  )
}
