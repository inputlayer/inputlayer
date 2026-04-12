"use client"

import { useState, useCallback, useRef, Component, type ReactNode, type ErrorInfo } from "react"
import {
  TreePine,
  Download,
  Loader2,
  ChevronRight,
  ChevronDown,
  Database,
  GitBranch,
  ShieldOff,
  Search,
  Layers,
  AlertTriangle,
  RefreshCw,
  FileJson,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useIQLStore } from "@/lib/iql-store"
import { cn } from "@/lib/utils"
import type { QueryResult } from "@/lib/iql-store"
import type { WsProofTree, WsProofNode, JsonValue } from "@/lib/ws-types"

// ── Error Boundary ────────────────────────────────────────────────

interface ErrorBoundaryProps {
  children: ReactNode
  fallback?: ReactNode
}

interface ErrorBoundaryState {
  hasError: boolean
  error: Error | null
}

class ProofTreeErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo) {
    console.error("ProofTreePanel error:", error, errorInfo)
  }

  render() {
    if (this.state.hasError) {
      return this.props.fallback ?? (
        <div className="p-4 text-sm text-red-500">
          Error rendering proof tree: {this.state.error?.message}
        </div>
      )
    }
    return this.props.children
  }
}

// ── Helpers ────────────────────────────────────────────────────

function extractQueryLine(editorContent: string): string | null {
  // Find the LAST ?query(...) line, since the server returns results
  // for the last query when multiple statements are in the editor.
  let lastQuery: string | null = null
  for (const line of editorContent.split("\n")) {
    const trimmed = line.trim()
    if (trimmed.startsWith("?") && trimmed.includes("(")) {
      lastQuery = trimmed
    }
    const whyMatch = trimmed.match(/^\.why\s+(?:full\s+)?(.+)$/)
    if (whyMatch) lastQuery = whyMatch[1]
    if (trimmed.startsWith(".why_not ")) return null
  }
  return lastQuery
}

function formatCellValue(v: JsonValue): string {
  if (v === null) return "NULL"
  if (typeof v === "string") return `"${v}"`
  return String(v)
}

// ── Node kind configuration ────────────────────────────────────

const kindConfig: Record<string, { icon: typeof Database; label: string; color: string }> = {
  fact: { icon: Database, label: "fact", color: "emerald" },
  rule: { icon: GitBranch, label: "rule", color: "blue" },
  negation: { icon: ShieldOff, label: "absent", color: "amber" },
  vector_search: { icon: Search, label: "vector", color: "purple" },
  aggregate: { icon: Layers, label: "aggregate", color: "cyan" },
  truncated: { icon: AlertTriangle, label: "truncated", color: "red" },
  why_not: { icon: ShieldOff, label: "not derived", color: "rose" },
}

// ── Proof Node Renderer ────────────────────────────────────

function ProofNodeView({
  nodeId,
  graph,
  defaultOpen = true,
  depth = 0,
}: {
  nodeId: string
  graph: WsProofTree
  defaultOpen?: boolean
  depth?: number
}) {
  const autoCollapse = depth >= 10
  const [open, setOpen] = useState(defaultOpen && !autoCollapse)

  const node = graph.nodes[nodeId]
  if (!node) return null

  const hasChildren = node.children && node.children.length > 0
  const cfg = kindConfig[node.kind] ?? { icon: TreePine, label: node.kind, color: "slate" }
  const Icon = cfg.icon

  // Build display text
  const conclusionStr = `${node.conclusion.pred}(${node.conclusion.args.map(formatCellValue).join(", ")})`

  let detailText = ""
  if (node.kind === "rule" && node.rule_id) {
    detailText = node.rule_id
  } else if (node.kind === "aggregate" && node.aggregate) {
    detailText = `${node.aggregate.fn}(${node.aggregate.value_var}) = ${formatCellValue(node.aggregate.result)} over ${node.aggregate.contributing_count} tuples`
  } else if (node.kind === "negation" && node.negation) {
    detailText = `no matching ${node.negation.pattern}`
  } else if (node.kind === "vector_search" && node.vector_search) {
    detailText = `index=${node.vector_search.index_name}, metric=${node.vector_search.metric}, distance=${node.vector_search.distance.toFixed(6)}, k=${node.vector_search.k}`
  } else if (node.kind === "truncated" && node.truncated) {
    detailText = `derivation exceeds depth limit (${node.truncated.depth_limit})`
  } else if (node.kind === "why_not" && node.why_not) {
    detailText = `${node.why_not.blocker.type}: ${node.why_not.blocker.reason ?? node.why_not.blocker.predicate_text ?? ""}`
  }

  return (
    <div>
      <div
        className={cn(
          "flex items-start gap-2 rounded-md border-l-2 px-2.5 py-1.5 transition-colors",
          hasChildren && "cursor-pointer",
        )}
        style={{
          borderLeftColor: `var(--color-${cfg.color}-500, oklch(0.7 0.15 160))`,
          backgroundColor: `color-mix(in oklch, var(--color-${cfg.color}-500, oklch(0.7 0.15 160)) 3%, transparent)`,
        }}
        onClick={() => hasChildren && setOpen(!open)}
      >
        {hasChildren ? (
          open ? <ChevronDown className="mt-0.5 h-3 w-3 flex-shrink-0 text-muted-foreground/50" />
               : <ChevronRight className="mt-0.5 h-3 w-3 flex-shrink-0 text-muted-foreground/50" />
        ) : (
          <span className="w-3 flex-shrink-0" />
        )}
        <Icon className={cn("mt-0.5 h-3.5 w-3.5 flex-shrink-0")} style={{ color: `var(--color-${cfg.color}-500)` }} />
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="font-mono text-xs font-medium text-foreground/90">{conclusionStr}</span>
            <Badge
              variant={node.kind === "truncated" || node.kind === "why_not" ? "destructive" : "secondary"}
              className="h-4 px-1.5 text-[10px] font-normal"
            >
              {cfg.label}
            </Badge>
          </div>
          {detailText && (
            <p className="mt-0.5 font-mono text-[11px] text-muted-foreground leading-relaxed">{detailText}</p>
          )}
          {node.bindings && Object.keys(node.bindings).length > 0 && (
            <div className="mt-1 flex flex-wrap gap-1">
              {Object.entries(node.bindings).map(([variable, value], i) => (
                <span key={i} className="inline-flex items-center rounded-sm bg-foreground/[0.05] px-1.5 py-0.5 font-mono text-[10px] leading-none">
                  <span className="text-blue-500 dark:text-blue-400">{variable}</span>
                  <span className="mx-0.5 text-muted-foreground/60">=</span>
                  <span className="text-foreground/80">{formatCellValue(value)}</span>
                </span>
              ))}
            </div>
          )}
          {node.kind === "aggregate" && node.aggregate?.sample_inputs && node.aggregate.sample_inputs.length > 0 && (
            <div className="mt-1 space-y-0.5">
              {node.aggregate.sample_inputs.map((row, i) => (
                <div key={i} className="font-mono text-[10px] text-muted-foreground/80">
                  ({row.map(formatCellValue).join(", ")})
                </div>
              ))}
              {(node.aggregate.contributing_count ?? 0) > node.aggregate.sample_inputs.length && (
                <div className="font-mono text-[10px] text-muted-foreground/50 italic">
                  ... and {(node.aggregate.contributing_count ?? 0) - node.aggregate.sample_inputs.length} more
                </div>
              )}
            </div>
          )}
          {node.kind === "why_not" && node.why_not?.clause_text && (
            <p className="mt-0.5 font-mono text-[10px] text-muted-foreground/60">{node.why_not.clause_text}</p>
          )}
        </div>
      </div>
      {open && hasChildren && (
        <div className="ml-[18px] mt-0.5 space-y-0.5 border-l border-border/30 pl-2">
          {node.children.map((childId, i) => (
            <ProofNodeView key={i} nodeId={childId} graph={graph} defaultOpen={true} depth={depth + 1} />
          ))}
        </div>
      )}
    </div>
  )
}

// --- Result tuple card with inline proof tree ---

function ResultTupleCard({
  index,
  columns,
  row,
  graph,
}: {
  index: number
  columns: string[]
  row: (string | number | boolean | null)[]
  graph: WsProofTree | null
}) {
  return (
    <div className="rounded-lg border bg-card">
      <div className="flex items-center gap-2 border-b px-3 py-1.5">
        <span className="text-xs font-medium text-muted-foreground">#{index + 1}</span>
        <div className="flex gap-2">
          {row.map((val, i) => (
            <span key={i} className="font-mono text-xs">
              {columns[i] && <span className="text-muted-foreground/60">{columns[i]}=</span>}
              <span className="text-foreground">{formatCellValue(val)}</span>
            </span>
          ))}
        </div>
      </div>
      {graph && (
        <div className="p-2 space-y-0.5">
          {graph.roots.map((rootId, i) => (
            <ProofNodeView key={i} nodeId={rootId} graph={graph} defaultOpen={true} depth={0} />
          ))}
        </div>
      )}
      {!graph && (
        <div className="p-2 text-xs text-muted-foreground">No proof tree available</div>
      )}
    </div>
  )
}

// --- Main Panel ---

interface ProofTreePanelProps {
  query: string
  result: QueryResult | null
}

export function ProofTreePanel(props: ProofTreePanelProps) {
  return (
    <ProofTreeErrorBoundary>
      <ProofTreePanelInner {...props} />
    </ProofTreeErrorBoundary>
  )
}

function ProofTreePanelInner({ query, result }: ProofTreePanelProps) {
  const cachedRef = useRef<{ query: string; result: QueryResult } | null>(null)
  const [whyResult, setWhyResult] = useState<QueryResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [viewMode, setViewMode] = useState<"tree" | "json">("tree")

  // Use graphs from the result if it was a .why query, otherwise use separately fetched
  const hasInlineGraphs = result?.proofTrees && result.proofTrees.length > 0
  const activeResult = hasInlineGraphs ? result : whyResult
  const graphs = activeResult?.proofTrees ?? null

  const loadProof = useCallback(async () => {
    if (!query) return
    setLoading(true)
    setError(null)
    try {
      const queryLine = extractQueryLine(query)
      if (!queryLine) {
        setError("No query found to explain. Run a ?query(...) first.")
        setLoading(false)
        return
      }
      const response = await (useIQLStore.getState().executeInternalQuery(`.why ${queryLine}`))
      if (response.status === "error") {
        setError(response.error ?? "Failed to compute proof tree")
      } else {
        setWhyResult(response)
        cachedRef.current = { query, result: response }
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [query])

  // Restore cache
  if (!whyResult && cachedRef.current?.query === query) {
    setWhyResult(cachedRef.current.result)
  }

  const handleExportJson = useCallback(async () => {
    if (!query) return
    const queryLine = extractQueryLine(query)
    if (!queryLine) return

    // Export uses .why full for complete, verifiable proof trees
    try {
      const fullResponse = await useIQLStore.getState().executeInternalQuery(`.why full ${queryLine}`)
      const exportGraphs = fullResponse?.proofTrees ?? graphs
      if (!exportGraphs) return
      const blob = new Blob([JSON.stringify(exportGraphs, null, 2)], { type: "application/json" })
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = "proof-trees.json"
      a.click()
      URL.revokeObjectURL(url)
    } catch {
      if (!graphs) return
      const blob = new Blob([JSON.stringify(graphs, null, 2)], { type: "application/json" })
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = "proof-trees.json"
      a.click()
      URL.revokeObjectURL(url)
    }
  }, [query, graphs])

  const rows = activeResult?.data ?? result?.data ?? []
  const columns = activeResult?.columns ?? result?.columns ?? []

  // Show compute button if no graphs available yet
  if (!graphs || graphs.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-12 text-muted-foreground">
        <TreePine className="h-8 w-8 opacity-30" />
        {error ? (
          <>
            <p className="text-sm text-red-500 max-w-md text-center">{error}</p>
            <Button variant="outline" size="sm" onClick={loadProof} disabled={loading}>
              {loading ? <Loader2 className="mr-2 h-3 w-3 animate-spin" /> : <RefreshCw className="mr-2 h-3 w-3" />}
              Retry
            </Button>
          </>
        ) : (
          <>
            <p className="text-sm">Run a query, then click below to see how the results were derived.</p>
            <Button variant="outline" size="sm" onClick={loadProof} disabled={loading || !query}>
              {loading ? <Loader2 className="mr-2 h-3 w-3 animate-spin" /> : <TreePine className="mr-2 h-3 w-3" />}
              Compute Proof Tree
            </Button>
          </>
        )}
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-2 p-2 h-full overflow-auto">
      <div className="flex items-center justify-between flex-shrink-0">
        <span className="text-xs font-medium text-muted-foreground">
          {rows.length} result{rows.length !== 1 ? "s" : ""} with proof trees
        </span>
        <div className="flex gap-1">
          <Button
            variant={viewMode === "tree" ? "secondary" : "ghost"}
            size="sm"
            className="h-6 gap-1 px-2 text-xs"
            onClick={() => setViewMode("tree")}
          >
            <TreePine className="h-3 w-3" />
            Tree
          </Button>
          <Button
            variant={viewMode === "json" ? "secondary" : "ghost"}
            size="sm"
            className="h-6 gap-1 px-2 text-xs"
            onClick={() => setViewMode("json")}
          >
            <FileJson className="h-3 w-3" />
            JSON
          </Button>
          <div className="w-px h-4 bg-border mx-0.5" />
          <Button variant="ghost" size="sm" className="h-6 gap-1 px-2 text-xs" onClick={loadProof} disabled={loading}>
            <RefreshCw className={cn("h-3 w-3", loading && "animate-spin")} />
            Refresh
          </Button>
          <Button variant="ghost" size="sm" className="h-6 gap-1 px-2 text-xs" onClick={handleExportJson}>
            <Download className="h-3 w-3" />
            Export
          </Button>
        </div>
      </div>
      {viewMode === "tree" ? (
        <div className="space-y-2">
          {rows.map((row, i) => (
            <ResultTupleCard
              key={i}
              index={i}
              columns={columns.map((c) => String(c))}
              row={row}
              graph={graphs[i] ?? null}
            />
          ))}
        </div>
      ) : (
        <pre className="flex-1 p-3 text-[11px] font-mono text-muted-foreground bg-muted/30 rounded-md overflow-auto">
          {JSON.stringify(graphs, null, 2)}
        </pre>
      )}
    </div>
  )
}
