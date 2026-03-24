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
  Table2,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useDatalogStore } from "@/lib/datalog-store"
import { cn } from "@/lib/utils"
import type { QueryResult } from "@/lib/datalog-store"
import type { WsProofTree } from "@/lib/ws-types"

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
        <div className="flex h-full flex-col items-center justify-center gap-3 p-8">
          <AlertTriangle className="h-6 w-6 text-destructive" />
          <p className="text-sm text-destructive">Failed to render proof tree</p>
          <p className="text-xs text-muted-foreground max-w-sm text-center">{this.state.error?.message}</p>
          <Button variant="outline" size="sm" onClick={() => this.setState({ hasError: false, error: null })}>
            Retry
          </Button>
        </div>
      )
    }
    return this.props.children
  }
}

interface ProofTreePanelProps {
  query: string
  result: QueryResult | null
}

/**
 * Extract the query portion from editor content for .why computation.
 * Handles: ?query(X), .why ?query(X), .why full ?query(X), multi-line programs.
 */
function extractQueryLine(text: string): string | null {
  const lines = text.split("\n").map((l) => l.trim()).filter(Boolean)

  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i]

    // Direct ?query shorthand
    if (line.startsWith("?")) return line

    // .why or .why full - extract the query argument
    const whyMatch = line.match(/^\.why\s+(?:full\s+)?(.+)$/)
    if (whyMatch) return whyMatch[1]

    // .why_not - the whole line IS the command, no separate query to extract
    if (line.startsWith(".why_not ")) return null

    // Bare query rule (not insert/delete/meta)
    if (line.includes("<-") && !line.startsWith("+") && !line.startsWith("-") && !line.startsWith(".")) {
      return line
    }
  }

  // Single line that looks like a query
  const trimmed = text.trim()
  if (trimmed && !trimmed.includes("\n") && !trimmed.startsWith("+") && !trimmed.startsWith(".")) {
    return trimmed
  }

  return null
}

function formatCellValue(v: string | number | boolean | null): string {
  if (v === null) return "NULL"
  if (typeof v === "string") return `"${v}"`
  return String(v)
}

function formatBindingValue(v: string | number | boolean | null): string {
  if (v === null) return "NULL"
  if (typeof v === "string") return `"${v}"`
  return String(v)
}

// --- Proof tree node renderer (consumes structured WsProofTree) ---

function ProofNodeView({ node, defaultOpen = true, depth = 0 }: { node: WsProofTree; defaultOpen?: boolean; depth?: number }) {
  const autoCollapse = depth >= 10
  const [open, setOpen] = useState(autoCollapse ? false : defaultOpen)
  const children = node.children ?? (node.inner ? [node.inner] : [])
  const hasChildren = children.length > 0

  const typeConfig: Record<string, { icon: typeof Database; label: string; color: string }> = {
    base_fact:      { icon: Database,      label: "fact",      color: "emerald" },
    rule_application: { icon: GitBranch,   label: "rule",      color: "blue" },
    negation:       { icon: ShieldOff,     label: "absent",    color: "amber" },
    vector_search:  { icon: Search,        label: "vector",    color: "purple" },
    aggregation:    { icon: Layers,        label: "aggregate", color: "cyan" },
    truncated:      { icon: AlertTriangle, label: "truncated", color: "red" },
    why_not:        { icon: ShieldOff,     label: "why-not",   color: "amber" },
  }

  const cfg = typeConfig[node.node_type] ?? { icon: TreePine, label: node.node_type, color: "slate" }
  const Icon = cfg.icon

  // Build display text based on node type
  let mainText = ""
  let detailText = ""
  if (node.node_type === "base_fact") {
    const vals = node.values?.map(formatBindingValue).join(", ") ?? ""
    mainText = `${node.relation ?? "?"}(${vals})`
  } else if (node.node_type === "rule_application") {
    mainText = `${node.rule_name ?? "?"} (clause ${node.clause_index ?? 0})`
    detailText = node.clause_text ?? ""
  } else if (node.node_type === "negation") {
    mainText = `no matching ${node.relation ?? "?"}(${node.pattern ?? ""})`
  } else if (node.node_type === "vector_search") {
    mainText = `index=${node.index_name}, metric=${node.metric}`
    detailText = `result: id=${node.result_id}, distance=${node.distance?.toFixed(6)}, k=${node.k}`
  } else if (node.node_type === "aggregation") {
    mainText = `${node.rule_name}.${node.aggregate_fn}`
    detailText = `${node.contributing_count} contributing tuples`
  } else if (node.node_type === "truncated") {
    mainText = `proof exceeds depth limit (${node.depth_limit})`
  } else if (node.node_type === "why_not") {
    mainText = `${node.relation ?? "?"} - negative explanation`
  } else {
    mainText = node.node_type
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
            <span className="font-mono text-xs font-medium text-foreground/90">{mainText}</span>
            <Badge
              variant={node.node_type === "truncated" ? "destructive" : "secondary"}
              className="h-4 px-1.5 text-[10px] font-normal"
            >
              {cfg.label}
            </Badge>
          </div>
          {detailText && (
            <p className="mt-0.5 font-mono text-[11px] text-muted-foreground leading-relaxed">{detailText}</p>
          )}
          {node.bindings && node.bindings.length > 0 && (
            <div className="mt-1 flex flex-wrap gap-1">
              {node.bindings.map((b, i) => (
                <span key={i} className="inline-flex items-center rounded-sm bg-foreground/[0.05] px-1.5 py-0.5 font-mono text-[10px] leading-none">
                  <span className="text-blue-500 dark:text-blue-400">{b.variable}</span>
                  <span className="mx-0.5 text-muted-foreground/60">=</span>
                  <span className="text-foreground/80">{formatBindingValue(b.value)}</span>
                </span>
              ))}
            </div>
          )}
        </div>
      </div>
      {open && hasChildren && (
        <div className="ml-[18px] mt-0.5 space-y-0.5 border-l border-border/30 pl-2">
          {children.map((child, i) => (
            <ProofNodeView key={i} node={child} defaultOpen={true} depth={depth + 1} />
          ))}
        </div>
      )}
    </div>
  )
}

// --- Result tuple card with inline proof ---

function ResultTupleCard({
  index,
  columns,
  row,
  proof,
}: {
  index: number
  columns: string[]
  row: (string | number | boolean | null)[]
  proof: WsProofTree | null
}) {
  const [open, setOpen] = useState(true)

  return (
    <div className="rounded-lg border border-border/60 bg-card overflow-hidden">
      <div
        className="flex items-center gap-3 px-3 py-2 bg-muted/30 border-b border-border/40 cursor-pointer hover:bg-muted/50 transition-colors"
        onClick={() => setOpen(!open)}
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground/60 flex-shrink-0" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground/60 flex-shrink-0" />
        )}
        <Table2 className="h-3.5 w-3.5 text-foreground/50 flex-shrink-0" />
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <Badge variant="outline" className="h-4 px-1.5 text-[10px] font-mono flex-shrink-0">
            #{index + 1}
          </Badge>
          <div className="flex items-center gap-1.5 overflow-hidden">
            {columns.map((col, ci) => (
              <span key={ci} className="inline-flex items-center gap-0.5 font-mono text-xs truncate">
                <span className="text-muted-foreground">{col}=</span>
                <span className="font-medium text-foreground/90">{formatCellValue(row[ci])}</span>
                {ci < columns.length - 1 && <span className="text-muted-foreground/40 mx-0.5">,</span>}
              </span>
            ))}
          </div>
        </div>
      </div>
      {open && (
        <div className="px-2 py-2">
          {proof ? (
            <div className="space-y-0.5">
              {/* If this is a wrapper with children, show children directly */}
              {proof.children && proof.children.length > 0 ? (
                proof.children.map((child, i) => (
                  <ProofNodeView key={i} node={child} defaultOpen={true} />
                ))
              ) : (
                <ProofNodeView node={proof} defaultOpen={true} />
              )}
            </div>
          ) : (
            <p className="px-2 py-3 text-center text-xs text-muted-foreground italic">
              No proof available for this result
            </p>
          )}
        </div>
      )}
    </div>
  )
}

// --- Main panel ---

export function ProofTreePanel(props: ProofTreePanelProps) {
  return (
    <ProofTreeErrorBoundary>
      <ProofTreePanelInner {...props} />
    </ProofTreeErrorBoundary>
  )
}

function ProofTreePanelInner({ query, result }: ProofTreePanelProps) {
  const cachedProofsRef = useRef<{ query: string; result: QueryResult } | null>(null)
  const [whyResult, setWhyResult] = useState<QueryResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const { whyQuery } = useDatalogStore()

  // Use proofs from the result if it was a .why query, otherwise use separately fetched
  const hasInlineProofs = result?.proofTrees && result.proofTrees.length > 0
  const activeResult = hasInlineProofs ? result : whyResult
  const proofTrees = activeResult?.proofTrees ?? null

  const loadProof = useCallback(async () => {
    if (!query) return
    setLoading(true)
    setError(null)
    try {
      // Extract the ?query(...) portion from potentially multi-line editor content
      const queryLine = extractQueryLine(query)
      if (!queryLine) {
        setError("No query found to explain. Run a ?query(...) first.")
        setLoading(false)
        return
      }
      // Execute .why query - returns structured QueryResult with proof_trees
      const response = await (useDatalogStore.getState().executeInternalQuery(`.why ${queryLine}`))
      if (response.status === "error") {
        setError(response.error ?? "Failed to compute proofs")
      } else {
        setWhyResult(response)
        cachedProofsRef.current = { query, result: response }
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [query])

  // Restore cache
  if (!whyResult && cachedProofsRef.current?.query === query) {
    setWhyResult(cachedProofsRef.current.result)
  }

  const handleExportJson = useCallback(() => {
    if (!proofTrees) return
    const blob = new Blob([JSON.stringify(proofTrees, null, 2)], { type: "application/json" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "proof-trees.json"
    a.click()
    URL.revokeObjectURL(url)
  }, [proofTrees])

  const rows = activeResult?.data ?? result?.data ?? []
  const columns = activeResult?.columns ?? result?.columns ?? []

  // Show compute button if no proofs available yet
  if (!hasInlineProofs && !whyResult && !loading && !error) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4 p-8 text-muted-foreground">
        <div className="rounded-full bg-emerald-500/10 p-4">
          <TreePine className="h-8 w-8 text-emerald-500/50" />
        </div>
        <div className="text-center">
          <p className="text-sm font-medium text-foreground/70">Derivation Proofs</p>
          <p className="mt-1 max-w-xs text-xs text-muted-foreground">
            See why each result was derived - trace through rules and base facts
          </p>
        </div>
        <Button onClick={loadProof} variant="outline" size="sm" className="gap-2">
          <TreePine className="h-4 w-4" />
          Compute Proofs
        </Button>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
        <Loader2 className="h-6 w-6 animate-spin text-emerald-500" />
        <p className="text-sm">Building proof trees...</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-4 p-8">
        <p className="text-sm text-destructive">{error}</p>
        <Button onClick={loadProof} variant="outline" size="sm">Retry</Button>
      </div>
    )
  }

  return (
    <div className="flex h-full flex-col">
      <div className="flex h-9 items-center justify-between border-b border-border/50 bg-emerald-500/5 px-3 flex-shrink-0">
        <div className="flex items-center gap-2">
          <TreePine className="h-3.5 w-3.5 text-emerald-500" />
          <span className="text-xs font-medium text-emerald-600 dark:text-emerald-400">Derivation Proof</span>
          {rows.length > 0 && (
            <Badge variant="secondary" className="h-4 px-1.5 text-[10px]">
              {rows.length} result{rows.length !== 1 ? "s" : ""}
            </Badge>
          )}
        </div>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" className="h-6 gap-1 px-2 text-xs" onClick={handleExportJson}>
            <FileJson className="h-3 w-3" />
            Export JSON
          </Button>
          {!hasInlineProofs && (
            <Button variant="ghost" size="sm" className="h-6 gap-1 px-2 text-xs" onClick={loadProof}>
              <RefreshCw className="h-3 w-3" />
            </Button>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-auto p-3 space-y-2">
        {rows.length > 0 && proofTrees ? (
          rows.map((row, i) => (
            <ResultTupleCard
              key={i}
              index={i}
              columns={columns}
              row={row}
              proof={proofTrees[i] ?? null}
            />
          ))
        ) : proofTrees && proofTrees.length > 0 ? (
          proofTrees.map((tree, i) => (
            <ProofNodeView key={i} node={tree} defaultOpen={true} />
          ))
        ) : (
          <p className="p-4 text-center text-sm text-muted-foreground">No results to explain.</p>
        )}
      </div>
    </div>
  )
}
