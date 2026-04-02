"use client"

import { Clock, Download, Rows3, AlertCircle, CheckCircle2, Loader2, Copy, Check, AlertTriangle, CheckSquare, Info, ArrowUp, ArrowDown, ArrowUpDown, FileJson, Share2, TreePine, Zap } from "lucide-react"
import { ProofTreePanel } from "@/components/proof-tree-panel"
import { TimingDisplay, formatUs, formatTimingSummary } from "@/components/timing-display"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { QueryResultGraph } from "@/components/query-result-graph"
import { cn } from "@/lib/utils"
import { formatTime, downloadBlob } from "@/lib/ui-utils"
import { toast } from "sonner"
import { useState, useMemo, useEffect, useRef } from "react"
import type { QueryResult, StructuredError } from "@/app/query/page"

interface QueryResultsPanelProps {
  result: QueryResult | null
  error: StructuredError | null
  isExecuting: boolean
  activeQuery: string
  sidebarOpen?: boolean
  onStartExample?: () => void
}

/** Detect if query is purely a mutation (insert/delete/meta, no query lines) */
/** Meta commands that return query-like results (not mutations). */
const RESULT_META_COMMANDS = [".why ", ".why_not ", ".debug ", ".rel", ".rule", ".status", ".session"]

function isMutationQuery(query: string): boolean {
  const lines = query.split("\n").map((l) => l.trim()).filter((l) => l.length > 0 && !l.startsWith("//"))
  if (lines.length === 0) return false
  // If any line is a query, it's not a pure mutation
  if (lines.some((l) => l.startsWith("?"))) return false
  // If any line is a result-returning meta command, it's not a mutation
  if (lines.some((l) => RESULT_META_COMMANDS.some((cmd) => l.startsWith(cmd)))) return false
  // Mutations: +insert/rule, -delete, .meta, ~session-rule, session facts (lowercase with parens but no <-)
  return lines.every((l) =>
    l.startsWith("+") || l.startsWith("-") || l.startsWith(".") || l.startsWith("~") ||
    (/^[a-z]/.test(l) && l.includes("(") && !l.includes("<-"))
  )
}

/** Classify a warning as info-level (ephemeral presence) or warning-level (overrides) */
function isWarningLevel(warning: string): boolean {
  return warning.includes("override") || warning.includes("overshadow")
}

const MAX_CELL_LENGTH = 200

function renderCell(cell: string | number | boolean | null) {
  if (cell === null) {
    return <span className="italic text-muted-foreground/50">null</span>
  }
  if (typeof cell === "boolean") {
    return (
      <Badge
        variant="outline"
        className={cn(
          "text-[10px] font-mono",
          cell ? "border-success/50 text-success bg-success/10" : "border-muted-foreground/50",
        )}
      >
        {cell.toString()}
      </Badge>
    )
  }
  if (typeof cell === "number") {
    return <span className="text-[var(--code-variable)]">{cell}</span>
  }
  const s = String(cell)
  if (s.length > MAX_CELL_LENGTH) {
    return <span title={s}>{s.slice(0, MAX_CELL_LENGTH)}...</span>
  }
  return <span>{s}</span>
}

type SortDirection = "asc" | "desc"
interface SortState {
  column: number
  direction: SortDirection
}

/** Compare values for sorting: nulls last, then type-aware comparison */
function compareValues(a: string | number | boolean | null, b: string | number | boolean | null, dir: SortDirection): number {
  if (a === null && b === null) return 0
  if (a === null) return 1
  if (b === null) return -1
  const mul = dir === "asc" ? 1 : -1
  if (typeof a === "number" && typeof b === "number") return (a - b) * mul
  if (typeof a === "boolean" && typeof b === "boolean") return ((a ? 1 : 0) - (b ? 1 : 0)) * mul
  return String(a).localeCompare(String(b)) * mul
}

const PAGE_SIZE = 200

export function QueryResultsPanel({ result, error, isExecuting, activeQuery, sidebarOpen, onStartExample }: QueryResultsPanelProps) {
  const [copied, setCopied] = useState(false)
  const [sort, setSort] = useState<SortState | null>(null)
  const [page, setPage] = useState(0)
  const [activeTab, setActiveTab] = useState<"table" | "graph" | "proof" | "perf">("table")
  const hasUserSelectedTab = useRef(false)
  const prevResultId = useRef<string | null>(null)

  // Reset page when result changes. Auto-switch tabs based on result content.
  useEffect(() => {
    setPage(0)
    const resultId = result?.id ?? null
    if (resultId !== prevResultId.current) {
      prevResultId.current = resultId
      hasUserSelectedTab.current = false
    }
    const hasProof = result?.proofTrees && result.proofTrees.length > 0
    if (!hasUserSelectedTab.current) {
      if (hasProof) {
        setActiveTab("proof")
      } else if (activeTab === "proof") {
        // Previous result had a proof tree but new one doesn't - switch back to table
        setActiveTab("table")
      }
    }
  }, [result]) // eslint-disable-line react-hooks/exhaustive-deps

  const handleSort = (colIndex: number) => {
    setSort((prev) => {
      if (prev?.column === colIndex) {
        // Cycle: asc → desc → none
        if (prev.direction === "asc") return { column: colIndex, direction: "desc" }
        return null
      }
      return { column: colIndex, direction: "asc" }
    })
  }

  // Compute sorted row indices (preserves provenance alignment)
  const sortedIndices = useMemo(() => {
    if (!result || !sort) return null
    const indices = Array.from({ length: result.data.length }, (_, i) => i)
    indices.sort((a, b) => compareValues(result.data[a][sort.column], result.data[b][sort.column], sort.direction))
    return indices
  }, [result, sort])

  const handleExportCsv = () => {
    if (!result) return
    const escapeCell = (v: unknown) => {
      const s = v === null ? "" : String(v)
      return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g, '""')}"` : s
    }
    const csv = [
      result.columns.map(escapeCell).join(","),
      ...result.data.map((row) => row.map(escapeCell).join(",")),
    ].join("\n")
    downloadBlob(csv, "text/csv", "query-results.csv")
  }

  const handleExportJson = () => {
    if (!result) return
    const rows = result.data.map((row) => {
      const obj: Record<string, string | number | boolean | null> = {}
      result.columns.forEach((col, i) => { obj[col] = row[i] })
      return obj
    })
    const json = JSON.stringify(rows, null, 2)
    downloadBlob(json, "application/json", "query-results.json")
  }

  const handleCopyResults = async () => {
    if (!result) return
    const text = [result.columns.join("\t"), ...result.data.map((row) => row.join("\t"))].join("\n")
    await navigator.clipboard.writeText(text)
    setCopied(true)
    toast.success("Copied to clipboard")
    setTimeout(() => setCopied(false), 2000)
  }

  if (isExecuting) {
    const queryPreview = activeQuery.length > 60 ? activeQuery.slice(0, 60) + "..." : activeQuery
    return (
      <div className="flex h-full items-center justify-center bg-background">
        <div className="text-center">
          <Loader2 className="mx-auto h-8 w-8 animate-spin text-primary" />
          <p className="mt-3 text-sm font-medium">Executing query...</p>
          <p className="mt-1 text-xs text-muted-foreground font-mono max-w-md truncate px-4">
            {queryPreview}
          </p>
        </div>
      </div>
    )
  }

  if (error) {
    const hasValidationErrors = error.validationErrors && error.validationErrors.length > 0
    return (
      <div className="flex h-full flex-col bg-background">
        <div className="flex h-9 items-center gap-2 border-b border-border/50 bg-destructive/5 px-3">
          <AlertCircle className="h-3.5 w-3.5 text-destructive" />
          <span className="text-xs font-medium text-destructive">
            Error{hasValidationErrors ? ` \u2014 ${error.validationErrors!.length} issue${error.validationErrors!.length !== 1 ? "s" : ""} found` : ""}
          </span>
        </div>
        <div className="flex flex-1 items-center justify-center p-6">
          <div className="max-w-lg w-full rounded-lg border border-destructive/20 bg-destructive/5 p-4">
            <div className="flex items-start gap-3">
              <AlertCircle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
              <div className="min-w-0 flex-1">
                <p className="font-medium text-destructive">Query Failed</p>
                <p className="mt-1 text-sm text-muted-foreground">{error.message}</p>
                {hasValidationErrors && (
                  <ul className="mt-2 space-y-1.5 border-t border-destructive/10 pt-2">
                    {error.validationErrors!.map((ve, i) => (
                      <li key={i} className="flex items-start gap-2 text-sm font-mono">
                        <Badge variant="outline" className="flex-shrink-0 text-[10px] px-1.5 py-0 border-destructive/30 text-destructive">
                          Ln {ve.line}
                        </Badge>
                        <span className="text-muted-foreground break-all">{ve.error}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (!result) {
    return <WelcomePanel sidebarOpen={sidebarOpen} onStartExample={onStartExample} />
  }

  // Compute row provenance breakdown
  const ephemeralCount = result.rowProvenance?.filter((p) => p === "ephemeral").length ?? 0
  const persistentCount = result.data.length - ephemeralCount

  // Detect mutation results
  const mutation = isMutationQuery(result.query)

  // Show mutation feedback (no data or single-column message rows)
  if (mutation) {
    const messageRows = result.columns.length === 1
      ? result.data.map((row) => String(row[0]))
      : []

    if (result.data.length === 0 || messageRows.length > 0) {
      return (
        <div className="flex h-full flex-col bg-background">
          <div className="flex h-9 items-center gap-2 border-b border-border/50 bg-emerald-500/5 px-3">
            <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
            <span className="text-xs font-medium text-emerald-600 dark:text-emerald-400">Mutation Applied</span>
            <div className="h-3 w-px bg-border" />
            <span className="flex items-center gap-1 text-xs text-muted-foreground">
              <Clock className="h-3 w-3" />
              {formatTime(result.executionTime)}
            </span>
          </div>
          <div className="flex flex-1 items-center justify-center p-6">
            <div className="text-center">
              <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-emerald-500/10">
                <CheckSquare className="h-5 w-5 text-emerald-500" />
              </div>
              <p className="text-sm font-medium">Mutation applied successfully</p>
              {messageRows.length > 0 ? (
                <div className="mt-2 space-y-1">
                  {messageRows.map((msg, i) => (
                    <p key={i} className="text-xs text-muted-foreground font-mono">{msg}</p>
                  ))}
                </div>
              ) : (
                <p className="mt-1 text-xs text-muted-foreground font-mono max-w-md truncate">
                  {result.query.length > 80 ? result.query.slice(0, 80) + "..." : result.query}
                </p>
              )}
            </div>
          </div>
        </div>
      )
    }
  }

  // Split warnings by severity: info-level vs warning-level
  const infoMessages = result.warnings?.filter((w) => !isWarningLevel(w)) ?? []
  const warnMessages = result.warnings?.filter(isWarningLevel) ?? []

  return (
    <div className="flex h-full flex-col bg-background">
      {/* Results toolbar */}
      <div className="flex h-9 items-center justify-between border-b border-border/50 bg-muted/30 px-3">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <CheckCircle2 className="h-3.5 w-3.5 text-success" />
            <span className="text-xs font-medium text-success">Success</span>
          </div>
          <div className="h-3 w-px bg-border" />
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <span className="flex items-center gap-1">
              <Rows3 className="h-3 w-3" />
              {result.data.length.toLocaleString()} rows
              {ephemeralCount > 0 && (
                <span className="text-muted-foreground/70">
                  ({persistentCount.toLocaleString()} persistent, {ephemeralCount.toLocaleString()} session)
                </span>
              )}
            </span>
            <span className="flex items-center gap-1" title={result.timingBreakdown ? formatTimingSummary(result.timingBreakdown) : undefined}>
              <Clock className="h-3 w-3" />
              {formatTime(result.executionTime)}
              {result.timingBreakdown && (
                <span className="text-muted-foreground/60 ml-0.5">
                  ({formatTimingSummary(result.timingBreakdown)})
                </span>
              )}
            </span>
            {result.hasEphemeral && (
              <>
                <div className="h-3 w-px bg-border" />
                <span className="flex items-center gap-1 text-amber-600 dark:text-amber-400">
                  <span className="h-1.5 w-1.5 rounded-full bg-amber-500" />
                  Session data{result.ephemeralSources && result.ephemeralSources.length > 0
                    ? ` (${result.ephemeralSources.join(", ")})`
                    : ""}
                </span>
              </>
            )}
          </div>
        </div>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" onClick={handleCopyResults} className="h-7 gap-1.5 px-2 text-xs">
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
            Copy
          </Button>
          <Button variant="ghost" size="sm" onClick={handleExportCsv} className="h-7 gap-1.5 px-2 text-xs">
            <Download className="h-3 w-3" />
            CSV
          </Button>
          <Button variant="ghost" size="sm" onClick={handleExportJson} className="h-7 gap-1.5 px-2 text-xs">
            <FileJson className="h-3 w-3" />
            JSON
          </Button>
        </div>
      </div>

      {/* Warning-level messages (amber - overrides, overshadowing) */}
      {warnMessages.length > 0 && (
        <div className="border-b border-border/50">
          {warnMessages.map((warning, i) => (
            <div key={`warn-${i}`} className="flex items-center gap-2 bg-amber-500/5 px-3 py-1.5 text-xs text-amber-700 dark:text-amber-400">
              <AlertTriangle className="h-3 w-3 flex-shrink-0" />
              <span>{warning}</span>
            </div>
          ))}
        </div>
      )}

      {/* Info-level messages (blue/neutral - session data presence) */}
      {infoMessages.length > 0 && (
        <div className="border-b border-border/50">
          {infoMessages.map((msg, i) => (
            <div key={`info-${i}`} className="flex items-center gap-2 bg-sky-500/5 px-3 py-1.5 text-xs text-sky-700 dark:text-sky-400">
              <Info className="h-3 w-3 flex-shrink-0" />
              <span>{msg}</span>
            </div>
          ))}
        </div>
      )}

      {/* Truncation warning */}
      {result.truncated && (
        <div className="flex items-center gap-2 border-b border-border/50 bg-amber-500/5 px-3 py-1.5 text-xs text-amber-700 dark:text-amber-400">
          <AlertTriangle className="h-3 w-3 flex-shrink-0" />
          <span>
            Results truncated - showing {result.data.length.toLocaleString()}{result.totalCount != null ? ` of ${result.totalCount.toLocaleString()}` : ""} rows
          </span>
        </div>
      )}

      {/* Row provenance legend */}
      {ephemeralCount > 0 && (
        <div className="flex items-center gap-2 border-b border-border/30 px-3 py-1 text-[10px] text-muted-foreground">
          <span className="h-1.5 w-1.5 rounded-full bg-amber-500" />
          <span>= session data</span>
        </div>
      )}

      {/* Content: table + graph tab */}
      {result.columns.length >= 1 ? (
        <Tabs value={activeTab} onValueChange={(v) => { hasUserSelectedTab.current = true; setActiveTab(v as "table" | "graph" | "proof" | "perf") }} className="flex-1 flex flex-col overflow-hidden min-h-0">
          <div className="border-b border-border/50 px-3 flex-shrink-0">
            <TabsList className="h-8 bg-transparent p-0 gap-2">
              <TabsTrigger
                value="table"
                className="h-7 gap-1.5 rounded-lg px-2.5 text-xs text-muted-foreground data-[state=active]:bg-chart-2/10 data-[state=active]:text-chart-2 data-[state=active]:shadow-none"
              >
                <Rows3 className="h-3.5 w-3.5" />
                Table
              </TabsTrigger>
              <TabsTrigger
                value="graph"
                className="h-7 gap-1.5 rounded-lg px-2.5 text-xs text-muted-foreground data-[state=active]:bg-chart-2/10 data-[state=active]:text-chart-2 data-[state=active]:shadow-none"
              >
                <Share2 className="h-3.5 w-3.5" />
                Graph
              </TabsTrigger>
              <TabsTrigger
                value="proof"
                className="h-7 gap-1.5 rounded-lg px-2.5 text-xs text-muted-foreground data-[state=active]:bg-emerald-500/10 data-[state=active]:text-emerald-600 dark:data-[state=active]:text-emerald-400 data-[state=active]:shadow-none"
              >
                <TreePine className="h-3.5 w-3.5" />
                Why?
              </TabsTrigger>
              {result.timingBreakdown && (
                <TabsTrigger
                  value="perf"
                  className="h-7 gap-1.5 rounded-lg px-2.5 text-xs text-muted-foreground data-[state=active]:bg-orange-500/10 data-[state=active]:text-orange-600 dark:data-[state=active]:text-orange-400 data-[state=active]:shadow-none"
                >
                  <Zap className="h-3.5 w-3.5" />
                  Performance
                </TabsTrigger>
              )}
            </TabsList>
          </div>
          <TabsContent value="table" className="flex-1 m-0 overflow-hidden flex flex-col">
            <ResultTable result={result} sort={sort} sortedIndices={sortedIndices} page={page} setPage={setPage} handleSort={handleSort} />
          </TabsContent>
          <TabsContent value="graph" className="flex-1 m-0 overflow-hidden">
            <QueryResultGraph data={result.data} columns={result.columns} name={result.query.match(/\?(\w+)\s*\(/)?.[1]} />
          </TabsContent>
          <TabsContent value="proof" forceMount className={cn("flex-1 m-0 overflow-hidden", activeTab !== "proof" && "hidden")}>
            <ProofTreePanel query={activeQuery} result={result} />
          </TabsContent>
          {result.timingBreakdown && (
            <TabsContent value="perf" className="flex-1 m-0 overflow-hidden flex flex-col">
              <div className="flex-1 overflow-auto p-4">
                <TimingDisplay tb={result.timingBreakdown} executionTimeMs={result.executionTime} rowCount={result.data.length} />
              </div>
            </TabsContent>
          )}
        </Tabs>
      ) : (
        <ResultTable result={result} sort={sort} sortedIndices={sortedIndices} page={page} setPage={setPage} handleSort={handleSort} />
      )}
    </div>
  )
}

/** Extracted table + pagination to avoid duplication between tabbed and non-tabbed modes */
function ResultTable({
  result, sort, sortedIndices, page, setPage, handleSort,
}: {
  result: QueryResult
  sort: SortState | null
  sortedIndices: number[] | null
  page: number
  setPage: (p: number) => void
  handleSort: (col: number) => void
}) {
  return (
    <>
      <div className="flex-1 overflow-auto scrollbar-thin">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-muted/80 backdrop-blur-sm">
              <th className="w-12 border-b border-r border-border/50 px-3 py-2 text-center text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                #
              </th>
              {result.columns.map((col, colIdx) => {
                const isSorted = sort?.column === colIdx
                return (
                  <th
                    key={`${col}-${colIdx}`}
                    onClick={() => handleSort(colIdx)}
                    aria-sort={isSorted ? (sort.direction === "asc" ? "ascending" : "descending") : "none"}
                    className={cn(
                      "group border-b border-r border-border/50 px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-muted-foreground last:border-r-0 cursor-pointer select-none hover:bg-muted/60 transition-colors",
                      isSorted && "text-foreground"
                    )}
                  >
                    <span className="flex items-center gap-1">
                      {col}
                      {isSorted ? (
                        sort.direction === "asc" ? <ArrowUp className="h-3 w-3" /> : <ArrowDown className="h-3 w-3" />
                      ) : (
                        <ArrowUpDown className="h-3 w-3 opacity-0 group-hover:opacity-30" />
                      )}
                    </span>
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {result.data.length === 0 ? (
              <tr>
                <td
                  colSpan={result.columns.length + 1}
                  className="border-b border-border/30 px-3 py-8 text-center text-sm text-muted-foreground"
                >
                  No matching rows
                </td>
              </tr>
            ) : (() => {
              const allIndices = sortedIndices ?? result.data.map((_, i) => i)
              const totalPages = Math.max(1, Math.ceil(allIndices.length / PAGE_SIZE))
              const safePage = Math.min(page, totalPages - 1)
              const pageIndices = allIndices.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE)
              return pageIndices.map((dataIndex, i) => {
                const row = result.data[dataIndex]
                const isEphemeral = result.rowProvenance?.[dataIndex] === "ephemeral"
                const displayIndex = safePage * PAGE_SIZE + i
                return (
                  <tr key={dataIndex} className={cn(
                    "group transition-colors hover:bg-muted/50",
                    isEphemeral && "bg-amber-500/5"
                  )}>
                    <td className={cn(
                      "border-b border-r border-border/30 px-3 py-2 text-center font-mono text-[10px] text-muted-foreground",
                      isEphemeral && "border-l-2 border-l-amber-500"
                    )}>
                      {displayIndex + 1}
                    </td>
                    {row.map((cell, cellIndex) => (
                      <td
                        key={cellIndex}
                        className="border-b border-r border-border/30 px-3 py-2 font-mono text-xs last:border-r-0"
                      >
                        {renderCell(cell)}
                      </td>
                    ))}
                  </tr>
                )
              })
            })()}
          </tbody>
        </table>
      </div>

      {/* Pagination footer */}
      {result.data.length > PAGE_SIZE && (() => {
        const totalRows = result.data.length
        const totalPages = Math.ceil(totalRows / PAGE_SIZE)
        const safePage = Math.min(page, totalPages - 1)
        return (
          <div className="flex items-center justify-between border-t border-border/50 bg-muted/30 px-4 py-2 flex-shrink-0">
            <p className="text-[10px] text-muted-foreground">
              Showing {(safePage * PAGE_SIZE + 1).toLocaleString()}–{Math.min((safePage + 1) * PAGE_SIZE, totalRows).toLocaleString()} of {totalRows.toLocaleString()} rows
            </p>
            <div className="flex items-center gap-1">
              <Button variant="ghost" size="sm" className="h-6 px-1.5 text-[10px]" disabled={safePage === 0} onClick={() => setPage(0)}>
                First
              </Button>
              <Button variant="ghost" size="sm" className="h-6 px-1.5 text-[10px]" disabled={safePage === 0} onClick={() => setPage(safePage - 1)}>
                Prev
              </Button>
              <span className="text-[10px] text-muted-foreground px-1">
                {safePage + 1} / {totalPages}
              </span>
              <Button variant="ghost" size="sm" className="h-6 px-1.5 text-[10px]" disabled={safePage >= totalPages - 1} onClick={() => setPage(safePage + 1)}>
                Next
              </Button>
              <Button variant="ghost" size="sm" className="h-6 px-1.5 text-[10px]" disabled={safePage >= totalPages - 1} onClick={() => setPage(totalPages - 1)}>
                Last
              </Button>
            </div>
          </div>
        )
      })()}
    </>
  )
}

// --- Welcome Panel (shown when no query has been run yet) ---

function WelcomePanel({ sidebarOpen, onStartExample }: { sidebarOpen?: boolean; onStartExample?: () => void }) {
  return (
    <div className="flex h-full items-center justify-center bg-muted/20 overflow-auto">
      <div className="max-w-md px-4 py-8 text-center">
        <h2 className="text-lg font-semibold text-foreground">Welcome to InputLayer</h2>
        <p className="mt-2 text-sm text-muted-foreground">
          A reasoning engine that derives conclusions from facts and rules, and explains why.
        </p>
        {sidebarOpen ? (
          <p className="mt-4 text-sm text-muted-foreground">
            Pick a learning journey from the sidebar to get started.
          </p>
        ) : (
          <button
            onClick={() => onStartExample?.()}
            className="mt-6 inline-flex items-center gap-2 rounded-lg bg-primary px-5 py-2.5 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90"
          >
            Let&apos;s get started
          </button>
        )}
      </div>
    </div>
  )
}
