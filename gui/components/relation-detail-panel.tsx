"use client"

import { useState, useEffect, useCallback, useMemo } from "react"
import { Network, Rows3, Columns3, Copy, Check, Download, Filter, ArrowUpDown, RefreshCw, Loader2, FileJson } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import { downloadBlob } from "@/lib/ui-utils"
import { toast } from "sonner"
import type { Relation } from "@/lib/datalog-store"
import { useDatalogStore } from "@/lib/datalog-store"

interface RelationDetailPanelProps {
  relation: Relation
}

export function RelationDetailPanel({ relation }: RelationDetailPanelProps) {
  const { loadRelationData } = useDatalogStore()
  const [copied, setCopied] = useState(false)
  const [filter, setFilter] = useState("")
  const [sortColumn, setSortColumn] = useState<number | null>(null)
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc")
  const [loading, setLoading] = useState(false)
  const [currentData, setCurrentData] = useState<(string | number | boolean | null)[][]>(relation.data)
  const [currentColumns, setCurrentColumns] = useState<string[]>(relation.columns)
  const [currentTupleCount, setCurrentTupleCount] = useState(relation.tupleCount)
  const [page, setPage] = useState(0)
  const pageSize = 100

  // Update local state when relation prop changes
  useEffect(() => {
    setCurrentData(relation.data)
    setCurrentColumns(relation.columns)
    setCurrentTupleCount(relation.tupleCount)
    setPage(0)
  }, [relation.data, relation.columns, relation.tupleCount])

  const handleRefresh = useCallback(async () => {
    setLoading(true)
    try {
      const updated = await loadRelationData(relation.name)
      if (updated) {
        setCurrentData(updated.data)
        setCurrentColumns(updated.columns)
        setCurrentTupleCount(updated.tupleCount)
      }
    } finally {
      setLoading(false)
    }
  }, [loadRelationData, relation.name])

  // Load data and reset UI state when switching relations
  useEffect(() => {
    setFilter("")
    setSortColumn(null)
    setSortDirection("asc")
    setPage(0)
    handleRefresh()
  }, [handleRefresh])

  const handleCopy = async () => {
    const text = [currentColumns.join("\t"), ...currentData.map((row) => row.join("\t"))].join("\n")
    await navigator.clipboard.writeText(text)
    setCopied(true)
    toast.success("Copied to clipboard")
    setTimeout(() => setCopied(false), 2000)
  }

  const handleExportCsv = () => {
    if (currentData.length === 0) return

    const escapeCell = (v: unknown) => {
      const s = v === null ? "" : String(v)
      return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g, '""')}"` : s
    }
    const csvContent = [
      currentColumns.map(escapeCell).join(","),
      ...currentData.map((row) => row.map(escapeCell).join(",")),
    ].join("\n")

    downloadBlob(csvContent, "text/csv", `${relation.name}.csv`)
  }

  const handleExportJson = () => {
    if (currentData.length === 0) return
    const rows = currentData.map((row) => {
      const obj: Record<string, string | number | boolean | null> = {}
      currentColumns.forEach((col, i) => { obj[col] = row[i] })
      return obj
    })
    downloadBlob(JSON.stringify(rows, null, 2), "application/json", `${relation.name}.json`)
  }

  const handleSort = (colIndex: number) => {
    if (sortColumn === colIndex) {
      // Cycle: asc → desc → none
      if (sortDirection === "asc") {
        setSortDirection("desc")
      } else {
        setSortColumn(null)
        setSortDirection("asc")
      }
    } else {
      setSortColumn(colIndex)
      setSortDirection("asc")
    }
  }

  const filteredData = useMemo(() =>
    currentData.filter((row) =>
      row.some((cell) => String(cell).toLowerCase().includes(filter.toLowerCase())),
    ),
    [currentData, filter]
  )

  const sortedData = useMemo(() =>
    sortColumn !== null
      ? [...filteredData].sort((a, b) => {
          const aVal = a[sortColumn] ?? ""
          const bVal = b[sortColumn] ?? ""
          const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
          return sortDirection === "asc" ? cmp : -cmp
        })
      : filteredData,
    [filteredData, sortColumn, sortDirection]
  )

  const totalPages = Math.max(1, Math.ceil(sortedData.length / pageSize))
  const safePage = Math.min(page, totalPages - 1)
  const displayData = sortedData.slice(safePage * pageSize, (safePage + 1) * pageSize)

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border/50 bg-muted/30 px-4 py-3">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-chart-1/10">
            <Network className="h-5 w-5 text-chart-1" />
          </div>
          <div>
            <h2 className="font-semibold font-mono">{relation.name}</h2>
            <p className="text-xs text-muted-foreground">Base Relation • Arity {relation.arity}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={handleRefresh}
            disabled={loading}
            className="h-8 gap-1.5 bg-transparent"
          >
            {loading ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <RefreshCw className="h-3.5 w-3.5" />
            )}
            Refresh
          </Button>
          <Button variant="outline" size="sm" onClick={handleCopy} className="h-8 gap-1.5 bg-transparent">
            {copied ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
            Copy
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleExportCsv}
            disabled={currentData.length === 0}
            className="h-8 gap-1.5 bg-transparent"
          >
            <Download className="h-3.5 w-3.5" />
            CSV
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleExportJson}
            disabled={currentData.length === 0}
            className="h-8 gap-1.5 bg-transparent"
          >
            <FileJson className="h-3.5 w-3.5" />
            JSON
          </Button>
        </div>
      </div>

      {/* Schema */}
      <div className="border-b border-border/50 p-4">
        <h3 className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Schema</h3>
        <code className="font-mono text-sm text-foreground">
          {relation.name}({currentColumns.map((col, i) =>
            `${col}: ${relation.columnTypes?.[i] || 'any'}`
          ).join(', ')})
        </code>
      </div>

      {/* Stats bar */}
      <div className="flex items-center justify-between border-b border-border/50 bg-background px-4 py-2">
        <div className="flex items-center gap-4 text-xs text-muted-foreground">
          <span className="flex items-center gap-1.5">
            <Columns3 className="h-3.5 w-3.5" />
            {currentColumns.length} columns
          </span>
          <span className="flex items-center gap-1.5">
            <Rows3 className="h-3.5 w-3.5" />
            {currentTupleCount.toLocaleString()} tuples
          </span>
        </div>
        <div className="flex items-center gap-2">
          {filter && (
            <span className="text-xs text-muted-foreground whitespace-nowrap">
              {filteredData.length} match{filteredData.length !== 1 ? "es" : ""}
            </span>
          )}
          <div className="relative w-48">
            <Filter className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Filter data..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="h-7 pl-8 text-xs"
              aria-label="Filter relation data"
            />
          </div>
        </div>
      </div>

      {/* Data table */}
      <div className="flex-1 overflow-auto scrollbar-thin">
        <table className="w-full border-collapse text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-muted/80 backdrop-blur-sm">
              <th className="w-12 border-b border-r border-border/50 px-3 py-2 text-center text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                #
              </th>
              {currentColumns.map((col, index) => (
                <th
                  key={`${col}-${index}`}
                  onClick={() => handleSort(index)}
                  className="cursor-pointer border-b border-r border-border/50 px-3 py-1.5 text-left text-[10px] font-semibold uppercase tracking-wider text-muted-foreground transition-colors hover:bg-muted last:border-r-0"
                >
                  <div className="flex items-center gap-1">
                    {col}
                    <ArrowUpDown className={cn("h-3 w-3", sortColumn === index ? "text-primary" : "opacity-30")} />
                  </div>
                  {relation.columnTypes?.[index] && (
                    <span className="text-[9px] font-normal normal-case tracking-normal text-muted-foreground/50">
                      {relation.columnTypes[index]}
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayData.map((row, rowIndex) => (
              <tr key={rowIndex} className="group transition-colors hover:bg-muted/50">
                <td className="border-b border-r border-border/30 px-3 py-2 text-center font-mono text-[10px] text-muted-foreground">
                  {safePage * pageSize + rowIndex + 1}
                </td>
                {row.map((cell, cellIndex) => (
                  <td
                    key={cellIndex}
                    className="border-b border-r border-border/30 px-3 py-2 font-mono text-xs last:border-r-0"
                  >
                    {cell === null ? (
                      <span className="italic text-muted-foreground/50">null</span>
                    ) : typeof cell === "boolean" ? (
                      <Badge
                        variant="outline"
                        className={cn(
                          "text-[10px] font-mono",
                          cell ? "border-success/50 text-success bg-success/10" : "border-muted-foreground/50",
                        )}
                      >
                        {cell.toString()}
                      </Badge>
                    ) : typeof cell === "number" ? (
                      <span className="text-[var(--code-variable)]">{cell}</span>
                    ) : (
                      <span>{String(cell)}</span>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Footer with pagination */}
      <div className="flex items-center justify-between border-t border-border/50 bg-muted/30 px-4 py-2">
        <p className="text-[10px] text-muted-foreground">
          {sortedData.length === 0
            ? (filter ? "No matching rows" : "No rows")
            : <>Showing {safePage * pageSize + 1}–{Math.min((safePage + 1) * pageSize, sortedData.length)} of {sortedData.length} rows
              {sortedData.length < currentTupleCount && ` (${currentTupleCount.toLocaleString()} total)`}
              {filter && ` (filtered)`}</>}
        </p>
        {totalPages > 1 && (
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
        )}
      </div>
    </div>
  )
}
