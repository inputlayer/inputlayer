"use client"

import { useState, useEffect, useCallback } from "react"
import { Network, Rows3, Columns3, Copy, Check, Download, Filter, ArrowUpDown, RefreshCw, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
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

  // Update local state when relation prop changes
  useEffect(() => {
    setCurrentData(relation.data)
    setCurrentColumns(relation.columns)
    setCurrentTupleCount(relation.tupleCount)
  }, [relation.data, relation.columns, relation.tupleCount])

  // Load data when a relation is selected
  useEffect(() => {
    handleRefresh()
  }, [relation.name])

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

  const handleCopy = async () => {
    const text = [currentColumns.join("\t"), ...currentData.map((row) => row.join("\t"))].join("\n")
    await navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const handleExport = () => {
    if (currentData.length === 0) return

    const csvContent = [
      currentColumns.join(","),
      ...currentData.map((row) => row.map((cell) => JSON.stringify(cell)).join(",")),
    ].join("\n")

    const blob = new Blob([csvContent], { type: "text/csv" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `${relation.name}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleSort = (colIndex: number) => {
    if (sortColumn === colIndex) {
      setSortDirection((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortColumn(colIndex)
      setSortDirection("asc")
    }
  }

  const filteredData = currentData.filter((row) =>
    row.some((cell) => String(cell).toLowerCase().includes(filter.toLowerCase())),
  )

  const sortedData =
    sortColumn !== null
      ? [...filteredData].sort((a, b) => {
          const aVal = a[sortColumn]
          const bVal = b[sortColumn]
          const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0
          return sortDirection === "asc" ? cmp : -cmp
        })
      : filteredData

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
            onClick={handleExport}
            disabled={currentData.length === 0}
            className="h-8 gap-1.5 bg-transparent"
          >
            <Download className="h-3.5 w-3.5" />
            Export
          </Button>
        </div>
      </div>

      {/* Schema */}
      <div className="border-b border-border/50 p-4">
        <h3 className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Schema</h3>
        <div className="flex flex-wrap gap-2">
          {currentColumns.map((col, index) => (
            <Badge key={`${col}-${index}`} variant="secondary" className="gap-1.5 font-mono text-xs">
              <span className="text-muted-foreground">{index}:</span>
              {col}
            </Badge>
          ))}
        </div>
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
        <div className="relative w-48">
          <Filter className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Filter data..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="h-7 pl-8 text-xs"
          />
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
                  className="cursor-pointer border-b border-r border-border/50 px-3 py-2 text-left text-[10px] font-semibold uppercase tracking-wider text-muted-foreground transition-colors hover:bg-muted last:border-r-0"
                >
                  <div className="flex items-center gap-1">
                    {col}
                    <ArrowUpDown className={cn("h-3 w-3", sortColumn === index ? "text-primary" : "opacity-30")} />
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedData.map((row, rowIndex) => (
              <tr key={rowIndex} className="group transition-colors hover:bg-muted/50">
                <td className="border-b border-r border-border/30 px-3 py-2 text-center font-mono text-[10px] text-muted-foreground">
                  {rowIndex + 1}
                </td>
                {row.map((cell, cellIndex) => (
                  <td
                    key={cellIndex}
                    className="border-b border-r border-border/30 px-3 py-2 font-mono text-xs last:border-r-0"
                  >
                    {typeof cell === "boolean" ? (
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
                      <span className="text-chart-4">{cell}</span>
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

      {/* Footer */}
      <div className="border-t border-border/50 bg-muted/30 px-4 py-2">
        <p className="text-[10px] text-muted-foreground">
          Showing {sortedData.length} of {currentTupleCount.toLocaleString()} tuples
          {filter && ` (filtered)`}
        </p>
      </div>
    </div>
  )
}
