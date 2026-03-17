"use client"

import { useState, useMemo } from "react"
import { Search, X, Network, Loader2, Eye } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Checkbox } from "@/components/ui/checkbox"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import { useDebounce } from "@/hooks/use-debounce"
import type { Relation } from "@/lib/datalog-store"
import type { GraphStats } from "@/lib/graph-utils"

interface GraphSidebarProps {
  relations: Relation[]
  selectedNames: Set<string>
  onToggleRelation: (name: string) => void
  onSelectAll: () => void
  onDeselectAll: () => void
  loadingRelations: Set<string>
  stats: GraphStats | null
}

export function GraphSidebar({
  relations,
  selectedNames,
  onToggleRelation,
  onSelectAll,
  onDeselectAll,
  loadingRelations,
  stats,
}: GraphSidebarProps) {
  const [search, setSearch] = useState("")
  const debouncedSearch = useDebounce(search, 150)

  const graphRelations = useMemo(
    () => relations.filter((r) => r.arity >= 1),
    [relations]
  )

  const filtered = useMemo(
    () =>
      graphRelations.filter((r) =>
        r.name.toLowerCase().includes(debouncedSearch.toLowerCase())
      ),
    [graphRelations, debouncedSearch]
  )

  return (
    <div className="flex h-full flex-col">
      {/* Header with select all/none */}
      <div className="p-2 border-b border-border/50 flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider px-2">
          Relations
        </span>
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" className="h-6 px-1.5 text-[10px]" onClick={onSelectAll}>
            All
          </Button>
          <Button variant="ghost" size="sm" className="h-6 px-1.5 text-[10px]" onClick={onDeselectAll}>
            None
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="border-b border-border/50 p-2">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Filter relations..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 pl-8 pr-8 text-xs"
            aria-label="Filter graph relations"
          />
          {search && (
            <button
              onClick={() => setSearch("")}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* Relation list */}
      <div className="flex-1 min-h-0 overflow-auto scrollbar-thin p-2">
        {graphRelations.length === 0 ? (
          <div className="py-8 text-center">
            <Network className="mx-auto h-8 w-8 text-muted-foreground/50" />
            <p className="mt-2 text-xs text-muted-foreground">No relations</p>
            <p className="mt-1 text-[10px] text-muted-foreground/70">
              Relations are visualized as graphs
            </p>
          </div>
        ) : filtered.length === 0 ? (
          <div className="py-8 text-center">
            <Search className="mx-auto h-6 w-6 text-muted-foreground/30" />
            <p className="mt-2 text-xs text-muted-foreground">No relations match &quot;{debouncedSearch}&quot;</p>
          </div>
        ) : (
          <div className="space-y-0.5">
            {filtered.map((rel) => {
              const isSelected = selectedNames.has(rel.name)
              const isLoading = loadingRelations.has(rel.name)
              return (
                <label
                  key={rel.name}
                  className={cn(
                    "flex items-center gap-2 rounded px-2 py-1.5 cursor-pointer transition-colors",
                    isSelected ? "bg-primary/5" : "hover:bg-muted"
                  )}
                >
                  {isLoading ? (
                    <Loader2 className="h-4 w-4 animate-spin text-muted-foreground flex-shrink-0" />
                  ) : (
                    <Checkbox
                      checked={isSelected}
                      onCheckedChange={() => onToggleRelation(rel.name)}
                    />
                  )}
                  {rel.isView ? (
                    <Eye className="h-3.5 w-3.5 flex-shrink-0 text-fuchsia-500" />
                  ) : (
                    <Network className="h-3.5 w-3.5 flex-shrink-0 text-teal-500" />
                  )}
                  <span className="flex-1 truncate font-mono text-xs">{rel.name}</span>
                  <Badge variant="secondary" className="text-[10px] px-1.5 h-4">
                    {rel.tupleCount}
                  </Badge>
                </label>
              )
            })}
          </div>
        )}
      </div>

      {/* Legend + Stats footer */}
      <div className="border-t border-border/50 p-3">
        <div className="flex items-center gap-4 mb-2">
          <div className="flex items-center gap-1.5">
            <Network className="h-3 w-3 text-teal-500" />
            <span className="text-[10px] text-muted-foreground">Relation</span>
          </div>
          <div className="flex items-center gap-1.5">
            <Eye className="h-3 w-3 text-fuchsia-500" />
            <span className="text-[10px] text-muted-foreground">Rule / View</span>
          </div>
        </div>
        <div className="flex flex-col gap-1 text-[10px] text-muted-foreground">
          <div className="flex items-center justify-between">
            <span>{graphRelations.length} relations</span>
            <span>{selectedNames.size} selected</span>
          </div>
          {stats && (
            <div className="flex items-center justify-between">
              <span>{stats.nodeCount} nodes</span>
              <span>{stats.edgeCount} edges</span>
            </div>
          )}
          {stats?.truncated && (
            <span className="text-amber-600 dark:text-amber-400">
              Graph truncated for performance
            </span>
          )}
        </div>
      </div>
    </div>
  )
}
