"use client"

import { useState, useMemo } from "react"
import { Network, Eye, Search, X, ChevronRight, Rows3 } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import { EmptyState } from "@/components/empty-state"
import { useDebounce } from "@/hooks/use-debounce"
import type { Relation, View } from "@/lib/datalog-store"

interface RelationsExplorerProps {
  relations: Relation[]
  views: View[]
  selectedRelationId: string | undefined
  selectedViewId: string | undefined
  onSelectRelation: (relation: Relation) => void
  onSelectView: (view: View) => void
}

export function RelationsExplorer({
  relations,
  views,
  selectedRelationId,
  selectedViewId,
  onSelectRelation,
  onSelectView,
}: RelationsExplorerProps) {
  const [search, setSearch] = useState("")
  const debouncedSearch = useDebounce(search, 150)
  const [expandedSections, setExpandedSections] = useState({
    relations: true,
    views: true,
  })

  const filteredRelations = useMemo(() =>
    relations.filter((r) => r.name.toLowerCase().includes(debouncedSearch.toLowerCase())),
    [relations, debouncedSearch]
  )
  const filteredViews = useMemo(() =>
    views.filter((v) => v.name.toLowerCase().includes(debouncedSearch.toLowerCase())),
    [views, debouncedSearch]
  )

  const toggleSection = (section: "relations" | "views") => {
    setExpandedSections((prev) => ({ ...prev, [section]: !prev[section] }))
  }

  return (
    <div className="flex h-full flex-col">
      {/* Search */}
      <div className="border-b border-border/50 p-2">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Filter..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 pl-8 pr-8 text-xs"
            aria-label="Filter relations and views"
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

      {/* Tree */}
      <div className="flex-1 overflow-auto scrollbar-thin p-2">
        {/* Empty state when filter matches nothing */}
        {debouncedSearch && filteredRelations.length === 0 && filteredViews.length === 0 && (
          <EmptyState icon={Search} title="No matches" subtitle="Try a different filter" />
        )}

        {/* Relations section */}
        <div className="mb-2">
          <button
            onClick={() => toggleSection("relations")}
            aria-expanded={expandedSections.relations}
            className="flex w-full items-center gap-1 rounded px-1 py-1.5 text-xs font-medium text-muted-foreground hover:bg-muted hover:text-foreground"
          >
            <ChevronRight
              className={cn("h-3.5 w-3.5 transition-transform", expandedSections.relations && "rotate-90")}
            />
            <Network className="h-3.5 w-3.5 text-primary" />
            Relations
            <Badge variant="secondary" className="ml-auto text-[10px] px-1.5">
              {filteredRelations.length}
            </Badge>
          </button>

          {expandedSections.relations && (
            <div className="ml-3 mt-1 space-y-0.5 border-l border-border/50 pl-2">
              {filteredRelations.map((relation) => (
                <button
                  key={relation.id}
                  onClick={() => onSelectRelation(relation)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left transition-colors",
                    selectedRelationId === relation.id
                      ? "bg-primary/10 text-primary"
                      : "text-foreground hover:bg-muted",
                  )}
                >
                  <Network className="h-3.5 w-3.5 flex-shrink-0 text-chart-1" />
                  <span className="flex-1 truncate font-mono text-xs">{relation.name}</span>
                  {relation.isSession && (
                    <Badge variant="outline" className="h-4 px-1 text-[9px] border-amber-500/50 text-amber-600 dark:text-amber-400 bg-amber-500/10">
                      session
                    </Badge>
                  )}
                  <span className="flex-shrink-0 text-[10px] text-muted-foreground">{relation.arity}</span>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Views section */}
        <div>
          <button
            onClick={() => toggleSection("views")}
            aria-expanded={expandedSections.views}
            className="flex w-full items-center gap-1 rounded px-1 py-1.5 text-xs font-medium text-muted-foreground hover:bg-muted hover:text-foreground"
          >
            <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", expandedSections.views && "rotate-90")} />
            <Eye className="h-3.5 w-3.5 text-accent" />
            Rules
            <Badge variant="secondary" className="ml-auto text-[10px] px-1.5">
              {filteredViews.length}
            </Badge>
          </button>

          {expandedSections.views && (
            <div className="ml-3 mt-1 space-y-0.5 border-l border-border/50 pl-2">
              {filteredViews.map((view) => (
                <button
                  key={view.id}
                  onClick={() => onSelectView(view)}
                  className={cn(
                    "flex w-full items-center gap-2 rounded px-2 py-1.5 text-left transition-colors",
                    selectedViewId === view.id ? "bg-accent/10 text-accent" : "text-foreground hover:bg-muted",
                  )}
                >
                  <Eye className="h-3.5 w-3.5 flex-shrink-0 text-chart-2" />
                  <span className="flex-1 truncate font-mono text-xs">{view.name}</span>
                  {view.isSession && (
                    <Badge variant="outline" className="h-4 px-1 text-[9px] border-amber-500/50 text-amber-600 dark:text-amber-400 bg-amber-500/10">
                      session
                    </Badge>
                  )}
                  <span className="flex-shrink-0 text-[10px] text-muted-foreground">{view.arity > 0 ? view.arity : ""}</span>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Stats */}
      <div className="border-t border-border/50 p-3">
        <div className="flex items-center justify-between text-[10px] text-muted-foreground">
          <span className="flex items-center gap-1">
            <Rows3 className="h-3 w-3" />
            {relations.reduce((acc, r) => acc + r.tupleCount, 0).toLocaleString()} tuples
          </span>
          <span>{relations.length + views.length} objects</span>
        </div>
      </div>
    </div>
  )
}
