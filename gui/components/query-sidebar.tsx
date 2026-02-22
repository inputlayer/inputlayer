"use client"

import { useState, useMemo } from "react"
import { History, BookOpen, Clock, Search, X, ChevronRight, CheckCircle2, XCircle, Play } from "lucide-react"
import { useDatalogStore } from "@/lib/datalog-store"
import { formatDistanceToNow } from "date-fns"
import { cn } from "@/lib/utils"
import { formatTime } from "@/lib/ui-utils"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { EXAMPLE_CATEGORIES, getAllExamples, getExampleCount } from "@/lib/examples"
import { highlightToHtml } from "@/lib/syntax-highlight"

interface QuerySidebarProps {
  onSelectQuery: (query: string) => void
  onLoadQuery?: (query: string) => void
}

const DIFFICULTY_COLORS = {
  beginner: "bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 border-emerald-500/30",
  intermediate: "bg-amber-500/15 text-amber-600 dark:text-amber-400 border-amber-500/30",
  advanced: "bg-violet-500/15 text-violet-600 dark:text-violet-400 border-violet-500/30",
} as const

export function QuerySidebar({ onSelectQuery, onLoadQuery }: QuerySidebarProps) {
  const [activeTab, setActiveTab] = useState<"history" | "examples">("examples")
  const [search, setSearch] = useState("")
  const [expandedCategory, setExpandedCategory] = useState<string | null>("getting-started")
  const { queryHistory } = useDatalogStore()

  const loadQuery = onLoadQuery ?? onSelectQuery

  const filteredHistory = queryHistory.filter(
    (item) => item.query && item.query.toLowerCase().includes(search.toLowerCase()),
  )

  // Search across all examples when search is active
  const allExamples = useMemo(() => getAllExamples(), [])
  const searchResults = search
    ? allExamples.filter(
        (ex) =>
          ex.name.toLowerCase().includes(search.toLowerCase()) ||
          ex.description.toLowerCase().includes(search.toLowerCase()) ||
          ex.code.toLowerCase().includes(search.toLowerCase()) ||
          ex.categoryName.toLowerCase().includes(search.toLowerCase()),
      )
    : []

  const formatTimestamp = (timestamp: Date | string) => {
    try {
      const date = typeof timestamp === "string" ? new Date(timestamp) : timestamp
      if (isNaN(date.getTime())) return "just now"
      return formatDistanceToNow(date, { addSuffix: true })
    } catch {
      return "just now"
    }
  }

  return (
    <div className="flex h-full flex-col">
      {/* Search */}
      <div className="flex-shrink-0 border-b border-border/50 p-2">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder={activeTab === "history" ? "Search history..." : "Search examples..."}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 pl-8 pr-8 text-xs"
            aria-label={activeTab === "history" ? "Search query history" : "Search examples"}
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

      {/* Tabs */}
      <div className="flex flex-shrink-0 border-b border-border/50">
        <button
          onClick={() => setActiveTab("history")}
          className={cn(
            "flex flex-1 items-center justify-center gap-1.5 py-2 text-xs font-medium transition-colors",
            activeTab === "history"
              ? "border-b-2 border-primary text-primary"
              : "text-muted-foreground hover:text-foreground",
          )}
        >
          <History className="h-3.5 w-3.5" />
          History
          {queryHistory.length > 0 && (
            <span className="text-[9px] text-muted-foreground">({queryHistory.length})</span>
          )}
        </button>
        <button
          onClick={() => setActiveTab("examples")}
          className={cn(
            "flex flex-1 items-center justify-center gap-1.5 py-2 text-xs font-medium transition-colors",
            activeTab === "examples"
              ? "border-b-2 border-primary text-primary"
              : "text-muted-foreground hover:text-foreground",
          )}
        >
          <BookOpen className="h-3.5 w-3.5" />
          Examples
          <span className="text-[9px] text-muted-foreground">({getExampleCount()})</span>
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-auto scrollbar-thin">
        {activeTab === "history" ? (
          <div className="space-y-1 p-2">
            {filteredHistory.length === 0 ? (
              <div className="py-8 text-center">
                <Clock className="mx-auto h-8 w-8 text-muted-foreground/50" />
                <p className="mt-2 text-xs text-muted-foreground">
                  {search ? "No matching queries" : "No query history yet"}
                </p>
                {!search && (
                  <p className="mt-1 text-[10px] text-muted-foreground/70">
                    Run a query and it will appear here
                  </p>
                )}
              </div>
            ) : (
              filteredHistory.map((item, index) => (
                <div
                  key={item.id ?? index}
                  className="group relative w-full rounded-md p-2 text-left transition-colors hover:bg-muted"
                >
                  <button
                    onClick={() => loadQuery(item.query)}
                    className="w-full text-left"
                  >
                    <div className="flex items-start justify-between gap-2">
                      <pre className="flex-1 truncate font-mono text-[11px] text-foreground">
                        {item.query.split("\n")[0]}
                      </pre>
                      <div className="flex items-center gap-1.5 flex-shrink-0">
                        {item.executionTime != null && (
                          <span className="text-[9px] text-muted-foreground tabular-nums">
                            {formatTime(item.executionTime)}
                          </span>
                        )}
                        {item.status === "success" ? (
                          <CheckCircle2 className="h-3 w-3 text-success" />
                        ) : (
                          <XCircle className="h-3 w-3 text-destructive" />
                        )}
                      </div>
                    </div>
                    <div className="mt-1 flex items-center gap-2 text-[10px] text-muted-foreground">
                      <span>{formatTimestamp(item.timestamp)}</span>
                      {item.query.includes("\n") && (
                        <span>{item.query.split("\n").length} lines</span>
                      )}
                      {item.data.length > 0 && (
                        <span>{item.data.length} rows</span>
                      )}
                    </div>
                  </button>
                  <button
                    onClick={(e) => { e.stopPropagation(); onSelectQuery(item.query) }}
                    className="absolute right-1.5 top-1.5 hidden rounded p-1 text-muted-foreground hover:bg-primary/10 hover:text-primary group-hover:block"
                    title="Re-run query"
                  >
                    <Play className="h-3 w-3" />
                  </button>
                </div>
              ))
            )}
          </div>
        ) : search ? (
          // Flat search results across all categories
          <div className="space-y-1 p-2">
            {searchResults.length === 0 ? (
              <div className="py-8 text-center">
                <Search className="mx-auto h-8 w-8 text-muted-foreground/50" />
                <p className="mt-2 text-xs text-muted-foreground">No matching examples</p>
              </div>
            ) : (
              <>
                <p className="px-1 pb-1 text-[10px] text-muted-foreground">
                  {searchResults.length} result{searchResults.length !== 1 ? "s" : ""}
                </p>
                {searchResults.map((ex) => (
                  <button
                    key={`${ex.categoryId}-${ex.name}`}
                    onClick={() => loadQuery(ex.code)}
                    className="group w-full rounded-md p-2.5 text-left transition-colors hover:bg-muted"
                  >
                    <div className="flex items-start justify-between gap-2">
                      <p className="text-xs font-medium">{ex.name}</p>
                      <Badge variant="outline" className={cn("text-[9px] px-1.5 py-0", DIFFICULTY_COLORS[ex.difficulty])}>
                        {ex.difficulty}
                      </Badge>
                    </div>
                    <p className="mt-0.5 text-[10px] text-muted-foreground">{ex.description}</p>
                    <p className="mt-1 text-[9px] text-muted-foreground/60">{ex.categoryName}</p>
                    <pre
                      className="mt-1.5 overflow-hidden rounded bg-[var(--code-bg)] p-2 font-mono text-[10px] whitespace-pre-wrap"
                      dangerouslySetInnerHTML={{ __html: highlightToHtml(ex.code.length > 80 ? ex.code.slice(0, 80) + "..." : ex.code) }}
                    />
                  </button>
                ))}
              </>
            )}
          </div>
        ) : (
          // Categorized examples browser
          <div className="p-2">
            {EXAMPLE_CATEGORIES.map((cat) => {
              const isExpanded = expandedCategory === cat.id
              return (
                <div key={cat.id} className="mb-1">
                  <button
                    onClick={() => setExpandedCategory(isExpanded ? null : cat.id)}
                    aria-expanded={isExpanded}
                    className="flex w-full items-center gap-1.5 rounded px-1.5 py-1.5 text-xs font-medium text-muted-foreground hover:bg-muted hover:text-foreground transition-colors"
                  >
                    <ChevronRight
                      className={cn("h-3.5 w-3.5 transition-transform flex-shrink-0", isExpanded && "rotate-90")}
                    />
                    <span className="flex-1 text-left">{cat.name}</span>
                    <Badge variant="secondary" className="text-[9px] px-1.5 h-4">
                      {cat.examples.length}
                    </Badge>
                  </button>

                  {isExpanded && (
                    <div className="ml-3 mt-0.5 space-y-0.5 border-l border-border/50 pl-2">
                      <p className="px-1.5 py-1 text-[10px] text-muted-foreground/70">{cat.description}</p>
                      {cat.examples.map((ex) => (
                        <button
                          key={ex.name}
                          onClick={() => loadQuery(ex.code)}
                          className="group w-full rounded-md p-2 text-left transition-colors hover:bg-muted"
                        >
                          <div className="flex items-start justify-between gap-2">
                            <p className="text-xs font-medium text-foreground">{ex.name}</p>
                            <Badge
                              variant="outline"
                              title={ex.difficulty}
                              className={cn("text-[9px] px-1 py-0 flex-shrink-0", DIFFICULTY_COLORS[ex.difficulty])}
                            >
                              {ex.difficulty === "beginner" ? "basic" : ex.difficulty === "intermediate" ? "mid" : "adv"}
                            </Badge>
                          </div>
                          <p className="mt-0.5 text-[10px] text-muted-foreground">{ex.description}</p>
                          <pre
                            className="mt-1.5 overflow-hidden rounded bg-[var(--code-bg)] p-2 font-mono text-[10px] whitespace-pre-wrap leading-relaxed"
                            dangerouslySetInnerHTML={{ __html: highlightToHtml(
                              ex.code.split("\n").slice(0, 3).join("\n") + (ex.code.split("\n").length > 3 ? "\n..." : "")
                            ) }}
                          />
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
