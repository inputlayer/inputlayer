"use client"

import { useState } from "react"
import { History, Code2, Clock, Search, X } from "lucide-react"
import { useDatalogStore } from "@/lib/datalog-store"
import { formatDistanceToNow } from "date-fns"
import { cn } from "@/lib/utils"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"

interface QuerySidebarProps {
  onSelectQuery: (query: string) => void
}

const SNIPPETS = [
  {
    name: "Find All Tuples",
    description: "Retrieve all tuples from a relation",
    code: "?- relation(X, Y, Z).",
  },
  {
    name: "Filtered Query",
    description: "Query with conditions",
    code: "?- user(Id, Name, _), Id > 10.",
  },
  {
    name: "Join Relations",
    description: "Combine multiple relations",
    code: "?- r1(X, Y), r2(Y, Z).",
  },
  {
    name: "Recursive Path",
    description: "Transitive closure pattern",
    code: `path(X, Y) :- edge(X, Y).
path(X, Y) :- edge(X, Z), path(Z, Y).
?- path(A, B).`,
  },
  {
    name: "Aggregation",
    description: "Count matching tuples",
    code: "count(N) :- N = #count { X : user(X, _, _) }.",
  },
]

export function QuerySidebar({ onSelectQuery }: QuerySidebarProps) {
  const [activeTab, setActiveTab] = useState<"history" | "snippets">("history")
  const [search, setSearch] = useState("")
  const { queryHistory } = useDatalogStore()

  const filteredHistory = queryHistory.filter(
    (item) => item.query && item.query.toLowerCase().includes(search.toLowerCase()),
  )

  const filteredSnippets = SNIPPETS.filter(
    (s) => s.name.toLowerCase().includes(search.toLowerCase()) || s.code.toLowerCase().includes(search.toLowerCase()),
  )

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
            placeholder="Search..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 pl-8 pr-8 text-xs"
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
        </button>
        <button
          onClick={() => setActiveTab("snippets")}
          className={cn(
            "flex flex-1 items-center justify-center gap-1.5 py-2 text-xs font-medium transition-colors",
            activeTab === "snippets"
              ? "border-b-2 border-primary text-primary"
              : "text-muted-foreground hover:text-foreground",
          )}
        >
          <Code2 className="h-3.5 w-3.5" />
          Snippets
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-auto scrollbar-thin">
        {activeTab === "history" ? (
          <div className="space-y-1 p-2">
            {filteredHistory.length === 0 ? (
              <div className="py-8 text-center">
                <Clock className="mx-auto h-8 w-8 text-muted-foreground/50" />
                <p className="mt-2 text-xs text-muted-foreground">
                  {search ? "No matching queries" : "No query history"}
                </p>
              </div>
            ) : (
              filteredHistory.map((item, index) => (
                <button
                  key={index}
                  onClick={() => onSelectQuery(item.query)}
                  className="group w-full rounded-md p-2 text-left transition-colors hover:bg-muted"
                >
                  <div className="flex items-start justify-between gap-2">
                    <pre className="flex-1 truncate font-mono text-[11px] text-foreground">
                      {item.query.split("\n")[0]}
                    </pre>
                    <Badge
                      variant="outline"
                      className={cn(
                        "flex-shrink-0 text-[9px]",
                        item.status === "success"
                          ? "border-success/50 text-success"
                          : "border-destructive/50 text-destructive",
                      )}
                    >
                      {item.status === "success" ? "OK" : "ERR"}
                    </Badge>
                  </div>
                  <p className="mt-1 text-[10px] text-muted-foreground">{formatTimestamp(item.timestamp)}</p>
                </button>
              ))
            )}
          </div>
        ) : (
          <div className="space-y-1 p-2">
            {filteredSnippets.map((snippet) => (
              <button
                key={snippet.name}
                onClick={() => onSelectQuery(snippet.code)}
                className="group w-full rounded-md p-2.5 text-left transition-colors hover:bg-muted"
              >
                <p className="text-xs font-medium">{snippet.name}</p>
                <p className="mt-0.5 text-[10px] text-muted-foreground">{snippet.description}</p>
                <pre className="mt-2 overflow-hidden rounded bg-[var(--code-bg)] p-2 font-mono text-[10px] text-muted-foreground">
                  {snippet.code.length > 50 ? snippet.code.slice(0, 50) + "..." : snippet.code}
                </pre>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
