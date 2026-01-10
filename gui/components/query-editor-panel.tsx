"use client"

import type React from "react"

import { useState, useRef } from "react"
import { Play, Copy, Check, Trash2, Sparkles, ChevronDown, Lightbulb } from "lucide-react"
import { Button } from "@/components/ui/button"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { cn } from "@/lib/utils"

interface QueryEditorPanelProps {
  onExecute: (query: string) => void
  onExplain?: (query: string) => void
  isExecuting: boolean
  isExplaining?: boolean
}

const EXAMPLE_QUERIES = [
  { label: "Query all edges", code: "?- edge(X, Y)." },
  { label: "Transitive closure", code: "path(X, Y) :- edge(X, Y).\npath(X, Y) :- edge(X, Z), path(Z, Y).\n?- path(X, Y)." },
  { label: "List all relations", code: "?- $relation(Name, Arity)." },
]

export function QueryEditorPanel({ onExecute, onExplain, isExecuting, isExplaining = false }: QueryEditorPanelProps) {
  const [query, setQuery] = useState(`% Datalog Query
?- edge(X, Y).`)
  const [copied, setCopied] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const lineNumbersRef = useRef<HTMLDivElement>(null)

  const lines = query.split("\n")
  const lineCount = lines.length

  const handleExecute = () => {
    if (query.trim() && !isExecuting) {
      onExecute(query)
    }
  }

  const handleExplain = () => {
    if (query.trim() && !isExplaining && onExplain) {
      onExplain(query)
    }
  }

  const handleCopy = async () => {
    await navigator.clipboard.writeText(query)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault()
      handleExecute()
    }
    // Tab support
    if (e.key === "Tab") {
      e.preventDefault()
      const start = e.currentTarget.selectionStart
      const end = e.currentTarget.selectionEnd
      setQuery(query.substring(0, start) + "  " + query.substring(end))
      setTimeout(() => {
        if (textareaRef.current) {
          textareaRef.current.selectionStart = textareaRef.current.selectionEnd = start + 2
        }
      }, 0)
    }
  }

  // Sync scroll between textarea and line numbers
  const handleScroll = () => {
    if (textareaRef.current && lineNumbersRef.current) {
      lineNumbersRef.current.scrollTop = textareaRef.current.scrollTop
    }
  }

  return (
    <div className="flex h-full flex-col">
      {/* Editor toolbar */}
      <div className="flex h-9 items-center justify-between border-b border-border/50 bg-background px-2">
        <div className="flex items-center gap-1">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm" className="h-7 gap-1 px-2 text-xs">
                <Sparkles className="h-3 w-3" />
                Examples
                <ChevronDown className="h-3 w-3 opacity-50" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="w-64">
              {EXAMPLE_QUERIES.map((ex) => (
                <DropdownMenuItem
                  key={ex.label}
                  onClick={() => setQuery(ex.code)}
                  className="flex-col items-start gap-1"
                >
                  <span className="font-medium">{ex.label}</span>
                  <code className="text-[10px] text-muted-foreground font-mono truncate w-full">
                    {ex.code.split("\n")[0]}
                  </code>
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>

        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" onClick={handleCopy} className="h-7 w-7 p-0">
            {copied ? <Check className="h-3.5 w-3.5 text-success" /> : <Copy className="h-3.5 w-3.5" />}
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setQuery("")} className="h-7 w-7 p-0">
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
          <div className="mx-1 h-4 w-px bg-border" />
          {onExplain && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleExplain}
              disabled={isExplaining || !query.trim()}
              className="h-7 gap-1.5 px-3 text-xs"
            >
              <Lightbulb className={cn("h-3 w-3", isExplaining && "animate-pulse")} />
              {isExplaining ? "Explaining..." : "Explain"}
            </Button>
          )}
          <Button
            size="sm"
            onClick={handleExecute}
            disabled={isExecuting || !query.trim()}
            className="h-7 gap-1.5 px-3 text-xs"
          >
            <Play className={cn("h-3 w-3", isExecuting && "animate-pulse")} />
            {isExecuting ? "Running..." : "Run"}
            <kbd className="ml-1 hidden rounded bg-primary-foreground/20 px-1 py-0.5 text-[10px] font-medium sm:inline">
              ⌘↵
            </kbd>
          </Button>
        </div>
      </div>

      {/* Code editor area */}
      <div className="relative flex flex-1 overflow-hidden bg-[var(--code-bg)]">
        {/* Line numbers */}
        <div
          ref={lineNumbersRef}
          className="flex-shrink-0 select-none overflow-hidden border-r border-border/30 bg-[var(--code-bg)] py-3 text-right font-mono text-xs leading-6"
          style={{ width: "3rem" }}
        >
          {Array.from({ length: lineCount }, (_, i) => (
            <div key={i + 1} className="px-2 text-[var(--code-line-number)]">
              {i + 1}
            </div>
          ))}
        </div>

        {/* Editor textarea */}
        <textarea
          ref={textareaRef}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          onScroll={handleScroll}
          spellCheck={false}
          className="flex-1 resize-none bg-transparent p-3 font-mono text-sm leading-6 text-foreground outline-none placeholder:text-muted-foreground scrollbar-thin"
          placeholder="Enter your Datalog query..."
        />
      </div>

      {/* Status bar */}
      <div className="flex h-6 items-center justify-between border-t border-border/50 bg-muted/30 px-3 text-[10px] text-muted-foreground">
        <span>
          Ln {lineCount}, Col {query.length - query.lastIndexOf("\n")}
        </span>
        <span>{query.length} characters</span>
      </div>
    </div>
  )
}
