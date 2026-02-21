"use client"

import type React from "react"

import { useState, useRef, useCallback } from "react"
import { Play, Copy, Check, Trash2, Sparkles, ChevronDown, Lightbulb } from "lucide-react"
import { Button } from "@/components/ui/button"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { cn } from "@/lib/utils"
import { useDatalogStore } from "@/lib/datalog-store"
import { AutocompletePopup } from "@/components/autocomplete-popup"
import { getCompletions, getCursorCoordinates, type CompletionItem } from "@/lib/autocomplete"

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

const MAX_COMPLETIONS = 30
const MAX_COMPLETIONS_FORCED = 100

export function QueryEditorPanel({ onExecute, onExplain, isExecuting, isExplaining = false }: QueryEditorPanelProps) {
  const [query, setQuery] = useState(`% Datalog Query
?- edge(X, Y).`)
  const [copied, setCopied] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const lineNumbersRef = useRef<HTMLDivElement>(null)
  const editorAreaRef = useRef<HTMLDivElement>(null)

  // Autocomplete state
  const [completionItems, setCompletionItems] = useState<CompletionItem[]>([])
  const [completionStartIndex, setCompletionStartIndex] = useState(0)
  const [selectedIndex, setSelectedIndex] = useState(0)
  const [popupPosition, setPopupPosition] = useState({ top: 0, left: 0 })
  const showAutocomplete = completionItems.length > 0

  const { relations, views } = useDatalogStore()

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

  const dismissAutocomplete = useCallback(() => {
    setCompletionItems([])
    setSelectedIndex(0)
  }, [])

  const acceptCompletion = useCallback((item: CompletionItem) => {
    if (!textareaRef.current) return

    const newQuery =
      query.substring(0, completionStartIndex) +
      item.insertText +
      query.substring(textareaRef.current.selectionStart)

    setQuery(newQuery)
    dismissAutocomplete()

    // Restore cursor after inserted text
    const newPos = completionStartIndex + item.insertText.length
    setTimeout(() => {
      if (textareaRef.current) {
        textareaRef.current.selectionStart = newPos
        textareaRef.current.selectionEnd = newPos
        textareaRef.current.focus()
      }
    }, 0)
  }, [query, completionStartIndex, dismissAutocomplete])

  const updateCompletions = useCallback((text: string, cursorPos: number) => {
    const { items, startIndex } = getCompletions(text, cursorPos, relations, views)
    const limited = items.slice(0, MAX_COMPLETIONS)

    if (limited.length > 0 && textareaRef.current && editorAreaRef.current) {
      const coords = getCursorCoordinates(textareaRef.current, cursorPos)
      // Position relative to the editor area, offset by line numbers width (3rem = ~48px)
      const lineNumberWidth = 48
      setPopupPosition({
        top: coords.top + 24, // Below the current line
        left: coords.left + lineNumberWidth,
      })
      setCompletionItems(limited)
      setCompletionStartIndex(startIndex)
      setSelectedIndex(0)
    } else {
      dismissAutocomplete()
    }
  }, [relations, views, dismissAutocomplete])

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newQuery = e.target.value
    setQuery(newQuery)

    const cursorPos = e.target.selectionStart
    updateCompletions(newQuery, cursorPos)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    // Autocomplete keyboard handling (when popup is open)
    if (showAutocomplete) {
      if (e.key === "ArrowDown") {
        e.preventDefault()
        setSelectedIndex((prev) => (prev + 1) % completionItems.length)
        return
      }
      if (e.key === "ArrowUp") {
        e.preventDefault()
        setSelectedIndex((prev) => (prev - 1 + completionItems.length) % completionItems.length)
        return
      }
      if (e.key === "Enter" || e.key === "Tab") {
        e.preventDefault()
        acceptCompletion(completionItems[selectedIndex])
        return
      }
      if (e.key === "Escape") {
        e.preventDefault()
        dismissAutocomplete()
        return
      }
    }

    // Ctrl+A: force show all completions
    if (e.ctrlKey && e.key === "a") {
      e.preventDefault()
      if (textareaRef.current) {
        const cursorPos = textareaRef.current.selectionStart
        const { items, startIndex } = getCompletions(query, cursorPos, relations, views, true)
        const limited = items.slice(0, MAX_COMPLETIONS_FORCED)
        if (limited.length > 0 && editorAreaRef.current) {
          const coords = getCursorCoordinates(textareaRef.current, cursorPos)
          const lineNumberWidth = 48
          setPopupPosition({
            top: coords.top + 24,
            left: coords.left + lineNumberWidth,
          })
          setCompletionItems(limited)
          setCompletionStartIndex(startIndex)
          setSelectedIndex(0)
        }
      }
      return
    }

    // Normal editor keyboard handling
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault()
      handleExecute()
    }
    // Tab support (only when autocomplete is NOT open)
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
    // Dismiss autocomplete on scroll
    dismissAutocomplete()
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
      <div ref={editorAreaRef} className="relative flex flex-1 overflow-hidden bg-[var(--code-bg)]">
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
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onScroll={handleScroll}
          spellCheck={false}
          className="flex-1 resize-none bg-transparent p-3 font-mono text-sm leading-6 text-foreground outline-none placeholder:text-muted-foreground scrollbar-thin"
          placeholder="Enter your Datalog query..."
        />

        {/* Autocomplete popup */}
        {showAutocomplete && (
          <AutocompletePopup
            items={completionItems}
            selectedIndex={selectedIndex}
            position={popupPosition}
            onSelect={acceptCompletion}
            onSetSelected={setSelectedIndex}
          />
        )}
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
