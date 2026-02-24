"use client"

import type React from "react"

import { useState, useRef, useCallback, useMemo, useEffect, useSyncExternalStore } from "react"
import { Play, Copy, Check, Trash2, Lightbulb, Square } from "lucide-react"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import { useDatalogStore } from "@/lib/datalog-store"
import { AutocompletePopup } from "@/components/autocomplete-popup"
import { KeyboardShortcutsDialog } from "@/components/keyboard-shortcuts-dialog"
import { getCompletions, getCursorCoordinates, type CompletionItem } from "@/lib/autocomplete"
import { highlightToHtml } from "@/lib/syntax-highlight"
import { classifyLines, getStatementLabel, type StatementType } from "@/lib/statement-classifier"

interface QueryEditorPanelProps {
  onExecute: (query: string) => void
  onExplain?: (query: string) => void
  onCancel?: () => void
  isExecuting: boolean
  isExplaining?: boolean
  errorLines?: Set<number>
}

const MAX_COMPLETIONS = 30
const MAX_COMPLETIONS_FORCED = 100

// SSR-safe platform detection
const subscribePlatform = () => () => {}
const getIsMac = () => typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform)
const getIsMacServer = () => false

const GUTTER_WIDTH = "4rem"
const LINE_NUMBER_WIDTH_PX = 64

const STATEMENT_COLORS: Record<NonNullable<StatementType>, string> = {
  "persistent-rule": "bg-blue-500",
  "session-rule": "bg-amber-500",
  "insert": "bg-emerald-500",
  "delete": "bg-red-500",
  "query": "bg-cyan-500",
  "meta": "bg-violet-500",
  "session-fact": "bg-amber-500",
  "schema": "bg-blue-400",
  "comment": "bg-muted-foreground/30",
}

export function QueryEditorPanel({ onExecute, onExplain, onCancel, isExecuting, isExplaining = false, errorLines }: QueryEditorPanelProps) {
  const isMac = useSyncExternalStore(subscribePlatform, getIsMac, getIsMacServer)
  const { editorContent, setEditorContent, selectedKnowledgeGraph, relations, views } = useDatalogStore()
  const [query, setQuery] = useState(() => editorContent || "")
  const [copied, setCopied] = useState(false)
  const [cursorPos, setCursorPos] = useState({ line: 1, col: 1 })
  const [selectionLength, setSelectionLength] = useState(0)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const highlightRef = useRef<HTMLPreElement>(null)
  const lineNumbersRef = useRef<HTMLDivElement>(null)
  const editorAreaRef = useRef<HTMLDivElement>(null)

  // Autocomplete state (batched to avoid desync during fast typing)
  const [autocomplete, setAutocomplete] = useState<{
    items: CompletionItem[]
    startIndex: number
    selectedIndex: number
    position: { top: number; left: number }
  }>({ items: [], startIndex: 0, selectedIndex: 0, position: { top: 0, left: 0 } })
  const showAutocomplete = autocomplete.items.length > 0

  // Debounced sync of query text back to store for persistence across navigation
  useEffect(() => {
    const t = setTimeout(() => setEditorContent(query), 300)
    return () => clearTimeout(t)
  }, [query, setEditorContent])

  // Sync editor when store is updated externally (e.g., clicking history item)
  useEffect(() => {
    if (editorContent !== null && editorContent !== query) {
      setQuery(editorContent)
    }
  }, [editorContent]) // eslint-disable-line react-hooks/exhaustive-deps

  const highlightedHtml = useMemo(() => highlightToHtml(query), [query])

  const lines = query.split("\n")
  const lineCount = lines.length

  // Statement type classification (Phase 2)
  const lineTypes = useMemo(() => classifyLines(query), [query])

  // Cursor position tracking - reads from textarea.value directly to avoid stale closures
  const updateCursorInfo = useCallback(() => {
    if (!textareaRef.current) return
    const ta = textareaRef.current
    const pos = ta.selectionStart
    const end = ta.selectionEnd
    const text = ta.value
    const before = text.substring(0, pos)
    const line = (before.match(/\n/g) || []).length + 1
    const col = pos - before.lastIndexOf("\n")
    setCursorPos({ line, col })
    setSelectionLength(end - pos)
  }, [])

  const getExecutionText = useCallback(() => {
    if (!textareaRef.current) return query
    const { selectionStart, selectionEnd } = textareaRef.current
    if (selectionStart !== selectionEnd) {
      return query.substring(selectionStart, selectionEnd)
    }
    return query
  }, [query])

  const handleExecute = () => {
    const text = getExecutionText()
    if (text.trim() && !isExecuting) {
      onExecute(text)
    }
  }

  const handleExplain = () => {
    const text = getExecutionText()
    if (text.trim() && !isExplaining && onExplain) {
      onExplain(text)
    }
  }

  const handleCopy = async () => {
    await navigator.clipboard.writeText(query)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const dismissAutocomplete = useCallback(() => {
    setAutocomplete((prev) => ({ ...prev, items: [], selectedIndex: 0 }))
  }, [])

  const acceptCompletion = useCallback((item: CompletionItem) => {
    if (!textareaRef.current) return

    const si = autocomplete.startIndex

    // Indentation-aware insertion (Phase 5A)
    const lineStart = query.lastIndexOf("\n", si - 1) + 1
    const indent = query.substring(lineStart, si).match(/^(\s*)/)?.[1] || ""
    let text = item.insertText
    if (text.includes("\n")) {
      text = text.split("\n").map((l, i) => i === 0 ? l : indent + l).join("\n")
    }

    const newQuery =
      query.substring(0, si) +
      text +
      query.substring(textareaRef.current.selectionStart)

    setQuery(newQuery)
    dismissAutocomplete()

    // Restore cursor after inserted text
    const newPos = si + text.length
    setTimeout(() => {
      if (textareaRef.current) {
        textareaRef.current.selectionStart = newPos
        textareaRef.current.selectionEnd = newPos
        textareaRef.current.focus()
      }
    }, 0)
  }, [query, autocomplete.startIndex, dismissAutocomplete])

  const updateCompletions = useCallback((text: string, cursorPosition: number) => {
    const { items, startIndex } = getCompletions(text, cursorPosition, relations, views)
    const limited = items.slice(0, MAX_COMPLETIONS)

    if (limited.length > 0 && textareaRef.current && editorAreaRef.current) {
      const coords = getCursorCoordinates(textareaRef.current, cursorPosition)
      setAutocomplete({
        items: limited,
        startIndex,
        selectedIndex: 0,
        position: { top: coords.top + 24, left: coords.left + LINE_NUMBER_WIDTH_PX },
      })
    } else {
      dismissAutocomplete()
    }
  }, [relations, views, dismissAutocomplete])

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newQuery = e.target.value
    setQuery(newQuery)

    const cp = e.target.selectionStart
    updateCompletions(newQuery, cp)
    updateCursorInfo()
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    // Autocomplete keyboard handling (when popup is open)
    if (showAutocomplete) {
      if (e.key === "ArrowDown") {
        e.preventDefault()
        setAutocomplete((prev) => ({ ...prev, selectedIndex: (prev.selectedIndex + 1) % prev.items.length }))
        return
      }
      if (e.key === "ArrowUp") {
        e.preventDefault()
        setAutocomplete((prev) => ({ ...prev, selectedIndex: (prev.selectedIndex - 1 + prev.items.length) % prev.items.length }))
        return
      }
      if (e.key === "Enter" || e.key === "Tab") {
        e.preventDefault()
        acceptCompletion(autocomplete.items[autocomplete.selectedIndex])
        return
      }
      if (e.key === "Escape") {
        e.preventDefault()
        dismissAutocomplete()
        return
      }
    }

    // Ctrl+Space: open autocomplete manually (or Ctrl+Shift+A when already open to show all)
    if (e.ctrlKey && (e.key === " " || (e.shiftKey && e.key === "A" && showAutocomplete))) {
      e.preventDefault()
      if (textareaRef.current) {
        const cp = textareaRef.current.selectionStart
        const forceAll = e.key === "A"
        const { items, startIndex } = getCompletions(query, cp, relations, views, forceAll)
        const limited = items.slice(0, forceAll ? MAX_COMPLETIONS_FORCED : MAX_COMPLETIONS)
        if (limited.length > 0 && editorAreaRef.current) {
          const coords = getCursorCoordinates(textareaRef.current, cp)
          setAutocomplete({
            items: limited,
            startIndex,
            selectedIndex: 0,
            position: { top: coords.top + 24, left: coords.left + LINE_NUMBER_WIDTH_PX },
          })
        }
      }
      return
    }

    // Cmd+Shift+Enter explains
    if ((e.metaKey || e.ctrlKey) && e.shiftKey && e.key === "Enter") {
      e.preventDefault()
      handleExplain()
      return
    }

    // Cmd+Enter executes
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault()
      handleExecute()
      return
    }

    // Auto-indent on Enter
    if (e.key === "Enter" && !showAutocomplete) {
      e.preventDefault()
      const ta = textareaRef.current
      if (!ta) return
      const pos = ta.selectionStart
      const before = query.substring(0, pos)

      // Current line's whitespace prefix
      const lineStart = before.lastIndexOf("\n") + 1
      const currentLine = before.substring(lineStart)
      const indent = currentLine.match(/^(\s*)/)?.[1] || ""

      // Extra indent if CURRENT LINE ends with `<-` (with optional trailing whitespace)
      const currentLineTrimmed = currentLine.trimEnd()
      const extraIndent = currentLineTrimmed.endsWith("<-") ? "  " : ""

      const insertion = "\n" + indent + extraIndent
      const newQuery = before + insertion + query.substring(pos)
      setQuery(newQuery)

      const newPos = pos + insertion.length
      setTimeout(() => {
        if (ta) {
          ta.selectionStart = ta.selectionEnd = newPos
          updateCursorInfo()
        }
      }, 0)
      return
    }

    // Tab support (only when autocomplete is NOT open)
    if (e.key === "Tab" && textareaRef.current) {
      e.preventDefault()
      const start = textareaRef.current.selectionStart
      const end = textareaRef.current.selectionEnd
      setQuery(query.substring(0, start) + "  " + query.substring(end))
      setTimeout(() => {
        if (textareaRef.current) {
          textareaRef.current.selectionStart = textareaRef.current.selectionEnd = start + 2
        }
      }, 0)
    }
  }

  // Sync scroll between textarea, line numbers, and highlight overlay
  const handleScroll = () => {
    if (textareaRef.current) {
      if (lineNumbersRef.current) {
        lineNumbersRef.current.scrollTop = textareaRef.current.scrollTop
      }
      if (highlightRef.current) {
        highlightRef.current.scrollTop = textareaRef.current.scrollTop
        highlightRef.current.scrollLeft = textareaRef.current.scrollLeft
      }
    }
    // Dismiss autocomplete on scroll
    dismissAutocomplete()
  }

  return (
    <div className="flex h-full flex-col">
      {/* Editor toolbar */}
      <div className="flex h-9 items-center justify-between border-b border-border/50 bg-background px-2">
        <div className="flex items-center gap-1">
          <KeyboardShortcutsDialog />
        </div>

        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" onClick={handleCopy} className="h-7 w-7 p-0" aria-label={copied ? "Copied" : "Copy query"}>
            {copied ? <Check className="h-3.5 w-3.5 text-success" /> : <Copy className="h-3.5 w-3.5" />}
          </Button>
          <Button variant="ghost" size="sm" onClick={() => setQuery("")} className="h-7 w-7 p-0" aria-label="Clear editor">
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
          {isExecuting && onCancel ? (
            <Button
              size="sm"
              variant="destructive"
              onClick={onCancel}
              className="h-7 gap-1.5 px-3 text-xs"
            >
              <Square className="h-3 w-3" />
              Cancel
            </Button>
          ) : (
            <Button
              size="sm"
              onClick={handleExecute}
              disabled={isExecuting || !query.trim()}
              className="h-7 gap-1.5 px-3 text-xs"
            >
              <Play className={cn("h-3 w-3", isExecuting && "animate-pulse")} />
              {isExecuting ? "Running..." : selectionLength > 0 ? "Run Selection" : "Run"}
              <kbd className="ml-1 hidden rounded bg-primary-foreground/20 px-1 py-0.5 text-[10px] font-medium sm:inline">
                {isMac ? "⌘↵" : "Ctrl+↵"}
              </kbd>
            </Button>
          )}
        </div>
      </div>

      {/* Code editor area */}
      <div ref={editorAreaRef} className="relative flex flex-1 overflow-hidden bg-[var(--code-bg)]">
        {/* Line numbers with statement type indicators */}
        <div
          ref={lineNumbersRef}
          className="flex-shrink-0 select-none overflow-hidden border-r border-border/30 bg-[var(--code-bg)] py-3 text-right font-mono text-xs leading-6"
          style={{ width: GUTTER_WIDTH }}
        >
          {Array.from({ length: lineCount }, (_, i) => {
            const lineNum = i + 1
            const hasError = errorLines?.has(lineNum)
            const stmtType = lineTypes[i]
            const colorClass = stmtType ? STATEMENT_COLORS[stmtType] : null
            return (
              <div
                key={lineNum}
                className={cn(
                  "flex items-center text-[var(--code-line-number)]",
                  hasError && "bg-destructive/15 text-destructive font-medium"
                )}
                title={stmtType ? getStatementLabel(stmtType) : undefined}
              >
                <div className={cn("w-[3px] self-stretch flex-shrink-0", colorClass)} />
                <span className="flex-1 px-2">{lineNum}</span>
              </div>
            )
          })}
        </div>

        {/* Syntax highlight overlay (behind textarea) */}
        <pre
          ref={highlightRef}
          aria-hidden="true"
          className="pointer-events-none absolute inset-0 m-0 overflow-hidden whitespace-pre-wrap break-words border-0 p-3 font-mono text-sm leading-6"
          style={{ left: GUTTER_WIDTH }}
          dangerouslySetInnerHTML={{ __html: highlightedHtml }}
        />

        {/* Editor textarea (transparent text, captures input) */}
        <textarea
          ref={textareaRef}
          value={query}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onScroll={handleScroll}
          onSelect={updateCursorInfo}
          onClick={updateCursorInfo}
          spellCheck={false}
          aria-label="Datalog query editor"
          aria-autocomplete="list"
          aria-expanded={showAutocomplete}
          aria-controls={showAutocomplete ? "autocomplete-listbox" : undefined}
          aria-activedescendant={showAutocomplete ? `autocomplete-option-${autocomplete.selectedIndex}` : undefined}
          className={cn(
            "editor-textarea relative flex-1 resize-none bg-transparent p-3 font-mono text-sm leading-6 caret-foreground outline-none placeholder:text-muted-foreground scrollbar-thin",
            query ? "text-transparent" : "text-foreground"
          )}
          placeholder={selectedKnowledgeGraph ? `Type a query for '${selectedKnowledgeGraph.name}'...` : "Select a knowledge graph to begin"}
        />

        {/* Autocomplete popup */}
        {showAutocomplete && (
          <AutocompletePopup
            items={autocomplete.items}
            selectedIndex={autocomplete.selectedIndex}
            position={autocomplete.position}
            onSelect={acceptCompletion}
            onSetSelected={(idx) => setAutocomplete((prev) => ({ ...prev, selectedIndex: idx }))}
          />
        )}
      </div>

      {/* Status bar */}
      <div className="flex h-6 items-center justify-between border-t border-border/50 bg-muted/30 px-3 text-[10px] text-muted-foreground">
        <div className="flex items-center gap-3">
          <span>
            Ln {cursorPos.line}, Col {cursorPos.col}
            {selectionLength > 0 && ` (${selectionLength} selected)`}
          </span>
          {lineTypes[cursorPos.line - 1] && (
            <>
              <div className="h-3 w-px bg-border" />
              <span className="flex items-center gap-1.5">
                <span className={cn("h-1.5 w-1.5 rounded-full", STATEMENT_COLORS[lineTypes[cursorPos.line - 1]!])} />
                {getStatementLabel(lineTypes[cursorPos.line - 1]!)}
              </span>
            </>
          )}
        </div>
        <div className="flex items-center gap-2">
          {query.length === 0 && (
            <span className="text-muted-foreground/50">
              Type to begin • Tab to indent
            </span>
          )}
          <span>{query.length} chars</span>
        </div>
      </div>
    </div>
  )
}
