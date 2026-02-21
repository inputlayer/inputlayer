"use client"

import { useEffect, useRef } from "react"
import { cn } from "@/lib/utils"
import type { CompletionItem, CompletionKind } from "@/lib/autocomplete"
import { Database, FunctionSquare, Sigma, Hash, Type, Terminal, Eye } from "lucide-react"

const KIND_CONFIG: Record<CompletionKind, { icon: typeof Database; className: string }> = {
  relation: { icon: Database, className: "text-[var(--chart-1)]" },        // cyan/teal primary
  view: { icon: Eye, className: "text-[var(--chart-5)]" },                 // pink/magenta
  column: { icon: Hash, className: "text-[var(--chart-3)]" },              // green
  function: { icon: FunctionSquare, className: "text-[var(--chart-5)]" },  // pink/magenta
  aggregate: { icon: Sigma, className: "text-[var(--chart-5)]" },          // pink/magenta
  keyword: { icon: Type, className: "text-[var(--code-keyword)]" },        // keyword purple
  meta: { icon: Terminal, className: "text-[var(--muted-foreground)]" },    // muted
}

interface AutocompletePopupProps {
  items: CompletionItem[]
  selectedIndex: number
  position: { top: number; left: number }
  onSelect: (item: CompletionItem) => void
  onSetSelected: (index: number) => void
}

export function AutocompletePopup({
  items,
  selectedIndex,
  position,
  onSelect,
  onSetSelected,
}: AutocompletePopupProps) {
  const listRef = useRef<HTMLDivElement>(null)
  const selectedRef = useRef<HTMLDivElement>(null)

  // Scroll selected item into view
  useEffect(() => {
    if (selectedRef.current) {
      selectedRef.current.scrollIntoView({ block: "nearest" })
    }
  }, [selectedIndex])

  if (items.length === 0) return null

  return (
    <div
      className="absolute z-50 min-w-[280px] max-w-[400px] rounded-md border border-border bg-popover text-popover-foreground shadow-lg"
      style={{
        top: `${position.top}px`,
        left: `${position.left}px`,
      }}
    >
      <div
        ref={listRef}
        className="max-h-[300px] overflow-y-auto overflow-x-hidden py-1 scrollbar-thin"
      >
        {items.map((item, index) => {
          const config = KIND_CONFIG[item.kind]
          const Icon = config.icon
          const isSelected = index === selectedIndex

          return (
            <div
              key={`${item.kind}-${item.label}-${index}`}
              ref={isSelected ? selectedRef : undefined}
              className={cn(
                "flex cursor-pointer items-center gap-2 px-2 py-1 text-sm",
                isSelected
                  ? "bg-fuchsia-500/20 text-foreground"
                  : "hover:bg-fuchsia-500/15"
              )}
              onMouseDown={(e) => {
                e.preventDefault() // Prevent textarea blur
                onSelect(item)
              }}
              onMouseEnter={() => onSetSelected(index)}
            >
              <Icon className={cn("h-3.5 w-3.5 shrink-0", config.className)} />
              <span className="font-mono text-xs font-medium truncate">
                {item.label}
              </span>
              {item.detail && (
                <span className="ml-auto text-[10px] text-muted-foreground truncate max-w-[160px]">
                  {item.detail}
                </span>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
