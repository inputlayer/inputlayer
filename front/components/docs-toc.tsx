"use client"

import { useEffect, useState } from "react"
import { cn } from "@/lib/utils"
import type { TocEntry } from "@/lib/docs-bundle"

interface DocsTocProps {
  entries: TocEntry[]
}

export function DocsToc({ entries }: DocsTocProps) {
  const [activeId, setActiveId] = useState<string>("")

  useEffect(() => {
    const observer = new IntersectionObserver(
      (observerEntries) => {
        for (const entry of observerEntries) {
          if (entry.isIntersecting) {
            setActiveId(entry.target.id)
          }
        }
      },
      { rootMargin: "-80px 0px -80% 0px", threshold: 0.1 }
    )

    const headings = entries
      .map((e) => document.getElementById(e.id))
      .filter(Boolean)

    for (const heading of headings) {
      if (heading) observer.observe(heading)
    }

    return () => observer.disconnect()
  }, [entries])

  return (
    <div className="sticky top-0 p-4">
      <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
        On this page
      </p>
      <nav className="space-y-1">
        {entries.map((entry) => (
          <a
            key={entry.id}
            href={`#${entry.id}`}
            className={cn(
              "block text-xs py-0.5 transition-colors hover:text-foreground",
              entry.level === 3 && "pl-3",
              activeId === entry.id
                ? "text-primary font-medium"
                : "text-muted-foreground"
            )}
          >
            {entry.text}
          </a>
        ))}
      </nav>
    </div>
  )
}
