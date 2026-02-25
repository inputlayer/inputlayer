"use client"

import { useState, useMemo } from "react"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { docsNavigation, type NavItem } from "@/lib/docs-bundle"
import { ChevronRight, Search } from "lucide-react"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"

function NavTreeItem({ item, depth = 0 }: { item: NavItem; depth?: number }) {
  const pathname = usePathname()
  const isActive = pathname === item.href || pathname === `${item.href}/`
  const hasChildren = item.children.length > 0
  const isChildActive = hasChildren && pathname.startsWith(item.href)
  const [isOpen, setIsOpen] = useState(isChildActive)

  if (!hasChildren) {
    return (
      <Link
        href={item.href}
        className={cn(
          "block py-1.5 px-3 text-sm rounded-md transition-colors",
          isActive
            ? "bg-primary/10 text-primary font-medium"
            : "text-muted-foreground hover:bg-muted hover:text-foreground"
        )}
        style={{ paddingLeft: `${(depth + 1) * 12}px` }}
      >
        {item.label}
      </Link>
    )
  }

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen}>
      <CollapsibleTrigger className="w-full" asChild>
        <button
          className={cn(
            "flex items-center w-full py-1.5 px-3 text-sm rounded-md transition-colors",
            isChildActive
              ? "text-foreground font-medium"
              : "text-muted-foreground hover:bg-muted hover:text-foreground"
          )}
          style={{ paddingLeft: `${(depth + 1) * 12}px` }}
        >
          <ChevronRight
            className={cn(
              "h-3.5 w-3.5 mr-1 shrink-0 transition-transform",
              isOpen && "rotate-90"
            )}
          />
          {item.label}
        </button>
      </CollapsibleTrigger>
      <CollapsibleContent>
        {item.children.map((child) => (
          <NavTreeItem key={child.key} item={child} depth={depth + 1} />
        ))}
      </CollapsibleContent>
    </Collapsible>
  )
}

export function DocsNav() {
  const [search, setSearch] = useState("")

  const filteredNav = useMemo(() => {
    if (!search.trim()) return docsNavigation

    const term = search.toLowerCase()

    function filterItems(items: NavItem[]): NavItem[] {
      return items.reduce<NavItem[]>((acc, item) => {
        const labelMatch = item.label.toLowerCase().includes(term)
        const filteredChildren = filterItems(item.children)

        if (labelMatch || filteredChildren.length > 0) {
          acc.push({
            ...item,
            children: labelMatch ? item.children : filteredChildren,
          })
        }
        return acc
      }, [])
    }

    return filterItems(docsNavigation)
  }, [search])

  return (
    <div className="flex flex-col h-full">
      {/* Search */}
      <div className="p-3 border-b border-border/50">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search docs..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full h-8 pl-8 pr-3 text-sm rounded-md border border-border bg-background placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </div>
      </div>

      {/* Navigation tree */}
      <nav className="flex-1 overflow-y-auto p-2 space-y-0.5">
        {filteredNav.map((item) => (
          <NavTreeItem key={item.key} item={item} />
        ))}
        {filteredNav.length === 0 && (
          <p className="text-xs text-muted-foreground px-3 py-4">
            No results for &ldquo;{search}&rdquo;
          </p>
        )}
      </nav>
    </div>
  )
}
