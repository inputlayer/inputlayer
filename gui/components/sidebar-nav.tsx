"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { FileCode, Network, Database } from "lucide-react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"

const mainNavItems = [
  { title: "Query Editor", href: "/query", icon: FileCode },
  { title: "Relations", href: "/relations", icon: Network },
]

const bottomNavItems = [
  { title: "Database", href: "/database", icon: Database },
]

export function SidebarNav() {
  const pathname = usePathname()

  return (
    <TooltipProvider delayDuration={0}>
      <div className="flex h-full w-14 flex-col border-r border-border/50 bg-muted/30">
        {/* Main navigation */}
        <nav className="flex-1 flex flex-col items-center gap-1 p-2 pt-4">
          {mainNavItems.map((item) => {
            const Icon = item.icon
            const isActive = pathname === item.href

            return (
              <Tooltip key={item.href}>
                <TooltipTrigger asChild>
                  <Link
                    href={item.href}
                    aria-current={isActive ? "page" : undefined}
                    className={cn(
                      "flex h-10 w-10 items-center justify-center rounded-lg transition-all",
                      isActive
                        ? "bg-primary/10 text-primary"
                        : "text-muted-foreground hover:bg-muted hover:text-foreground",
                    )}
                  >
                    <Icon className="h-5 w-5" />
                  </Link>
                </TooltipTrigger>
                <TooltipContent side="right" sideOffset={8}>
                  {item.title}
                </TooltipContent>
              </Tooltip>
            )
          })}
        </nav>

        <div className="border-t border-border/50 p-2 flex flex-col items-center gap-1">
          {bottomNavItems.map((item) => {
            const Icon = item.icon
            const isActive = pathname === item.href
            return (
              <Tooltip key={item.href}>
                <TooltipTrigger asChild>
                  <Link
                    href={item.href}
                    aria-current={isActive ? "page" : undefined}
                    className={cn(
                      "flex h-10 w-10 items-center justify-center rounded-lg transition-all",
                      isActive
                        ? "bg-primary/10 text-primary"
                        : "text-muted-foreground hover:bg-muted hover:text-foreground",
                    )}
                  >
                    <Icon className="h-5 w-5" />
                  </Link>
                </TooltipTrigger>
                <TooltipContent side="right" sideOffset={8}>
                  {item.title}
                </TooltipContent>
              </Tooltip>
            )
          })}
        </div>
      </div>
    </TooltipProvider>
  )
}
