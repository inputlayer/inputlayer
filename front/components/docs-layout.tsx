"use client"

import type { ReactNode } from "react"
import { DocsNav } from "@/components/docs-nav"
import { SiteHeader } from "@/components/site-header"

interface DocsLayoutProps {
  children: ReactNode
}

/** Docs layout: site header + docs nav sidebar + content area. */
export function DocsLayout({ children }: DocsLayoutProps) {
  return (
    <div className="flex flex-col h-dvh">
      <SiteHeader />
      <div className="flex flex-1 overflow-hidden">
        {/* Docs navigation sidebar */}
        <div className="w-64 shrink-0 border-r border-border/50 overflow-y-auto">
          <DocsNav />
        </div>
        {/* Page content */}
        <div className="flex flex-1 flex-col overflow-hidden">
          {children}
        </div>
      </div>
    </div>
  )
}
