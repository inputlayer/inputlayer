"use client"

import type { ReactNode } from "react"
import { DocsNav } from "@/components/docs-nav"

interface DocsLayoutProps {
  children: ReactNode
}

/** Docs-specific inner layout: docs nav sidebar + content area. Sits inside AppShell's <main>. */
export function DocsLayout({ children }: DocsLayoutProps) {
  return (
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
  )
}
