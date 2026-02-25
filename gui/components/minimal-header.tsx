"use client"

import { Logo } from "@/components/logo"
import { ThemeToggle } from "@/components/theme-toggle"

/** Minimal header for unauthenticated pages (e.g. docs). Logo + theme toggle only. */
export function MinimalHeader() {
  return (
    <header className="sticky top-0 z-50 w-full border-b border-border/50 bg-background/80 backdrop-blur-xl">
      <div className="flex h-12 items-center px-4">
        <Logo size="sm" />
        <div className="flex-1" />
        <ThemeToggle />
      </div>
    </header>
  )
}
