"use client"

import Link from "next/link"
import { useEffect, useState } from "react"
import { Logo } from "@/components/logo"
import { ThemeToggle } from "@/components/theme-toggle"
import { ExternalLink, Star, GitFork } from "lucide-react"

function GitHubStats() {
  const [stars, setStars] = useState<number | null>(null)
  const [forks, setForks] = useState<number | null>(null)

  useEffect(() => {
    fetch("https://api.github.com/repos/inputlayer/inputlayer")
      .then((res) => res.ok ? res.json() : null)
      .then((data) => {
        if (data?.stargazers_count != null) {
          setStars(data.stargazers_count)
        }
        if (data?.forks_count != null) {
          setForks(data.forks_count)
        }
      })
      .catch(() => {})
  }, [])

  return (
    <div className="inline-flex items-center gap-1.5">
      <a
        href="https://github.com/inputlayer/inputlayer"
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1.5 rounded-md border border-border bg-secondary/50 px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground hover:bg-secondary"
      >
        <Star className="h-3.5 w-3.5" />
        {stars !== null ? (
          <span>{stars >= 1000 ? `${(stars / 1000).toFixed(1)}k` : stars}</span>
        ) : (
          <span>Star</span>
        )}
      </a>
      <a
        href="https://github.com/inputlayer/inputlayer/fork"
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1.5 rounded-md border border-border bg-secondary/50 px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground hover:bg-secondary"
      >
        <GitFork className="h-3.5 w-3.5" />
        {forks !== null ? (
          <span>{forks >= 1000 ? `${(forks / 1000).toFixed(1)}k` : forks}</span>
        ) : (
          <span>Fork</span>
        )}
      </a>
    </div>
  )
}

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-50 w-full border-b border-border/50 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="flex h-14 items-center px-6">
        <Link href="/" className="mr-8">
          <Logo size="md" />
        </Link>

        <nav className="flex items-center gap-6 text-sm">
          <Link
            href="/docs/"
            className="text-muted-foreground transition-colors hover:text-foreground"
          >
            Docs
          </Link>
          <a
            href="https://demo.inputlayer.ai"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-muted-foreground transition-colors hover:text-foreground"
          >
            Demo
            <ExternalLink className="h-3 w-3" />
          </a>
        </nav>

        <div className="ml-auto flex items-center gap-3">
          <GitHubStats />
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}
