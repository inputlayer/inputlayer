"use client"

import type { Components } from "react-markdown"
import Link from "next/link"
import { cn } from "@/lib/utils"
import { createElement, isValidElement } from "react"
import { highlightToHtml } from "@/lib/syntax-highlight"
import { highlightGeneric } from "@/lib/generic-highlight"
import { isDiagramLanguage, DiagramRenderer, type DiagramLanguage } from "@/components/diagrams"

function HeadingWithId({
  level,
  children,
  ...props
}: {
  level: number
  children?: React.ReactNode
  [key: string]: unknown
}) {
  const text = String(children ?? "")
  const id =
    text
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .trim() || undefined

  return createElement(`h${level}`, { id, ...props }, children)
}

function isInternalLink(href: string): boolean {
  return href.startsWith("/") || href.startsWith("#")
}

export const MdxComponents: Components = {
  h1: ({ children, ...props }) => (
    <HeadingWithId level={1} {...props}>
      {children}
    </HeadingWithId>
  ),
  h2: ({ children, ...props }) => (
    <HeadingWithId level={2} {...props}>
      {children}
    </HeadingWithId>
  ),
  h3: ({ children, ...props }) => (
    <HeadingWithId level={3} {...props}>
      {children}
    </HeadingWithId>
  ),
  h4: ({ children, ...props }) => (
    <HeadingWithId level={4} {...props}>
      {children}
    </HeadingWithId>
  ),

  a: ({ href, children, ...props }) => {
    if (!href) return <span {...props}>{children}</span>
    if (isInternalLink(href)) {
      return (
        <Link href={href} {...props}>
          {children}
        </Link>
      )
    }
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" {...props}>
        {children}
      </a>
    )
  },

  pre: ({ children, ...props }) => {
    // If the child code block is a diagram language, render without pre wrapper
    if (isValidElement(children)) {
      const child = children as React.ReactElement<{ className?: string; children?: React.ReactNode }>
      const cls = child.props?.className || ""
      const langMatch = cls.match(/language-(\w+)/)
      if (langMatch && isDiagramLanguage(langMatch[1])) {
        const raw = String(child.props?.children || "").replace(/\n$/, "")
        return <DiagramRenderer content={raw} type={langMatch[1] as DiagramLanguage} />
      }
    }
    return (
      <pre className="rounded-lg bg-[var(--code-bg)] p-4 overflow-x-auto mb-4 text-sm font-mono" {...props}>
        {children}
      </pre>
    )
  },

  code: ({ className, children, ...props }) => {
    // Fenced code blocks get a className like "language-datalog"
    const isBlock = className?.startsWith("language-")
    if (isBlock) {
      const raw = String(children).replace(/\n$/, "")
      const lang = className!.replace("language-", "")

      // Use our custom Datalog tokenizer
      if (lang === "datalog") {
        const html = highlightToHtml(raw)
        return (
          <code
            className={cn(className, "text-sm")}
            dangerouslySetInnerHTML={{ __html: html }}
          />
        )
      }

      // Try generic highlighting for other languages
      const html = highlightGeneric(raw, lang)
      if (html) {
        return (
          <code
            className={cn(className, "text-sm")}
            dangerouslySetInnerHTML={{ __html: html }}
          />
        )
      }

      // Fallback: plain text for unsupported languages
      return (
        <code className={cn(className, "text-sm")} {...props}>
          {children}
        </code>
      )
    }
    // Inline code
    return (
      <code
        className="bg-muted rounded px-1.5 py-0.5 font-mono text-sm"
        {...props}
      >
        {children}
      </code>
    )
  },

  table: ({ children, ...props }) => (
    <div className="my-4 overflow-x-auto">
      <table className="w-full border-collapse" {...props}>
        {children}
      </table>
    </div>
  ),

  th: ({ children, ...props }) => (
    <th
      className="border px-4 py-2 text-left font-semibold bg-muted/50"
      {...props}
    >
      {children}
    </th>
  ),

  td: ({ children, ...props }) => (
    <td className="border px-4 py-2" {...props}>
      {children}
    </td>
  ),

  blockquote: ({ children, ...props }) => (
    <blockquote
      className="border-l-4 border-primary/30 pl-4 italic text-muted-foreground my-4"
      {...props}
    >
      {children}
    </blockquote>
  ),
}
