"use client"

import { DocsLayout } from "@/components/docs-layout"
import { MdxComponents } from "@/components/mdx-components"
import { DocsToc } from "@/components/docs-toc"
import type { DocPage } from "@/lib/docs-bundle"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

interface DocsPageClientProps {
  page: DocPage | null
  slugKey: string
}

export function DocsPageClient({ page, slugKey }: DocsPageClientProps) {
  if (!page) {
    return (
      <DocsLayout>
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <h1 className="text-2xl font-bold mb-2">Page not found</h1>
            <p className="text-muted-foreground">
              The documentation page <code>/{slugKey}</code> does not exist.
            </p>
          </div>
        </div>
      </DocsLayout>
    )
  }

  return (
    <DocsLayout>
      <div className="flex flex-1 overflow-hidden">
        {/* Main content */}
        <div className="flex-1 overflow-y-auto px-8 py-6 max-w-4xl mx-auto">
          <article className="docs-prose">
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={MdxComponents}
            >
              {page.content}
            </ReactMarkdown>
          </article>
        </div>

        {/* Table of contents (right sidebar) */}
        {page.toc.length > 0 && (
          <div className="hidden lg:block w-48 shrink-0 border-l border-border/50">
            <DocsToc entries={page.toc} />
          </div>
        )}
      </div>
    </DocsLayout>
  )
}
