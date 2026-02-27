"use client"

import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import { MdxComponents } from "@/components/mdx-components"
import type { ComparisonPage } from "@/lib/content-bundle"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

interface CompareClientProps {
  page: ComparisonPage | null
  slug: string
}

export function CompareClient({ page, slug }: CompareClientProps) {
  if (!page) {
    return (
      <PageLayout>
        <div className="flex flex-1 items-center justify-center py-20">
          <div className="text-center">
            <h1 className="text-2xl font-bold mb-2">Page not found</h1>
            <p className="text-muted-foreground">
              The comparison page <code>/{slug}</code> does not exist.
            </p>
          </div>
        </div>
      </PageLayout>
    )
  }

  return (
    <PageLayout>
      <ContentHero
        heading={page.title}
        breadcrumbs={[
          { label: "Compare", href: "/compare/" },
        ]}
      />

      <article className="mx-auto max-w-3xl px-6 py-12">
        <div className="docs-prose">
          <ReactMarkdown remarkPlugins={[remarkGfm]} components={MdxComponents}>
            {page.content}
          </ReactMarkdown>
        </div>
      </article>

      <CTABanner
        heading="See the difference"
        description="Try InputLayer in your browser. No installation required."
        buttons={[
          { label: "Launch demo", href: "https://demo.inputlayer.ai", external: true },
          { label: "Get started", href: "/docs/guides/quickstart/", variant: "secondary" },
        ]}
      />
    </PageLayout>
  )
}
