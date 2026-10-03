"use client"

import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import { MdxComponents } from "@/components/mdx-components"
import type { CustomerStory } from "@/lib/content-bundle"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

interface CustomerClientProps {
  story: CustomerStory | null
  slug: string
}

export function CustomerClient({ story, slug }: CustomerClientProps) {
  if (!story) {
    return (
      <PageLayout>
        <div className="flex flex-1 items-center justify-center py-20">
          <div className="text-center">
            <h1 className="text-2xl font-bold mb-2">Page not found</h1>
            <p className="text-muted-foreground">
              The customer story <code>/{slug}</code> does not exist.
            </p>
          </div>
        </div>
      </PageLayout>
    )
  }

  return (
    <PageLayout>
      <ContentHero
        heading={story.title}
        subtitle={`Illustrative story${story.industry ? ` · Industry: ${story.industry}` : ""}`}
        breadcrumbs={[
          { label: "Customers", href: "/customers/" },
        ]}
      />

      <article className="mx-auto max-w-3xl px-6 py-12">
        <p className="mb-8 rounded-lg border border-border bg-muted/40 px-4 py-3 text-sm text-muted-foreground">
          This is an illustrative story, not a confirmed customer reference. Figures in it are not independently
          measured.
        </p>
        <div className="docs-prose">
          <ReactMarkdown remarkPlugins={[remarkGfm]} components={MdxComponents}>
            {story.content}
          </ReactMarkdown>
        </div>
      </article>

      <CTABanner
        heading="Models think. InputLayer knows."
        description="The live knowledge graph for AI agents. Self-hosted and source-available under the Elastic License 2.0."
        buttons={[
          { label: "Quickstart", href: "/docs/guides/quickstart/" },
          { label: "Read the docs", href: "/docs/", variant: "secondary" },
        ]}
      />
    </PageLayout>
  )
}
