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
        subtitle={story.industry ? `Industry: ${story.industry}` : undefined}
        breadcrumbs={[
          { label: "Customers", href: "/customers/" },
        ]}
      />

      <article className="mx-auto max-w-3xl px-6 py-12">
        <div className="docs-prose">
          <ReactMarkdown remarkPlugins={[remarkGfm]} components={MdxComponents}>
            {story.content}
          </ReactMarkdown>
        </div>
      </article>

      <CTABanner
        heading="See what InputLayer can do for you"
        description="Open-source. Single Docker container. Start building in minutes."
        buttons={[
          { label: "Get started", href: "/docs/guides/quickstart/" },
          { label: "Launch demo", href: "https://demo.inputlayer.ai", variant: "secondary", external: true },
        ]}
      />
    </PageLayout>
  )
}
