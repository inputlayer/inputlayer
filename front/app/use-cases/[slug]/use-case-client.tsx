"use client"

import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import { MdxComponents } from "@/components/mdx-components"
import type { UseCase } from "@/lib/content-bundle"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

interface UseCaseClientProps {
  useCase: UseCase | null
  slug: string
}

export function UseCaseClient({ useCase, slug }: UseCaseClientProps) {
  if (!useCase) {
    return (
      <PageLayout>
        <div className="flex flex-1 items-center justify-center py-20">
          <div className="text-center">
            <h1 className="text-2xl font-bold mb-2">Page not found</h1>
            <p className="text-muted-foreground">
              The use case <code>/{slug}</code> does not exist.
            </p>
          </div>
        </div>
      </PageLayout>
    )
  }

  return (
    <PageLayout>
      <ContentHero
        heading={useCase.title}
        subtitle={useCase.subtitle}
        breadcrumbs={[
          { label: "Use Cases", href: "/use-cases/" },
        ]}
      />

      <article className="mx-auto max-w-3xl px-6 py-12">
        <div className="docs-prose">
          <ReactMarkdown remarkPlugins={[remarkGfm]} components={MdxComponents}>
            {useCase.content}
          </ReactMarkdown>
        </div>
      </article>

      <CTABanner
        heading="Ready to build?"
        description="InputLayer is open-source. Pull the Docker image and start building in minutes."
        buttons={[
          { label: "Read the docs", href: "/docs/" },
          { label: "Launch demo", href: "https://demo.inputlayer.ai", variant: "secondary", external: true },
        ]}
      />
    </PageLayout>
  )
}
