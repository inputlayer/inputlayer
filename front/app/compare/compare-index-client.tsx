"use client"

import Link from "next/link"
import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import type { ComparisonPage } from "@/lib/content-bundle"
import { ArrowRight } from "lucide-react"

interface CompareIndexClientProps {
  pages: ComparisonPage[]
}

export function CompareIndexClient({ pages }: CompareIndexClientProps) {
  return (
    <PageLayout>
      <ContentHero
        heading="Compare"
        subtitle="InputLayer is the live rules engine for AI agents. Here is how it compares with the tools already in your stack: what it sits beside, and where each of them wins."
      />

      <section className="mx-auto max-w-6xl px-6 py-12">
        {pages.length === 0 ? (
          <p className="text-muted-foreground text-center py-12">Comparison pages coming soon.</p>
        ) : (
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            {pages.map((page) => (
              <Link
                key={page.slug}
                href={`/compare/${page.slug}/`}
                className="group rounded-xl border border-border bg-card p-6 space-y-4 transition-colors hover:border-primary/30 hover:bg-card/80"
              >
                <h3 className="text-lg font-semibold group-hover:text-primary transition-colors">
                  {page.title}
                </h3>
                {page.competitors.length > 0 && (
                  <div className="flex flex-wrap gap-2">
                    {page.competitors.map((c) => (
                      <span
                        key={c}
                        className="inline-flex items-center rounded-full border border-border bg-secondary/50 px-2.5 py-0.5 text-xs font-medium text-muted-foreground"
                      >
                        {c}
                      </span>
                    ))}
                  </div>
                )}
                <span className="inline-flex items-center gap-1 text-sm text-primary font-medium">
                  Read comparison <ArrowRight className="h-3.5 w-3.5" />
                </span>
              </Link>
            ))}
          </div>
        )}
      </section>

      <CTABanner
        heading="Take the rules out of your prompts."
        description="The live rules engine for AI agents. Self-hosted and source-available under the Elastic License 2.0."
        buttons={[
          { label: "Quickstart", href: "/docs/guides/quickstart/" },
          { label: "Read the docs", href: "/docs/", variant: "secondary" },
        ]}
      />
    </PageLayout>
  )
}
