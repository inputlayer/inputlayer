"use client"

import Link from "next/link"
import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import type { CustomerStory } from "@/lib/content-bundle"
import { ArrowRight } from "lucide-react"

interface CustomersIndexClientProps {
  stories: CustomerStory[]
}

export function CustomersIndexClient({ stories }: CustomersIndexClientProps) {
  return (
    <PageLayout>
      <ContentHero
        heading="Customer Stories"
        subtitle="See how teams use InputLayer in production."
      />

      <section className="mx-auto max-w-6xl px-6 py-12">
        {stories.length === 0 ? (
          <p className="text-muted-foreground text-center py-12">Customer stories coming soon.</p>
        ) : (
          <div className="grid gap-6 md:grid-cols-2">
            {stories.map((story) => (
              <Link
                key={story.slug}
                href={`/customers/${story.slug}/`}
                className="group rounded-xl border border-border bg-card p-6 space-y-4 transition-colors hover:border-primary/30 hover:bg-card/80"
              >
                {story.industry && (
                  <span className="inline-flex items-center rounded-full border border-border bg-secondary/50 px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
                    {story.industry}
                  </span>
                )}
                <h3 className="text-lg font-semibold group-hover:text-primary transition-colors">
                  {story.title}
                </h3>
                {story.keyMetric && (
                  <p className="text-2xl font-extrabold text-primary">{story.keyMetric}</p>
                )}
                <span className="inline-flex items-center gap-1 text-sm text-primary font-medium">
                  Read story <ArrowRight className="h-3.5 w-3.5" />
                </span>
              </Link>
            ))}
          </div>
        )}
      </section>

      <CTABanner
        heading="Ready to get started?"
        description="InputLayer is open-source. Pull the Docker image and start building."
        buttons={[
          { label: "Read the docs", href: "/docs/" },
          { label: "View on GitHub", href: "https://github.com/inputlayer/inputlayer", variant: "secondary", external: true },
        ]}
      />
    </PageLayout>
  )
}
