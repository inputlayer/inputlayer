"use client"

import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { BlogCard } from "@/components/blog-card"
import { CTABanner } from "@/components/cta-banner"
import type { BlogPost } from "@/lib/content-bundle"

interface BlogIndexClientProps {
  posts: BlogPost[]
}

export function BlogIndexClient({ posts }: BlogIndexClientProps) {
  return (
    <PageLayout>
      <ContentHero
        heading="Blog"
        subtitle="Engineering insights, tutorials, and product updates from the InputLayer team."
      />

      <section className="mx-auto max-w-6xl px-6 py-12">
        {posts.length === 0 ? (
          <p className="text-muted-foreground text-center py-12">No posts yet. Check back soon!</p>
        ) : (
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            {posts.map((post) => (
              <BlogCard
                key={post.slug}
                slug={post.slug}
                title={post.title}
                date={post.date}
                author={post.author}
                excerpt={post.excerpt}
                category={post.category}
              />
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
