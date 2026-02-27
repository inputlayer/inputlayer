"use client"

import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import { MdxComponents } from "@/components/mdx-components"
import type { BlogPost } from "@/lib/content-bundle"
import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

interface BlogPostClientProps {
  post: BlogPost | null
  slug: string
}

export function BlogPostClient({ post, slug }: BlogPostClientProps) {
  if (!post) {
    return (
      <PageLayout>
        <div className="flex flex-1 items-center justify-center py-20">
          <div className="text-center">
            <h1 className="text-2xl font-bold mb-2">Post not found</h1>
            <p className="text-muted-foreground">
              The blog post <code>/{slug}</code> does not exist.
            </p>
          </div>
        </div>
      </PageLayout>
    )
  }

  return (
    <PageLayout>
      <ContentHero
        heading={post.title}
        subtitle={`${post.date ? new Date(post.date).toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" }) : ""}${post.author ? ` · ${post.author}` : ""}`}
        breadcrumbs={[
          { label: "Blog", href: "/blog/" },
        ]}
      />

      <article className="mx-auto max-w-3xl px-6 py-12">
        <div className="docs-prose">
          <ReactMarkdown remarkPlugins={[remarkGfm]} components={MdxComponents}>
            {post.content}
          </ReactMarkdown>
        </div>
      </article>

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
