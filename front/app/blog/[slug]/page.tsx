import type { Metadata } from "next"
import { blogPosts } from "@/lib/content-bundle"
import { BlogPostClient } from "./blog-post-client"

interface BlogPostPageProps {
  params: Promise<{ slug: string }>
}

export async function generateMetadata({ params }: BlogPostPageProps): Promise<Metadata> {
  const { slug } = await params
  const post = blogPosts.find((p) => p.slug === slug)

  if (!post) {
    return { title: "Post Not Found - InputLayer Blog" }
  }

  const title = `${post.title} - InputLayer Blog`
  const description = post.excerpt || `Read "${post.title}" on the InputLayer blog.`

  return {
    title,
    description,
    openGraph: {
      title,
      description,
      type: "article",
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
    },
  }
}

export default async function BlogPostPage({ params }: BlogPostPageProps) {
  const { slug } = await params
  const post = blogPosts.find((p) => p.slug === slug) ?? null
  return <BlogPostClient post={post} slug={slug} />
}

export function generateStaticParams() {
  return blogPosts.map((post) => ({ slug: post.slug }))
}
