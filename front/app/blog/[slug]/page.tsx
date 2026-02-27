import { blogPosts } from "@/lib/content-bundle"
import { BlogPostClient } from "./blog-post-client"

interface BlogPostPageProps {
  params: Promise<{ slug: string }>
}

export default async function BlogPostPage({ params }: BlogPostPageProps) {
  const { slug } = await params
  const post = blogPosts.find((p) => p.slug === slug) ?? null
  return <BlogPostClient post={post} slug={slug} />
}

export function generateStaticParams() {
  return blogPosts.map((post) => ({ slug: post.slug }))
}
