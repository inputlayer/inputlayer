import { blogPosts } from "@/lib/content-bundle"
import { BlogIndexClient } from "./blog-index-client"

export default function BlogPage() {
  return <BlogIndexClient posts={blogPosts} />
}
