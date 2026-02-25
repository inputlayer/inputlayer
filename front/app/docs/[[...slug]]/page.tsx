import { docsPages } from "@/lib/docs-bundle"
import { DocsPageClient } from "./docs-page-client"

interface DocsPageProps {
  params: Promise<{ slug?: string[] }>
}

export default async function DocsPage({ params }: DocsPageProps) {
  const { slug } = await params
  const key = !slug || slug.length === 0 ? "index" : slug.join("/")
  const page = docsPages[key] ?? null

  return <DocsPageClient page={page} slugKey={key} />
}

export function generateStaticParams() {
  return Object.keys(docsPages).map((key) => ({
    slug: key === "index" ? undefined : key.split("/"),
  }))
}
